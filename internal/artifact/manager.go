package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/samarth1412/RunMesh/internal/storage"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	MaxArtifactBytes = int64(100 << 20)
	MaxLogBytes      = int64(10 << 20)
	InlineThreshold  = int64(256 << 10)
)

type Config struct {
	Endpoint, PublicEndpoint, Region, Bucket, AccessKey, SecretKey string
	PathStyle, CreateBucket                                        bool
	PresignExpiry                                                  time.Duration
}

type Manager struct {
	Store           *storage.Store
	client          *s3.Client
	presign         *s3.PresignClient
	internalPresign *s3.PresignClient
	bucket          string
	expiry          time.Duration
}

type Upload struct {
	Artifact  storage.Artifact  `json:"artifact"`
	URL       string            `json:"upload_url"`
	Headers   map[string]string `json:"headers"`
	ExpiresAt time.Time         `json:"expires_at"`
}

type Download struct {
	Artifact  storage.Artifact `json:"artifact"`
	URL       string           `json:"download_url"`
	ExpiresAt time.Time        `json:"expires_at"`
}

func New(ctx context.Context, store *storage.Store, cfg Config) (*Manager, error) {
	options := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(cfg.Region)}
	if cfg.AccessKey != "" {
		options = append(options, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("load S3 configuration: %w", err)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = cfg.PathStyle
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(strings.TrimRight(cfg.Endpoint, "/"))
		}
	})
	presignClient := client
	if cfg.PublicEndpoint != "" && strings.TrimRight(cfg.PublicEndpoint, "/") != strings.TrimRight(cfg.Endpoint, "/") {
		presignClient = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			o.UsePathStyle = cfg.PathStyle
			o.BaseEndpoint = aws.String(strings.TrimRight(cfg.PublicEndpoint, "/"))
		})
	}
	m := &Manager{
		Store: store, client: client,
		presign: s3.NewPresignClient(presignClient), internalPresign: s3.NewPresignClient(client),
		bucket: cfg.Bucket, expiry: cfg.PresignExpiry,
	}
	if m.expiry == 0 {
		m.expiry = 15 * time.Minute
	}
	if cfg.CreateBucket {
		if err = m.ensureBucket(ctx); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (m *Manager) ensureBucket(ctx context.Context) error {
	if _, err := m.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(m.bucket)}); err == nil {
		return nil
	}
	_, err := m.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(m.bucket)})
	if err != nil {
		if _, headErr := m.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(m.bucket)}); headErr == nil {
			return nil
		}
		return fmt.Errorf("create artifact bucket: %w", err)
	}
	return nil
}

func (m *Manager) CreateUpload(ctx context.Context, tenantID, userID, kind, contentType string, size int64, checksum string, taskRunID *string) (Upload, error) {
	return m.createUpload(ctx, m.presign, tenantID, userID, kind, contentType, size, checksum, taskRunID)
}

// CreateInternalUpload signs against the service endpoint reachable by workers.
// Browser/API callers use CreateUpload, which may use a separate public endpoint.
func (m *Manager) CreateInternalUpload(ctx context.Context, tenantID, userID, kind, contentType string, size int64, checksum string, taskRunID *string) (Upload, error) {
	return m.createUpload(ctx, m.internalPresign, tenantID, userID, kind, contentType, size, checksum, taskRunID)
}

func (m *Manager) createUpload(ctx context.Context, presigner *s3.PresignClient, tenantID, userID, kind, contentType string, size int64, checksum string, taskRunID *string) (Upload, error) {
	ctx, span := otel.Tracer("runmesh/artifact").Start(ctx, "artifact.create_upload", trace.WithAttributes(attribute.String("tenant_id", tenantID), attribute.String("artifact.kind", kind), attribute.Int64("artifact.size", size)))
	defer span.End()
	if kind != "input" && kind != "output" && kind != "log" {
		return Upload{}, fmt.Errorf("invalid artifact kind")
	}
	limit := MaxArtifactBytes
	if kind == "log" {
		limit = MaxLogBytes
	}
	if size < 0 || size > limit {
		return Upload{}, fmt.Errorf("artifact size exceeds %d bytes", limit)
	}
	checksum = strings.ToLower(strings.TrimSpace(checksum))
	if checksum != "" {
		decoded, err := hex.DecodeString(checksum)
		if err != nil || len(decoded) != sha256.Size {
			return Upload{}, fmt.Errorf("checksum_sha256 must be 64 hexadecimal characters")
		}
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	if taskRunID != nil {
		if err := m.Store.TaskBelongsToTenant(ctx, *taskRunID, tenantID); err != nil {
			return Upload{}, err
		}
	}
	id := uuid.NewString()
	key := tenantID + "/" + kind + "/" + id
	uri := "s3://" + m.bucket + "/" + key
	a, err := m.Store.CreateArtifact(ctx, id, tenantID, userID, kind, key, uri, contentType, size, checksum, taskRunID)
	if err != nil {
		return Upload{}, err
	}
	request, err := presigner.PresignPutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(m.bucket), Key: aws.String(key), ContentType: aws.String(contentType)}, s3.WithPresignExpires(m.expiry))
	if err != nil {
		return Upload{}, fmt.Errorf("sign artifact upload: %w", err)
	}
	headers := map[string]string{}
	for key, values := range request.SignedHeader {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}
	return Upload{Artifact: a, URL: request.URL, Headers: headers, ExpiresAt: time.Now().Add(m.expiry)}, nil
}

func (m *Manager) Complete(ctx context.Context, tenantID, id string) (storage.Artifact, error) {
	ctx, span := otel.Tracer("runmesh/artifact").Start(ctx, "artifact.complete", trace.WithAttributes(attribute.String("tenant_id", tenantID), attribute.String("artifact.id", id)))
	defer span.End()
	a, err := m.Store.GetArtifact(ctx, tenantID, id)
	if err != nil {
		return a, err
	}
	if a.Status == "READY" {
		return a, nil
	}
	head, err := m.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(m.bucket), Key: aws.String(a.ObjectKey)})
	if err != nil {
		return a, fmt.Errorf("inspect uploaded artifact: %w", err)
	}
	if head.ContentLength == nil || *head.ContentLength != a.DeclaredSize {
		return a, storage.ErrConflict
	}
	if a.ChecksumSHA256 != "" {
		object, getErr := m.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(m.bucket), Key: aws.String(a.ObjectKey)})
		if getErr != nil {
			return a, fmt.Errorf("verify artifact checksum: %w", getErr)
		}
		defer object.Body.Close()
		actual, checksumErr := Checksum(io.LimitReader(object.Body, a.DeclaredSize+1))
		if checksumErr != nil {
			return a, fmt.Errorf("verify artifact checksum: %w", checksumErr)
		}
		if actual != a.ChecksumSHA256 {
			return a, storage.ErrConflict
		}
	}
	return m.Store.CompleteArtifact(ctx, tenantID, id, *head.ContentLength)
}

func (m *Manager) Download(ctx context.Context, tenantID, idOrURI string) (Download, error) {
	return m.download(ctx, m.presign, tenantID, idOrURI)
}

// DownloadInternal signs against the service endpoint reachable by workers.
func (m *Manager) DownloadInternal(ctx context.Context, tenantID, idOrURI string) (Download, error) {
	return m.download(ctx, m.internalPresign, tenantID, idOrURI)
}

func (m *Manager) download(ctx context.Context, presigner *s3.PresignClient, tenantID, idOrURI string) (Download, error) {
	ctx, span := otel.Tracer("runmesh/artifact").Start(ctx, "artifact.sign_download", trace.WithAttributes(attribute.String("tenant_id", tenantID)))
	defer span.End()
	var a storage.Artifact
	var err error
	if strings.HasPrefix(idOrURI, "s3://") {
		a, err = m.Store.GetArtifactByURI(ctx, tenantID, idOrURI)
	} else {
		a, err = m.Store.GetArtifact(ctx, tenantID, idOrURI)
	}
	if err != nil {
		return Download{}, err
	}
	if a.Status != "READY" {
		return Download{}, storage.ErrConflict
	}
	request, err := presigner.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(m.bucket), Key: aws.String(a.ObjectKey)}, s3.WithPresignExpires(m.expiry))
	if err != nil {
		return Download{}, fmt.Errorf("sign artifact download: %w", err)
	}
	return Download{Artifact: a, URL: request.URL, ExpiresAt: time.Now().Add(m.expiry)}, nil
}

func ParseURI(raw string) (bucket, key string, err error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "s3" || u.Host == "" || strings.TrimPrefix(u.Path, "/") == "" {
		return "", "", errors.New("invalid artifact URI")
	}
	return u.Host, strings.TrimPrefix(u.Path, "/"), nil
}

func Checksum(reader io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, reader); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

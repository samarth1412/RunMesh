package runmesh

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	runmeshv1 "github.com/samarth1412/RunMesh/gen/runmesh/v1"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"
)

type Task struct {
	ID               string          `json:"id"`
	WorkflowRunID    string          `json:"workflow_run_id"`
	TaskKey          string          `json:"task_key"`
	Handler          string          `json:"handler"`
	Status           string          `json:"status"`
	AttemptCount     int             `json:"attempt_count"`
	MaximumAttempts  int             `json:"maximum_attempts"`
	TimeoutSeconds   int             `json:"timeout_seconds"`
	Input            json.RawMessage `json:"input"`
	InputArtifactURI string          `json:"input_artifact_uri,omitempty"`
}
type Heartbeat struct {
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
	Cancelled      bool      `json:"cancelled"`
}
type Client struct {
	token, workerID, httpBaseURL string
	http                         *http.Client
	connection                   *grpc.ClientConn
	worker                       runmeshv1.WorkerServiceClient
	initErr                      error
}

func NewClient(endpoint, token, workerID string) *Client {
	target := strings.TrimPrefix(strings.TrimPrefix(strings.TrimRight(endpoint, "/"), "http://"), "grpc://")
	httpTarget := target
	if strings.HasSuffix(httpTarget, "7001") {
		httpTarget = strings.TrimSuffix(httpTarget, "7001") + "8080"
	}
	connection, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	client := &Client{token: token, workerID: workerID, httpBaseURL: "http://" + httpTarget, http: &http.Client{Timeout: 30 * time.Second}, connection: connection, initErr: err}
	if err == nil {
		client.worker = runmeshv1.NewWorkerServiceClient(connection)
	}
	return client
}
func (c *Client) Close() error {
	if c.connection == nil {
		return nil
	}
	return c.connection.Close()
}
func (c *Client) rpcContext(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.token)
}
func (c *Client) Lease(ctx context.Context, id string) (Task, error) {
	if c.initErr != nil {
		return Task{}, c.initErr
	}
	response, err := c.worker.Lease(c.rpcContext(ctx), &runmeshv1.LeaseRequest{TaskRunId: id, WorkerId: c.workerID})
	if err != nil {
		return Task{}, err
	}
	result := task(response.Task)
	if result.InputArtifactURI != "" {
		result.Input, err = c.DownloadArtifact(ctx, id, result.InputArtifactURI)
		if err != nil {
			return Task{}, fmt.Errorf("download input artifact: %w", err)
		}
	}
	return result, nil
}
func (c *Client) Start(ctx context.Context, id string) (Task, error) {
	if c.initErr != nil {
		return Task{}, c.initErr
	}
	response, err := c.worker.Start(c.rpcContext(ctx), &runmeshv1.StartRequest{TaskRunId: id, WorkerId: c.workerID})
	if err != nil {
		return Task{}, err
	}
	return task(response.Task), nil
}
func (c *Client) Heartbeat(ctx context.Context, id string) (Heartbeat, error) {
	if c.initErr != nil {
		return Heartbeat{}, c.initErr
	}
	response, err := c.worker.Heartbeat(c.rpcContext(ctx), &runmeshv1.HeartbeatRequest{TaskRunId: id, WorkerId: c.workerID})
	if err != nil {
		return Heartbeat{}, err
	}
	return Heartbeat{LeaseExpiresAt: response.LeaseExpiresAt.AsTime(), Cancelled: response.Cancelled}, nil
}
func (c *Client) Complete(ctx context.Context, id string, output any) (Task, error) {
	if c.initErr != nil {
		return Task{}, c.initErr
	}
	raw, err := json.Marshal(output)
	if err != nil {
		return Task{}, err
	}
	var object map[string]any
	if err = json.Unmarshal(raw, &object); err != nil {
		return Task{}, err
	}
	artifactURI := ""
	if int64(len(raw)) > 256<<10 {
		artifactURI, err = c.UploadArtifact(ctx, id, "output", "application/json", raw)
		if err != nil {
			return Task{}, fmt.Errorf("upload output artifact: %w", err)
		}
		object = map[string]any{}
	}
	structured, err := structpb.NewStruct(object)
	if err != nil {
		return Task{}, err
	}
	logData, err := json.Marshal(map[string]any{"task_run_id": id, "status": "SUCCEEDED"})
	if err != nil {
		return Task{}, err
	}
	logArtifactURI, err := c.UploadArtifact(ctx, id, "log", "application/json", logData)
	if err != nil {
		return Task{}, fmt.Errorf("upload task log: %w", err)
	}
	response, err := c.worker.Complete(c.rpcContext(ctx), &runmeshv1.CompleteRequest{TaskRunId: id, WorkerId: c.workerID, Output: structured, OutputArtifactUri: artifactURI, LogArtifactUri: logArtifactURI})
	if err != nil {
		return Task{}, err
	}
	return task(response.Task), nil
}

func (c *Client) UploadArtifact(ctx context.Context, taskID, kind, contentType string, data []byte) (string, error) {
	checksum := fmt.Sprintf("%x", sha256.Sum256(data))
	response, err := c.worker.CreateArtifactUpload(c.rpcContext(ctx), &runmeshv1.CreateArtifactUploadRequest{TaskRunId: taskID, WorkerId: c.workerID, Kind: kind, ContentType: contentType, SizeBytes: int64(len(data)), ChecksumSha256: checksum})
	if err != nil {
		return "", err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, response.UploadUrl, bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	for key, value := range response.Headers {
		if !strings.EqualFold(key, "host") {
			request.Header.Set(key, value)
		}
	}
	putResponse, err := c.http.Do(request)
	if err != nil {
		return "", err
	}
	defer putResponse.Body.Close()
	if putResponse.StatusCode < 200 || putResponse.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(putResponse.Body, 4096))
		return "", fmt.Errorf("artifact upload returned %s: %s", putResponse.Status, raw)
	}
	completed, err := c.worker.CompleteArtifactUpload(c.rpcContext(ctx), &runmeshv1.CompleteArtifactUploadRequest{ArtifactId: response.ArtifactId, WorkerId: c.workerID, TaskRunId: taskID})
	if err != nil {
		return "", err
	}
	return completed.ArtifactUri, nil
}

func (c *Client) DownloadArtifact(ctx context.Context, taskID, artifactURI string) ([]byte, error) {
	response, err := c.worker.GetArtifactDownload(c.rpcContext(ctx), &runmeshv1.GetArtifactDownloadRequest{ArtifactUri: artifactURI, WorkerId: c.workerID, TaskRunId: taskID})
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, response.DownloadUrl, nil)
	if err != nil {
		return nil, err
	}
	download, err := c.http.Do(request)
	if err != nil {
		return nil, err
	}
	defer download.Body.Close()
	if download.StatusCode < 200 || download.StatusCode >= 300 {
		return nil, fmt.Errorf("artifact download returned %s", download.Status)
	}
	return io.ReadAll(io.LimitReader(download.Body, (100<<20)+1))
}
func (c *Client) Fail(ctx context.Context, id string, retryable bool, errValue error) (Task, error) {
	if c.initErr != nil {
		return Task{}, c.initErr
	}
	response, err := c.worker.Fail(c.rpcContext(ctx), &runmeshv1.FailRequest{TaskRunId: id, WorkerId: c.workerID, Retryable: retryable, ErrorType: fmt.Sprintf("%T", errValue), ErrorMessage: errValue.Error()})
	if err != nil {
		return Task{}, err
	}
	return task(response.Task), nil
}
func (c *Client) Report(ctx context.Context, handlers []string, active int) error {
	return c.postHTTP(ctx, "/internal/v1/workers/"+c.workerID+"/heartbeat", map[string]any{"handlers": handlers, "active_tasks": active, "metadata": map[string]string{"sdk": "go"}})
}
func (c *Client) postHTTP(ctx context.Context, path string, payload map[string]any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.httpBaseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	request.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("runmesh returned %s: %s", response.Status, string(raw))
	}
	return nil
}
func task(value *runmeshv1.TaskSnapshot) Task {
	input, _ := json.Marshal(value.Input.AsMap())
	return Task{ID: value.Id, WorkflowRunID: value.WorkflowRunId, TaskKey: value.TaskKey, Handler: value.Handler, Status: value.Status, AttemptCount: int(value.Attempt), TimeoutSeconds: int(value.TimeoutSeconds), Input: input, InputArtifactURI: value.InputArtifactUri}
}

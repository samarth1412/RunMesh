package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	runmeshv1 "github.com/runmesh/runmesh/gen/runmesh/v1"
	"github.com/runmesh/runmesh/internal/artifact"
	"github.com/runmesh/runmesh/internal/auth"
	"github.com/runmesh/runmesh/internal/storage"
	"github.com/runmesh/runmesh/internal/workflow"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type WorkerServer struct {
	runmeshv1.UnimplementedWorkerServiceServer
	Store         *storage.Store
	LeaseDuration time.Duration
	Authenticator *auth.Authenticator
	Artifacts     *artifact.Manager
}

func (s *WorkerServer) authorize(ctx context.Context) (auth.Principal, error) {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(values) != 1 {
		return auth.Principal{}, status.Error(codes.Unauthenticated, "invalid worker credential")
	}
	principal, err := s.Authenticator.AuthenticateWorker(ctx, strings.TrimPrefix(values[0], "Bearer "))
	if err != nil {
		return auth.Principal{}, status.Error(codes.Unauthenticated, "invalid worker credential")
	}
	return principal, nil
}
func (s *WorkerServer) authorizeTask(ctx context.Context, taskID string) error {
	principal, err := s.authorize(ctx)
	if err != nil {
		return err
	}
	if err = s.Store.TaskBelongsToTenant(ctx, taskID, principal.TenantID); err != nil {
		return rpcError(err)
	}
	return nil
}

func (s *WorkerServer) principalForTask(ctx context.Context, taskID string) (auth.Principal, error) {
	principal, err := s.authorize(ctx)
	if err != nil {
		return principal, err
	}
	if err = s.Store.TaskBelongsToTenant(ctx, taskID, principal.TenantID); err != nil {
		return principal, rpcError(err)
	}
	return principal, nil
}
func (s *WorkerServer) Lease(ctx context.Context, request *runmeshv1.LeaseRequest) (*runmeshv1.LeaseResponse, error) {
	if err := s.authorizeTask(ctx, request.TaskRunId); err != nil {
		return nil, err
	}
	task, err := s.Store.LeaseTask(ctx, request.TaskRunId, request.WorkerId, s.LeaseDuration)
	if err != nil {
		return nil, rpcError(err)
	}
	return &runmeshv1.LeaseResponse{Task: snapshot(task)}, nil
}
func (s *WorkerServer) Start(ctx context.Context, request *runmeshv1.StartRequest) (*runmeshv1.StartResponse, error) {
	if err := s.authorizeTask(ctx, request.TaskRunId); err != nil {
		return nil, err
	}
	task, err := s.Store.StartTask(ctx, request.TaskRunId, request.WorkerId)
	if err != nil {
		return nil, rpcError(err)
	}
	return &runmeshv1.StartResponse{Task: snapshot(task)}, nil
}
func (s *WorkerServer) Heartbeat(ctx context.Context, request *runmeshv1.HeartbeatRequest) (*runmeshv1.HeartbeatResponse, error) {
	if err := s.authorizeTask(ctx, request.TaskRunId); err != nil {
		return nil, err
	}
	expires, cancelled, err := s.Store.HeartbeatTask(ctx, request.TaskRunId, request.WorkerId, s.LeaseDuration)
	if err != nil {
		return nil, rpcError(err)
	}
	return &runmeshv1.HeartbeatResponse{LeaseExpiresAt: timestamppb.New(expires), Cancelled: cancelled}, nil
}
func (s *WorkerServer) Complete(ctx context.Context, request *runmeshv1.CompleteRequest) (*runmeshv1.CompleteResponse, error) {
	principal, err := s.principalForTask(ctx, request.TaskRunId)
	if err != nil {
		return nil, err
	}
	if request.OutputArtifactUri != "" {
		if s.Artifacts == nil {
			return nil, status.Error(codes.Unavailable, "artifact storage is not configured")
		}
		a, artifactErr := s.Store.GetArtifactByURI(ctx, principal.TenantID, request.OutputArtifactUri)
		if artifactErr != nil || a.Status != "READY" || a.Kind != "output" || a.TaskRunID == nil || *a.TaskRunID != request.TaskRunId {
			return nil, status.Error(codes.PermissionDenied, "invalid output artifact")
		}
	}
	if request.LogArtifactUri != "" {
		a, artifactErr := s.Store.GetArtifactByURI(ctx, principal.TenantID, request.LogArtifactUri)
		if artifactErr != nil || a.Status != "READY" || a.Kind != "log" || a.TaskRunID == nil || *a.TaskRunID != request.TaskRunId {
			return nil, status.Error(codes.PermissionDenied, "invalid log artifact")
		}
	}
	outputValue := map[string]any{}
	if request.Output != nil {
		outputValue = request.Output.AsMap()
	}
	output, _ := json.Marshal(outputValue)
	task, err := s.Store.CompleteTask(ctx, request.TaskRunId, request.WorkerId, output, request.OutputArtifactUri, request.LogArtifactUri)
	if err != nil {
		return nil, rpcError(err)
	}
	return &runmeshv1.CompleteResponse{Task: snapshot(task)}, nil
}

func (s *WorkerServer) CreateArtifactUpload(ctx context.Context, request *runmeshv1.CreateArtifactUploadRequest) (*runmeshv1.CreateArtifactUploadResponse, error) {
	if s.Artifacts == nil {
		return nil, status.Error(codes.Unavailable, "artifact storage is not configured")
	}
	principal, err := s.principalForTask(ctx, request.TaskRunId)
	if err != nil {
		return nil, err
	}
	upload, err := s.Artifacts.CreateUpload(ctx, principal.TenantID, principal.UserID, request.Kind, request.ContentType, request.SizeBytes, request.ChecksumSha256, &request.TaskRunId)
	if err != nil {
		return nil, rpcError(err)
	}
	return &runmeshv1.CreateArtifactUploadResponse{ArtifactId: upload.Artifact.ID, ArtifactUri: upload.Artifact.ObjectURI, UploadUrl: upload.URL, Headers: upload.Headers, ExpiresAt: timestamppb.New(upload.ExpiresAt)}, nil
}

func (s *WorkerServer) CompleteArtifactUpload(ctx context.Context, request *runmeshv1.CompleteArtifactUploadRequest) (*runmeshv1.CompleteArtifactUploadResponse, error) {
	if s.Artifacts == nil {
		return nil, status.Error(codes.Unavailable, "artifact storage is not configured")
	}
	principal, err := s.principalForTask(ctx, request.TaskRunId)
	if err != nil {
		return nil, err
	}
	pending, err := s.Store.GetArtifact(ctx, principal.TenantID, request.ArtifactId)
	if err != nil || pending.TaskRunID == nil || *pending.TaskRunID != request.TaskRunId {
		return nil, status.Error(codes.PermissionDenied, "invalid task artifact")
	}
	a, err := s.Artifacts.Complete(ctx, principal.TenantID, request.ArtifactId)
	if err != nil {
		return nil, rpcError(err)
	}
	return &runmeshv1.CompleteArtifactUploadResponse{ArtifactUri: a.ObjectURI}, nil
}

func (s *WorkerServer) GetArtifactDownload(ctx context.Context, request *runmeshv1.GetArtifactDownloadRequest) (*runmeshv1.GetArtifactDownloadResponse, error) {
	if s.Artifacts == nil {
		return nil, status.Error(codes.Unavailable, "artifact storage is not configured")
	}
	principal, err := s.principalForTask(ctx, request.TaskRunId)
	if err != nil {
		return nil, err
	}
	download, err := s.Artifacts.Download(ctx, principal.TenantID, request.ArtifactUri)
	if err != nil {
		return nil, rpcError(err)
	}
	return &runmeshv1.GetArtifactDownloadResponse{DownloadUrl: download.URL, ExpiresAt: timestamppb.New(download.ExpiresAt)}, nil
}
func (s *WorkerServer) Fail(ctx context.Context, request *runmeshv1.FailRequest) (*runmeshv1.FailResponse, error) {
	if err := s.authorizeTask(ctx, request.TaskRunId); err != nil {
		return nil, err
	}
	task, err := s.Store.FailTask(ctx, request.TaskRunId, request.WorkerId, storage.Failure{Retryable: request.Retryable, ErrorType: request.ErrorType, ErrorMessage: request.ErrorMessage, TraceID: request.TraceId})
	if err != nil {
		return nil, rpcError(err)
	}
	return &runmeshv1.FailResponse{Task: snapshot(task)}, nil
}
func snapshot(task workflow.TaskRun) *runmeshv1.TaskSnapshot {
	var input map[string]any
	_ = json.Unmarshal(task.Input, &input)
	structured, _ := structpb.NewStruct(input)
	snapshot := &runmeshv1.TaskSnapshot{Id: task.ID, WorkflowRunId: task.WorkflowRunID, TaskKey: task.TaskKey, Handler: task.Handler, Input: structured, Attempt: int32(task.AttemptCount), TimeoutSeconds: int32(task.TimeoutSeconds), Status: task.Status}
	if task.InputArtifactURI != nil {
		snapshot.InputArtifactUri = *task.InputArtifactURI
	}
	return snapshot
}
func rpcError(err error) error {
	if errors.Is(err, storage.ErrLeaseLost) || errors.Is(err, storage.ErrConflict) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	if errors.Is(err, storage.ErrNotFound) {
		return status.Error(codes.NotFound, err.Error())
	}
	return status.Error(codes.Internal, "internal worker protocol error")
}

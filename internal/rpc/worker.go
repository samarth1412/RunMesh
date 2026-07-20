package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	runmeshv1 "github.com/runmesh/runmesh/gen/runmesh/v1"
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
	Token         string
}

func (s *WorkerServer) authorize(ctx context.Context) error {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(values) != 1 || strings.TrimPrefix(values[0], "Bearer ") != s.Token {
		return status.Error(codes.Unauthenticated, "invalid internal token")
	}
	return nil
}
func (s *WorkerServer) Lease(ctx context.Context, request *runmeshv1.LeaseRequest) (*runmeshv1.LeaseResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	task, err := s.Store.LeaseTask(ctx, request.TaskRunId, request.WorkerId, s.LeaseDuration)
	if err != nil {
		return nil, rpcError(err)
	}
	return &runmeshv1.LeaseResponse{Task: snapshot(task)}, nil
}
func (s *WorkerServer) Start(ctx context.Context, request *runmeshv1.StartRequest) (*runmeshv1.StartResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	task, err := s.Store.StartTask(ctx, request.TaskRunId, request.WorkerId)
	if err != nil {
		return nil, rpcError(err)
	}
	return &runmeshv1.StartResponse{Task: snapshot(task)}, nil
}
func (s *WorkerServer) Heartbeat(ctx context.Context, request *runmeshv1.HeartbeatRequest) (*runmeshv1.HeartbeatResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	expires, cancelled, err := s.Store.HeartbeatTask(ctx, request.TaskRunId, request.WorkerId, s.LeaseDuration)
	if err != nil {
		return nil, rpcError(err)
	}
	return &runmeshv1.HeartbeatResponse{LeaseExpiresAt: timestamppb.New(expires), Cancelled: cancelled}, nil
}
func (s *WorkerServer) Complete(ctx context.Context, request *runmeshv1.CompleteRequest) (*runmeshv1.CompleteResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	outputValue := map[string]any{}
	if request.Output != nil {
		outputValue = request.Output.AsMap()
	}
	output, _ := json.Marshal(outputValue)
	task, err := s.Store.CompleteTask(ctx, request.TaskRunId, request.WorkerId, output, request.OutputArtifactUri)
	if err != nil {
		return nil, rpcError(err)
	}
	return &runmeshv1.CompleteResponse{Task: snapshot(task)}, nil
}
func (s *WorkerServer) Fail(ctx context.Context, request *runmeshv1.FailRequest) (*runmeshv1.FailResponse, error) {
	if err := s.authorize(ctx); err != nil {
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
	return &runmeshv1.TaskSnapshot{Id: task.ID, WorkflowRunId: task.WorkflowRunID, TaskKey: task.TaskKey, Handler: task.Handler, Input: structured, Attempt: int32(task.AttemptCount), TimeoutSeconds: int32(task.TimeoutSeconds), Status: task.Status}
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

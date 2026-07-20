package runmesh

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	runmeshv1 "github.com/runmesh/runmesh/gen/runmesh/v1"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"
)

type Task struct {
	ID              string          `json:"id"`
	WorkflowRunID   string          `json:"workflow_run_id"`
	TaskKey         string          `json:"task_key"`
	Handler         string          `json:"handler"`
	Status          string          `json:"status"`
	AttemptCount    int             `json:"attempt_count"`
	MaximumAttempts int             `json:"maximum_attempts"`
	TimeoutSeconds  int             `json:"timeout_seconds"`
	Input           json.RawMessage `json:"input"`
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
	return task(response.Task), nil
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
	structured, err := structpb.NewStruct(object)
	if err != nil {
		return Task{}, err
	}
	response, err := c.worker.Complete(c.rpcContext(ctx), &runmeshv1.CompleteRequest{TaskRunId: id, WorkerId: c.workerID, Output: structured})
	if err != nil {
		return Task{}, err
	}
	return task(response.Task), nil
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
	payload["worker_id"] = c.workerID
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
	return Task{ID: value.Id, WorkflowRunID: value.WorkflowRunId, TaskKey: value.TaskKey, Handler: value.Handler, Status: value.Status, AttemptCount: int(value.Attempt), TimeoutSeconds: int(value.TimeoutSeconds), Input: input}
}

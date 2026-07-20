package rpc

import (
	"encoding/json"
	"testing"

	"github.com/samarth1412/RunMesh/internal/workflow"
)

func TestTaskSnapshotContract(t *testing.T) {
	message := snapshot(workflow.TaskRun{ID: "task", WorkflowRunID: "run", TaskKey: "extract", Handler: "documents.extract", Status: "LEASED", AttemptCount: 2, TimeoutSeconds: 30, Input: json.RawMessage(`{"pages":14}`)})
	if message.Id != "task" || message.Attempt != 2 || message.Input.AsMap()["pages"] != float64(14) {
		t.Fatalf("unexpected snapshot: %v", message)
	}
}

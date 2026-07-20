package workflow

import (
	"encoding/json"
	"time"
)

type TaskSpec struct {
	Handler         string   `json:"handler"`
	DependsOn       []string `json:"depends_on,omitempty"`
	MaximumAttempts int      `json:"maximum_attempts,omitempty"`
	TimeoutSeconds  int      `json:"timeout_seconds,omitempty"`
	Priority        int      `json:"priority,omitempty"`
}

type DAG struct {
	Name  string              `json:"name,omitempty"`
	Tasks map[string]TaskSpec `json:"tasks"`
}

type Definition struct {
	ID        string    `json:"id"`
	TenantID  string    `json:"-"`
	Name      string    `json:"name"`
	Version   int       `json:"version"`
	DAG       DAG       `json:"dag"`
	CreatedAt time.Time `json:"created_at"`
}

type Run struct {
	ID                       string          `json:"id"`
	WorkflowDefinitionID     string          `json:"workflow_definition_id"`
	WorkflowVersion          int             `json:"workflow_version"`
	Status                   string          `json:"status"`
	Input                    json.RawMessage `json:"input"`
	InputArtifactURI         *string         `json:"input_artifact_uri,omitempty"`
	InputArtifactDownloadURL string          `json:"input_artifact_download_url,omitempty"`
	IdempotencyKey           string          `json:"idempotency_key"`
	StartedAt                *time.Time      `json:"started_at,omitempty"`
	CompletedAt              *time.Time      `json:"completed_at,omitempty"`
	CreatedAt                time.Time       `json:"created_at"`
	Tasks                    []TaskRun       `json:"tasks,omitempty"`
}

type TaskRun struct {
	ID                        string          `json:"id"`
	WorkflowRunID             string          `json:"workflow_run_id"`
	TaskKey                   string          `json:"task_key"`
	Handler                   string          `json:"handler"`
	Status                    string          `json:"status"`
	Priority                  int             `json:"priority"`
	AvailableAt               time.Time       `json:"available_at"`
	AttemptCount              int             `json:"attempt_count"`
	MaximumAttempts           int             `json:"maximum_attempts"`
	TimeoutSeconds            int             `json:"timeout_seconds"`
	LeaseOwner                *string         `json:"lease_owner,omitempty"`
	LeaseExpiresAt            *time.Time      `json:"lease_expires_at,omitempty"`
	Input                     json.RawMessage `json:"input"`
	Output                    json.RawMessage `json:"output,omitempty"`
	OutputArtifactURI         *string         `json:"output_artifact_uri,omitempty"`
	InputArtifactURI          *string         `json:"input_artifact_uri,omitempty"`
	LogArtifactURI            *string         `json:"log_artifact_uri,omitempty"`
	OutputArtifactDownloadURL string          `json:"output_artifact_download_url,omitempty"`
	LogArtifactDownloadURL    string          `json:"log_artifact_download_url,omitempty"`
}

type Event struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}

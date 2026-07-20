package storage

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

type Artifact struct {
	ID             string     `json:"id"`
	Kind           string     `json:"kind"`
	Status         string     `json:"status"`
	ObjectURI      string     `json:"artifact_uri"`
	ContentType    string     `json:"content_type"`
	DeclaredSize   int64      `json:"declared_size"`
	SizeBytes      *int64     `json:"size_bytes,omitempty"`
	ChecksumSHA256 string     `json:"checksum_sha256,omitempty"`
	WorkflowRunID  *string    `json:"workflow_run_id,omitempty"`
	TaskRunID      *string    `json:"task_run_id,omitempty"`
	ReadyAt        *time.Time `json:"ready_at,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	ObjectKey      string     `json:"-"`
}

func (s *Store) CreateArtifact(ctx context.Context, id, tenantID, userID, kind, objectKey, objectURI, contentType string, size int64, checksum string, taskRunID *string) (Artifact, error) {
	var artifact Artifact
	err := s.Pool.QueryRow(ctx, `INSERT INTO artifacts(id,tenant_id,kind,object_key,object_uri,content_type,declared_size,checksum_sha256,task_run_id,created_by) VALUES($1,$2,$3,$4,$5,$6,$7,NULLIF($8,''),$9,$10) RETURNING id,kind,status::text,object_key,object_uri,content_type,declared_size,size_bytes,COALESCE(checksum_sha256,''),workflow_run_id,task_run_id,ready_at,created_at`, id, tenantID, kind, objectKey, objectURI, contentType, size, checksum, taskRunID, nullableUUID(userID)).Scan(&artifact.ID, &artifact.Kind, &artifact.Status, &artifact.ObjectKey, &artifact.ObjectURI, &artifact.ContentType, &artifact.DeclaredSize, &artifact.SizeBytes, &artifact.ChecksumSHA256, &artifact.WorkflowRunID, &artifact.TaskRunID, &artifact.ReadyAt, &artifact.CreatedAt)
	return artifact, err
}

func (s *Store) GetArtifact(ctx context.Context, tenantID, id string) (Artifact, error) {
	return s.getArtifact(ctx, `id=$2`, tenantID, id)
}

func (s *Store) GetArtifactByURI(ctx context.Context, tenantID, uri string) (Artifact, error) {
	return s.getArtifact(ctx, `object_uri=$2`, tenantID, uri)
}

func (s *Store) getArtifact(ctx context.Context, predicate, tenantID, value string) (Artifact, error) {
	var artifact Artifact
	query := `SELECT id,kind,status::text,object_key,object_uri,content_type,declared_size,size_bytes,COALESCE(checksum_sha256,''),workflow_run_id,task_run_id,ready_at,created_at FROM artifacts WHERE tenant_id=$1 AND ` + predicate
	err := s.Pool.QueryRow(ctx, query, tenantID, value).Scan(&artifact.ID, &artifact.Kind, &artifact.Status, &artifact.ObjectKey, &artifact.ObjectURI, &artifact.ContentType, &artifact.DeclaredSize, &artifact.SizeBytes, &artifact.ChecksumSHA256, &artifact.WorkflowRunID, &artifact.TaskRunID, &artifact.ReadyAt, &artifact.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return artifact, ErrNotFound
	}
	return artifact, err
}

func (s *Store) CompleteArtifact(ctx context.Context, tenantID, id string, size int64) (Artifact, error) {
	var artifact Artifact
	err := s.Pool.QueryRow(ctx, `UPDATE artifacts SET status='READY',size_bytes=$3,ready_at=now() WHERE id=$1 AND tenant_id=$2 AND status='PENDING' AND declared_size=$3 RETURNING id,kind,status::text,object_key,object_uri,content_type,declared_size,size_bytes,COALESCE(checksum_sha256,''),workflow_run_id,task_run_id,ready_at,created_at`, id, tenantID, size).Scan(&artifact.ID, &artifact.Kind, &artifact.Status, &artifact.ObjectKey, &artifact.ObjectURI, &artifact.ContentType, &artifact.DeclaredSize, &artifact.SizeBytes, &artifact.ChecksumSHA256, &artifact.WorkflowRunID, &artifact.TaskRunID, &artifact.ReadyAt, &artifact.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return artifact, ErrConflict
	}
	return artifact, err
}

func (s *Store) AttachArtifactToRun(ctx context.Context, tenantID, artifactID, runID string) error {
	command, err := s.Pool.Exec(ctx, `UPDATE artifacts SET workflow_run_id=$3 WHERE id=$1 AND tenant_id=$2 AND status='READY' AND workflow_run_id IS NULL`, artifactID, tenantID, runID)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return ErrConflict
	}
	return nil
}

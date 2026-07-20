package storage

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

type APIKey struct {
	ID         string     `json:"id"`
	Name       string     `json:"name"`
	Scopes     []string   `json:"scopes"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
	RevokedAt  *time.Time `json:"revoked_at,omitempty"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
}

func (s *Store) CreateAPIKey(ctx context.Context, id, tenantID, userID, name string, scopes []string, expiresAt *time.Time, hash []byte) (APIKey, error) {
	var key APIKey
	err := s.Pool.QueryRow(ctx, `INSERT INTO api_keys(id,tenant_id,name,key_hash,scopes,expires_at,created_by) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING id,name,scopes,expires_at,revoked_at,last_used_at,created_at`, id, tenantID, name, hash, scopes, expiresAt, nullableUUID(userID)).Scan(&key.ID, &key.Name, &key.Scopes, &key.ExpiresAt, &key.RevokedAt, &key.LastUsedAt, &key.CreatedAt)
	if err != nil {
		return key, err
	}
	_, _ = s.Pool.Exec(ctx, `INSERT INTO audit_events(tenant_id,actor_id,action,resource_type,resource_id,metadata) VALUES($1,$2,'api_key.create','api_key',$3,jsonb_build_object('name',$4,'scopes',$5::text[]))`, tenantID, nullableUUID(userID), id, name, scopes)
	return key, nil
}

func (s *Store) ListAPIKeys(ctx context.Context, tenantID string) ([]APIKey, error) {
	rows, err := s.Pool.Query(ctx, `SELECT id,name,scopes,expires_at,revoked_at,last_used_at,created_at FROM api_keys WHERE tenant_id=$1 ORDER BY created_at DESC`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var keys []APIKey
	for rows.Next() {
		var key APIKey
		if err = rows.Scan(&key.ID, &key.Name, &key.Scopes, &key.ExpiresAt, &key.RevokedAt, &key.LastUsedAt, &key.CreatedAt); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

func (s *Store) RotateAPIKey(ctx context.Context, id, tenantID, userID string, hash []byte) (APIKey, error) {
	var key APIKey
	err := s.Pool.QueryRow(ctx, `UPDATE api_keys SET key_hash=$3,last_used_at=NULL,revoked_at=NULL WHERE id=$1 AND tenant_id=$2 RETURNING id,name,scopes,expires_at,revoked_at,last_used_at,created_at`, id, tenantID, hash).Scan(&key.ID, &key.Name, &key.Scopes, &key.ExpiresAt, &key.RevokedAt, &key.LastUsedAt, &key.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return key, ErrNotFound
	}
	if err == nil {
		_, _ = s.Pool.Exec(ctx, `INSERT INTO audit_events(tenant_id,actor_id,action,resource_type,resource_id) VALUES($1,$2,'api_key.rotate','api_key',$3)`, tenantID, nullableUUID(userID), id)
	}
	return key, err
}

func (s *Store) RevokeAPIKey(ctx context.Context, id, tenantID, userID string) error {
	command, err := s.Pool.Exec(ctx, `UPDATE api_keys SET revoked_at=now() WHERE id=$1 AND tenant_id=$2 AND revoked_at IS NULL`, id, tenantID)
	if err != nil {
		return err
	}
	if command.RowsAffected() == 0 {
		return ErrNotFound
	}
	_, _ = s.Pool.Exec(ctx, `INSERT INTO audit_events(tenant_id,actor_id,action,resource_type,resource_id) VALUES($1,$2,'api_key.revoke','api_key',$3)`, tenantID, nullableUUID(userID), id)
	return nil
}

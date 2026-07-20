ALTER TABLE outbox_events
  ADD COLUMN ordering_key UUID GENERATED ALWAYS AS (
    COALESCE(NULLIF(payload->>'workflow_run_id', '')::UUID, aggregate_id)
  ) STORED,
  ADD COLUMN claim_token UUID,
  ADD COLUMN claim_expires_at TIMESTAMPTZ,
  ADD COLUMN publish_attempts INTEGER NOT NULL DEFAULT 0 CHECK (publish_attempts >= 0);

CREATE INDEX outbox_ordering_head_idx
  ON outbox_events (ordering_key, created_at, id)
  WHERE published_at IS NULL;

CREATE INDEX outbox_claim_expiry_idx
  ON outbox_events (claim_expires_at)
  WHERE published_at IS NULL AND claim_token IS NOT NULL;

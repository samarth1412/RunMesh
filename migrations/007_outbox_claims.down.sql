DROP INDEX IF EXISTS outbox_claim_expiry_idx;
DROP INDEX IF EXISTS outbox_ordering_head_idx;

ALTER TABLE outbox_events
  DROP COLUMN IF EXISTS publish_attempts,
  DROP COLUMN IF EXISTS claim_expires_at,
  DROP COLUMN IF EXISTS claim_token,
  DROP COLUMN IF EXISTS ordering_key;

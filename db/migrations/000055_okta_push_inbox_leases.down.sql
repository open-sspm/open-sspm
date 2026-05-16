DROP INDEX IF EXISTS idx_okta_push_inbox_processing_lease;

ALTER TABLE okta_push_inbox
  DROP CONSTRAINT IF EXISTS okta_push_inbox_claim_token_nonempty,
  DROP CONSTRAINT IF EXISTS okta_push_inbox_claimed_by_nonempty,
  DROP COLUMN IF EXISTS claim_token,
  DROP COLUMN IF EXISTS lease_expires_at,
  DROP COLUMN IF EXISTS claimed_at,
  DROP COLUMN IF EXISTS claimed_by;

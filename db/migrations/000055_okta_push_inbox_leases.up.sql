ALTER TABLE okta_push_inbox
  ADD COLUMN IF NOT EXISTS claimed_by TEXT,
  ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS claim_token TEXT;

ALTER TABLE okta_push_inbox
  ADD CONSTRAINT okta_push_inbox_claimed_by_nonempty
    CHECK (claimed_by IS NULL OR trim(claimed_by) <> ''),
  ADD CONSTRAINT okta_push_inbox_claim_token_nonempty
    CHECK (claim_token IS NULL OR trim(claim_token) <> '');

CREATE INDEX IF NOT EXISTS idx_okta_push_inbox_processing_lease
  ON okta_push_inbox (lease_expires_at, id)
  WHERE status = 'processing';

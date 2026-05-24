ALTER TABLE identities
  ADD COLUMN IF NOT EXISTS resolution_state TEXT NOT NULL DEFAULT 'confirmed',
  ADD COLUMN IF NOT EXISTS identity_kind TEXT NOT NULL DEFAULT 'unknown';

ALTER TABLE identities
  DROP CONSTRAINT IF EXISTS identities_resolution_state_check,
  ADD CONSTRAINT identities_resolution_state_check
    CHECK (resolution_state IN ('confirmed', 'provisional', 'needs_review', 'merged', 'disabled'));

ALTER TABLE identities
  DROP CONSTRAINT IF EXISTS identities_identity_kind_check,
  ADD CONSTRAINT identities_identity_kind_check
    CHECK (identity_kind IN ('human', 'service', 'shared', 'application', 'bot', 'unknown'));

UPDATE identities
SET identity_kind = CASE
  WHEN kind IN ('human', 'service', 'bot') THEN kind
  ELSE 'unknown'
END
WHERE identity_kind = 'unknown';

CREATE TABLE IF NOT EXISTS identity_emails (
  id BIGSERIAL PRIMARY KEY,
  identity_id BIGINT NOT NULL REFERENCES identities(id) ON DELETE CASCADE,
  email TEXT NOT NULL,
  normalized_email TEXT NOT NULL,
  email_kind TEXT NOT NULL DEFAULT 'alias'
    CHECK (email_kind IN ('primary', 'login', 'alias', 'historical', 'external', 'unknown')),
  verification_state TEXT NOT NULL DEFAULT 'observed'
    CHECK (verification_state IN ('verified_authoritative', 'verified_source', 'manual', 'observed', 'inferred_legacy', 'unverified')),
  lifecycle_state TEXT NOT NULL DEFAULT 'active'
    CHECK (lifecycle_state IN ('active', 'historical', 'removed', 'rejected')),
  is_primary BOOLEAN NOT NULL DEFAULT false,
  source_kind TEXT,
  source_name TEXT,
  source_account_id BIGINT REFERENCES accounts(id) ON DELETE SET NULL,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  valid_from TIMESTAMPTZ,
  valid_until TIMESTAMPTZ,
  reviewed_by TEXT,
  reviewed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT identity_emails_normalized_email_nonempty CHECK (normalized_email <> '')
);

CREATE UNIQUE INDEX IF NOT EXISTS identity_emails_one_active_primary
  ON identity_emails(identity_id)
  WHERE is_primary = true
    AND lifecycle_state = 'active';

CREATE UNIQUE INDEX IF NOT EXISTS identity_emails_active_unique_per_identity
  ON identity_emails(identity_id, normalized_email)
  WHERE lifecycle_state = 'active';

CREATE INDEX IF NOT EXISTS identity_emails_lookup_active
  ON identity_emails(normalized_email)
  WHERE lifecycle_state = 'active';

CREATE INDEX IF NOT EXISTS identity_emails_lookup_all
  ON identity_emails(normalized_email, lifecycle_state, verification_state);

INSERT INTO identity_emails (
  identity_id,
  email,
  normalized_email,
  email_kind,
  verification_state,
  lifecycle_state,
  is_primary
)
SELECT
  id,
  primary_email,
  lower(trim(primary_email)),
  'primary',
  'inferred_legacy',
  'active',
  true
FROM identities
WHERE trim(primary_email) <> ''
ON CONFLICT DO NOTHING;

ALTER TABLE identities
  ADD COLUMN IF NOT EXISTS primary_email_id BIGINT REFERENCES identity_emails(id) ON DELETE SET NULL;

UPDATE identities i
SET primary_email_id = ie.id
FROM identity_emails ie
WHERE ie.identity_id = i.id
  AND ie.is_primary
  AND ie.lifecycle_state = 'active'
  AND i.primary_email_id IS NULL;

ALTER TABLE identity_accounts
  ADD COLUMN IF NOT EXISTS link_state TEXT NOT NULL DEFAULT 'accepted',
  ADD COLUMN IF NOT EXISTS resolver_version TEXT,
  ADD COLUMN IF NOT EXISTS linked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS reviewed_by TEXT,
  ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMPTZ;

ALTER TABLE identity_accounts
  DROP CONSTRAINT IF EXISTS identity_accounts_link_state_check,
  ADD CONSTRAINT identity_accounts_link_state_check
    CHECK (link_state IN ('accepted', 'provisional', 'manual_confirmed', 'needs_review'));

UPDATE identity_accounts
SET link_state = CASE
  WHEN lower(trim(link_reason)) IN ('manual', 'manual_merge', 'manual_service', 'manual_shared') THEN 'manual_confirmed'
  WHEN lower(trim(link_reason)) IN ('auto_provisional_ambiguous_email', 'auto_provisional_conflicting_anchor') THEN 'needs_review'
  WHEN lower(trim(link_reason)) IN ('auto_provisional_identity', 'seed_orphan') THEN 'provisional'
  ELSE 'accepted'
END
WHERE link_state = 'accepted';

CREATE TABLE IF NOT EXISTS account_anchors (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  source_kind TEXT NOT NULL,
  source_name TEXT NOT NULL,
  anchor_kind TEXT NOT NULL,
  issuer TEXT NOT NULL,
  anchor_value TEXT NOT NULL,
  normalized_anchor_value TEXT NOT NULL,
  extraction_method TEXT NOT NULL DEFAULT 'connector'
    CHECK (extraction_method IN ('connector', 'profile_field', 'raw_json', 'manual', 'backfill')),
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT account_anchors_normalized_anchor_nonempty CHECK (normalized_anchor_value <> ''),
  UNIQUE (account_id, anchor_kind, issuer, normalized_anchor_value)
);

CREATE INDEX IF NOT EXISTS account_anchors_lookup
  ON account_anchors(anchor_kind, issuer, normalized_anchor_value);

CREATE INDEX IF NOT EXISTS account_anchors_account
  ON account_anchors(account_id);

CREATE TABLE IF NOT EXISTS identity_anchors (
  id BIGSERIAL PRIMARY KEY,
  identity_id BIGINT NOT NULL REFERENCES identities(id) ON DELETE CASCADE,
  anchor_kind TEXT NOT NULL,
  issuer TEXT NOT NULL,
  anchor_value TEXT NOT NULL,
  normalized_anchor_value TEXT NOT NULL,
  source_kind TEXT,
  source_name TEXT,
  source_account_id BIGINT REFERENCES accounts(id) ON DELETE SET NULL,
  trust_level TEXT NOT NULL DEFAULT 'source_observed'
    CHECK (trust_level IN ('authoritative', 'source_observed', 'manual', 'backfilled')),
  lifecycle_state TEXT NOT NULL DEFAULT 'active'
    CHECK (lifecycle_state IN ('active', 'historical', 'revoked', 'rejected')),
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  reviewed_by TEXT,
  reviewed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT identity_anchors_normalized_anchor_nonempty CHECK (normalized_anchor_value <> '')
);

CREATE UNIQUE INDEX IF NOT EXISTS identity_anchors_unique_active_anchor
  ON identity_anchors(anchor_kind, issuer, normalized_anchor_value)
  WHERE lifecycle_state = 'active';

CREATE UNIQUE INDEX IF NOT EXISTS identity_anchors_unique_per_identity
  ON identity_anchors(identity_id, anchor_kind, issuer, normalized_anchor_value)
  WHERE lifecycle_state = 'active';

CREATE INDEX IF NOT EXISTS identity_anchors_identity
  ON identity_anchors(identity_id);

CREATE INDEX IF NOT EXISTS identity_anchors_lookup_all
  ON identity_anchors(anchor_kind, issuer, normalized_anchor_value, lifecycle_state);

CREATE TABLE IF NOT EXISTS identity_match_candidates (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  candidate_identity_id BIGINT NOT NULL REFERENCES identities(id) ON DELETE CASCADE,
  provisional_identity_id BIGINT REFERENCES identities(id) ON DELETE SET NULL,
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'accepted', 'rejected', 'superseded', 'expired')),
  confidence_band TEXT NOT NULL
    CHECK (confidence_band IN ('exact', 'high', 'medium', 'low', 'conflict')),
  score INTEGER NOT NULL,
  match_reason TEXT NOT NULL,
  ambiguity_key TEXT,
  resolver_version TEXT NOT NULL,
  resolver_fingerprint TEXT NOT NULL,
  reviewed_by TEXT,
  reviewed_at TIMESTAMPTZ,
  review_note TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS identity_match_candidates_pending_unique
  ON identity_match_candidates(account_id, candidate_identity_id, resolver_fingerprint)
  WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS identity_match_candidates_queue
  ON identity_match_candidates(status, confidence_band, created_at);

CREATE INDEX IF NOT EXISTS identity_match_candidates_account
  ON identity_match_candidates(account_id);

CREATE TABLE IF NOT EXISTS identity_link_evidence (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  identity_id BIGINT REFERENCES identities(id) ON DELETE CASCADE,
  candidate_id BIGINT REFERENCES identity_match_candidates(id) ON DELETE CASCADE,
  evidence_type TEXT NOT NULL
    CHECK (evidence_type IN (
      'anchor_exact',
      'email_exact_active',
      'email_historical',
      'legacy_primary_email',
      'display_name_similarity',
      'authoritative_source',
      'existing_link',
      'manual_override',
      'negative_service_account',
      'negative_conflicting_anchor',
      'negative_ambiguous_email',
      'negative_account_kind'
    )),
  evidence_key TEXT NOT NULL,
  account_value TEXT,
  identity_value TEXT,
  source_kind TEXT,
  source_name TEXT,
  strength INTEGER NOT NULL CHECK (strength >= -100 AND strength <= 100),
  is_positive BOOLEAN NOT NULL DEFAULT true,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (identity_id IS NOT NULL OR candidate_id IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS identity_link_evidence_account
  ON identity_link_evidence(account_id);

CREATE INDEX IF NOT EXISTS identity_link_evidence_identity
  ON identity_link_evidence(identity_id);

CREATE INDEX IF NOT EXISTS identity_link_evidence_candidate
  ON identity_link_evidence(candidate_id);

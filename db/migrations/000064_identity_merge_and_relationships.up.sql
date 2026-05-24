CREATE TABLE IF NOT EXISTS identity_merge_events (
  id BIGSERIAL PRIMARY KEY,
  source_identity_id BIGINT NOT NULL REFERENCES identities(id),
  target_identity_id BIGINT NOT NULL REFERENCES identities(id),
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'applied', 'rejected', 'failed')),
  reason TEXT NOT NULL,
  requested_by TEXT,
  reviewed_by TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  applied_at TIMESTAMPTZ,
  CONSTRAINT identity_merge_events_no_self_merge CHECK (source_identity_id <> target_identity_id)
);

CREATE INDEX IF NOT EXISTS identity_merge_events_source
  ON identity_merge_events(source_identity_id, status);

CREATE INDEX IF NOT EXISTS identity_merge_events_target
  ON identity_merge_events(target_identity_id, status);

CREATE TABLE IF NOT EXISTS identity_merge_redirects (
  source_identity_id BIGINT PRIMARY KEY REFERENCES identities(id),
  target_identity_id BIGINT NOT NULL REFERENCES identities(id),
  merge_event_id BIGINT NOT NULL REFERENCES identity_merge_events(id),
  merged_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT identity_merge_redirects_no_self_redirect CHECK (source_identity_id <> target_identity_id)
);

CREATE INDEX IF NOT EXISTS identity_merge_redirects_target
  ON identity_merge_redirects(target_identity_id);

CREATE TABLE IF NOT EXISTS account_identity_relationships (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  identity_id BIGINT NOT NULL REFERENCES identities(id) ON DELETE CASCADE,
  relationship_type TEXT NOT NULL
    CHECK (relationship_type IN ('owner', 'custodian', 'approver', 'attributed_user', 'last_observed_user')),
  source_kind TEXT,
  source_name TEXT,
  confidence INTEGER NOT NULL DEFAULT 50
    CHECK (confidence >= 0 AND confidence <= 100),
  lifecycle_state TEXT NOT NULL DEFAULT 'active'
    CHECK (lifecycle_state IN ('active', 'historical', 'rejected')),
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS account_identity_relationships_unique_active
  ON account_identity_relationships(account_id, identity_id, relationship_type)
  WHERE lifecycle_state = 'active';

CREATE INDEX IF NOT EXISTS account_identity_relationships_account
  ON account_identity_relationships(account_id, lifecycle_state);

CREATE INDEX IF NOT EXISTS account_identity_relationships_identity
  ON account_identity_relationships(identity_id, lifecycle_state);

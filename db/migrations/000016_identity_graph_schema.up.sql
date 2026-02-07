-- Identity graph schema foundation: accounts + identities + links + source settings.

ALTER TABLE app_users RENAME TO accounts;

ALTER TABLE accounts
  ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_accounts_source ON accounts (source_kind, source_name);

CREATE INDEX IF NOT EXISTS idx_accounts_source_email_lower_active
  ON accounts (source_kind, source_name, lower(email))
  WHERE expired_at IS NULL
    AND last_observed_run_id IS NOT NULL
    AND email <> '';

CREATE INDEX IF NOT EXISTS idx_accounts_active_last_observed_run_id
  ON accounts (last_observed_run_id)
  WHERE expired_at IS NULL;

CREATE TABLE IF NOT EXISTS identities (
  id BIGSERIAL PRIMARY KEY,
  kind TEXT NOT NULL DEFAULT 'unknown',
  display_name TEXT NOT NULL DEFAULT '',
  primary_email TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT identities_kind_check CHECK (kind IN ('human', 'service', 'bot', 'unknown'))
);

CREATE INDEX IF NOT EXISTS idx_identities_primary_email_lower
  ON identities (lower(primary_email))
  WHERE primary_email <> '';

CREATE TABLE IF NOT EXISTS identity_accounts (
  id BIGSERIAL PRIMARY KEY,
  identity_id BIGINT NOT NULL REFERENCES identities(id) ON DELETE CASCADE,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  link_reason TEXT NOT NULL DEFAULT 'manual',
  confidence REAL NOT NULL DEFAULT 1.0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (account_id)
);

CREATE INDEX IF NOT EXISTS idx_identity_accounts_identity_id ON identity_accounts (identity_id);
CREATE INDEX IF NOT EXISTS idx_identity_accounts_identity_account_cover ON identity_accounts (identity_id, account_id);

CREATE TABLE IF NOT EXISTS identity_source_settings (
  source_kind TEXT NOT NULL,
  source_name TEXT NOT NULL,
  is_authoritative BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (source_kind, source_name)
);

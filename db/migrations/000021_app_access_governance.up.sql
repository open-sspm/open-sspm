-- Programmatic access governance foundation: app assets, ownership, credentials, and audit events.

CREATE TABLE IF NOT EXISTS app_assets (
  id BIGSERIAL PRIMARY KEY,
  source_kind TEXT NOT NULL,
  source_name TEXT NOT NULL,
  asset_kind TEXT NOT NULL,
  external_id TEXT NOT NULL,
  parent_external_id TEXT NOT NULL DEFAULT '',
  display_name TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT '',
  created_at_source TIMESTAMPTZ,
  updated_at_source TIMESTAMPTZ,
  raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  seen_in_run_id BIGINT REFERENCES sync_runs(id),
  seen_at TIMESTAMPTZ,
  last_observed_run_id BIGINT REFERENCES sync_runs(id),
  last_observed_at TIMESTAMPTZ,
  expired_at TIMESTAMPTZ,
  expired_run_id BIGINT REFERENCES sync_runs(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (source_kind, source_name, asset_kind, external_id)
);

CREATE INDEX IF NOT EXISTS idx_app_assets_source_kind_name_kind
  ON app_assets (source_kind, source_name, asset_kind);

CREATE INDEX IF NOT EXISTS idx_app_assets_source_kind_name_external_id
  ON app_assets (source_kind, source_name, external_id);

CREATE INDEX IF NOT EXISTS idx_app_assets_source_kind_name_kind_display_name_active
  ON app_assets (source_kind, source_name, asset_kind, display_name)
  WHERE expired_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_app_assets_active_last_observed_run_id
  ON app_assets (last_observed_run_id)
  WHERE expired_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_app_assets_seen_in_run_id
  ON app_assets (seen_in_run_id);

CREATE TABLE IF NOT EXISTS app_asset_owners (
  id BIGSERIAL PRIMARY KEY,
  app_asset_id BIGINT NOT NULL REFERENCES app_assets(id) ON DELETE CASCADE,
  owner_kind TEXT NOT NULL,
  owner_external_id TEXT NOT NULL,
  owner_display_name TEXT NOT NULL DEFAULT '',
  owner_email TEXT NOT NULL DEFAULT '',
  raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  seen_in_run_id BIGINT REFERENCES sync_runs(id),
  seen_at TIMESTAMPTZ,
  last_observed_run_id BIGINT REFERENCES sync_runs(id),
  last_observed_at TIMESTAMPTZ,
  expired_at TIMESTAMPTZ,
  expired_run_id BIGINT REFERENCES sync_runs(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (app_asset_id, owner_kind, owner_external_id)
);

CREATE INDEX IF NOT EXISTS idx_app_asset_owners_app_asset_id
  ON app_asset_owners (app_asset_id);

CREATE INDEX IF NOT EXISTS idx_app_asset_owners_owner_kind_external_id
  ON app_asset_owners (owner_kind, owner_external_id);

CREATE INDEX IF NOT EXISTS idx_app_asset_owners_app_asset_id_active
  ON app_asset_owners (app_asset_id)
  WHERE expired_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_app_asset_owners_active_last_observed_run_id
  ON app_asset_owners (last_observed_run_id)
  WHERE expired_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_app_asset_owners_seen_in_run_id
  ON app_asset_owners (seen_in_run_id);

CREATE TABLE IF NOT EXISTS credential_artifacts (
  id BIGSERIAL PRIMARY KEY,
  source_kind TEXT NOT NULL,
  source_name TEXT NOT NULL,
  asset_ref_kind TEXT NOT NULL,
  asset_ref_external_id TEXT NOT NULL,
  credential_kind TEXT NOT NULL,
  external_id TEXT NOT NULL,
  display_name TEXT NOT NULL DEFAULT '',
  fingerprint TEXT NOT NULL DEFAULT '',
  scope_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT '',
  created_at_source TIMESTAMPTZ,
  expires_at_source TIMESTAMPTZ,
  last_used_at_source TIMESTAMPTZ,
  created_by_kind TEXT NOT NULL DEFAULT '',
  created_by_external_id TEXT NOT NULL DEFAULT '',
  created_by_display_name TEXT NOT NULL DEFAULT '',
  approved_by_kind TEXT NOT NULL DEFAULT '',
  approved_by_external_id TEXT NOT NULL DEFAULT '',
  approved_by_display_name TEXT NOT NULL DEFAULT '',
  raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  seen_in_run_id BIGINT REFERENCES sync_runs(id),
  seen_at TIMESTAMPTZ,
  last_observed_run_id BIGINT REFERENCES sync_runs(id),
  last_observed_at TIMESTAMPTZ,
  expired_at TIMESTAMPTZ,
  expired_run_id BIGINT REFERENCES sync_runs(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (source_kind, source_name, credential_kind, external_id, asset_ref_kind, asset_ref_external_id)
);

CREATE INDEX IF NOT EXISTS idx_credential_artifacts_source_kind_name_kind
  ON credential_artifacts (source_kind, source_name, credential_kind);

CREATE INDEX IF NOT EXISTS idx_credential_artifacts_source_kind_name_asset_ref
  ON credential_artifacts (source_kind, source_name, asset_ref_kind, asset_ref_external_id);

CREATE INDEX IF NOT EXISTS idx_credential_artifacts_active_expires_at
  ON credential_artifacts (expires_at_source)
  WHERE expired_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_credential_artifacts_created_by_external_id
  ON credential_artifacts (created_by_external_id);

CREATE INDEX IF NOT EXISTS idx_credential_artifacts_approved_by_external_id
  ON credential_artifacts (approved_by_external_id);

CREATE INDEX IF NOT EXISTS idx_credential_artifacts_active_last_observed_run_id
  ON credential_artifacts (last_observed_run_id)
  WHERE expired_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_credential_artifacts_seen_in_run_id
  ON credential_artifacts (seen_in_run_id);

CREATE TABLE IF NOT EXISTS credential_audit_events (
  id BIGSERIAL PRIMARY KEY,
  source_kind TEXT NOT NULL,
  source_name TEXT NOT NULL,
  event_external_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  event_time TIMESTAMPTZ NOT NULL,
  actor_kind TEXT NOT NULL DEFAULT '',
  actor_external_id TEXT NOT NULL DEFAULT '',
  actor_display_name TEXT NOT NULL DEFAULT '',
  target_kind TEXT NOT NULL DEFAULT '',
  target_external_id TEXT NOT NULL DEFAULT '',
  target_display_name TEXT NOT NULL DEFAULT '',
  credential_kind TEXT NOT NULL DEFAULT '',
  credential_external_id TEXT NOT NULL DEFAULT '',
  raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (source_kind, source_name, event_external_id)
);

CREATE INDEX IF NOT EXISTS idx_credential_audit_events_source_kind_name_event_time_desc
  ON credential_audit_events (source_kind, source_name, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_credential_audit_events_target_kind_external_id
  ON credential_audit_events (target_kind, target_external_id);

CREATE INDEX IF NOT EXISTS idx_credential_audit_events_actor_kind_external_id
  ON credential_audit_events (actor_kind, actor_external_id);

CREATE INDEX IF NOT EXISTS idx_credential_audit_events_credential_kind_external_id
  ON credential_audit_events (credential_kind, credential_external_id);

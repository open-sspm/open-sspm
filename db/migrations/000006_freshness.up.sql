-- Data freshness and sync correctness metadata

-- Sync run outcomes and UI-safe stats
ALTER TABLE sync_runs
  ADD COLUMN IF NOT EXISTS stats JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS error_kind TEXT NOT NULL DEFAULT '';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'sync_runs_status_check'
  ) THEN
    ALTER TABLE sync_runs
      ADD CONSTRAINT sync_runs_status_check
        CHECK (status IN ('running', 'success', 'error', 'canceled'));
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'sync_runs_running_finished_at_check'
  ) THEN
    ALTER TABLE sync_runs
      ADD CONSTRAINT sync_runs_running_finished_at_check
        CHECK ((status = 'running') = (finished_at IS NULL));
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_sync_runs_source_finished_at_desc
  ON sync_runs (source_kind, source_name, finished_at DESC);

CREATE INDEX IF NOT EXISTS idx_sync_runs_source_status_finished_at_desc
  ON sync_runs (source_kind, source_name, status, finished_at DESC);

-- Row-level freshness metadata for synced fact tables
ALTER TABLE idp_users
  ADD COLUMN IF NOT EXISTS seen_in_run_id BIGINT REFERENCES sync_runs(id),
  ADD COLUMN IF NOT EXISTS seen_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_observed_run_id BIGINT REFERENCES sync_runs(id),
  ADD COLUMN IF NOT EXISTS last_observed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS expired_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS expired_run_id BIGINT REFERENCES sync_runs(id);

CREATE INDEX IF NOT EXISTS idx_idp_users_seen_in_run_id ON idp_users (seen_in_run_id);
CREATE INDEX IF NOT EXISTS idx_idp_users_active_last_observed_run_id ON idp_users (last_observed_run_id) WHERE expired_at IS NULL;

ALTER TABLE okta_groups
  ADD COLUMN IF NOT EXISTS seen_in_run_id BIGINT REFERENCES sync_runs(id),
  ADD COLUMN IF NOT EXISTS seen_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_observed_run_id BIGINT REFERENCES sync_runs(id),
  ADD COLUMN IF NOT EXISTS last_observed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS expired_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS expired_run_id BIGINT REFERENCES sync_runs(id);

CREATE INDEX IF NOT EXISTS idx_okta_groups_seen_in_run_id ON okta_groups (seen_in_run_id);
CREATE INDEX IF NOT EXISTS idx_okta_groups_active_last_observed_run_id ON okta_groups (last_observed_run_id) WHERE expired_at IS NULL;

ALTER TABLE okta_user_groups
  ADD COLUMN IF NOT EXISTS seen_in_run_id BIGINT REFERENCES sync_runs(id),
  ADD COLUMN IF NOT EXISTS seen_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_observed_run_id BIGINT REFERENCES sync_runs(id),
  ADD COLUMN IF NOT EXISTS last_observed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS expired_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS expired_run_id BIGINT REFERENCES sync_runs(id);

CREATE INDEX IF NOT EXISTS idx_okta_user_groups_seen_in_run_id ON okta_user_groups (seen_in_run_id);
CREATE INDEX IF NOT EXISTS idx_okta_user_groups_active_last_observed_run_id ON okta_user_groups (last_observed_run_id) WHERE expired_at IS NULL;

ALTER TABLE okta_apps
  ADD COLUMN IF NOT EXISTS seen_in_run_id BIGINT REFERENCES sync_runs(id),
  ADD COLUMN IF NOT EXISTS seen_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_observed_run_id BIGINT REFERENCES sync_runs(id),
  ADD COLUMN IF NOT EXISTS last_observed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS expired_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS expired_run_id BIGINT REFERENCES sync_runs(id);

CREATE INDEX IF NOT EXISTS idx_okta_apps_seen_in_run_id ON okta_apps (seen_in_run_id);
CREATE INDEX IF NOT EXISTS idx_okta_apps_active_last_observed_run_id ON okta_apps (last_observed_run_id) WHERE expired_at IS NULL;

ALTER TABLE okta_user_app_assignments
  ADD COLUMN IF NOT EXISTS seen_in_run_id BIGINT REFERENCES sync_runs(id),
  ADD COLUMN IF NOT EXISTS seen_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_observed_run_id BIGINT REFERENCES sync_runs(id),
  ADD COLUMN IF NOT EXISTS last_observed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS expired_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS expired_run_id BIGINT REFERENCES sync_runs(id);

CREATE INDEX IF NOT EXISTS idx_okta_user_app_assignments_seen_in_run_id ON okta_user_app_assignments (seen_in_run_id);
CREATE INDEX IF NOT EXISTS idx_okta_user_app_assignments_active_last_observed_run_id ON okta_user_app_assignments (last_observed_run_id) WHERE expired_at IS NULL;

ALTER TABLE okta_app_group_assignments
  ADD COLUMN IF NOT EXISTS seen_in_run_id BIGINT REFERENCES sync_runs(id),
  ADD COLUMN IF NOT EXISTS seen_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_observed_run_id BIGINT REFERENCES sync_runs(id),
  ADD COLUMN IF NOT EXISTS last_observed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS expired_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS expired_run_id BIGINT REFERENCES sync_runs(id);

CREATE INDEX IF NOT EXISTS idx_okta_app_group_assignments_seen_in_run_id ON okta_app_group_assignments (seen_in_run_id);
CREATE INDEX IF NOT EXISTS idx_okta_app_group_assignments_active_last_observed_run_id ON okta_app_group_assignments (last_observed_run_id) WHERE expired_at IS NULL;

ALTER TABLE app_users
  ADD COLUMN IF NOT EXISTS seen_in_run_id BIGINT REFERENCES sync_runs(id),
  ADD COLUMN IF NOT EXISTS seen_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_observed_run_id BIGINT REFERENCES sync_runs(id),
  ADD COLUMN IF NOT EXISTS last_observed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS expired_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS expired_run_id BIGINT REFERENCES sync_runs(id);

CREATE INDEX IF NOT EXISTS idx_app_users_seen_in_run_id ON app_users (seen_in_run_id);
CREATE INDEX IF NOT EXISTS idx_app_users_active_last_observed_run_id ON app_users (last_observed_run_id) WHERE expired_at IS NULL;

ALTER TABLE entitlements
  ADD COLUMN IF NOT EXISTS seen_in_run_id BIGINT REFERENCES sync_runs(id),
  ADD COLUMN IF NOT EXISTS seen_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_observed_run_id BIGINT REFERENCES sync_runs(id),
  ADD COLUMN IF NOT EXISTS last_observed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS expired_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS expired_run_id BIGINT REFERENCES sync_runs(id),
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS idx_entitlements_seen_in_run_id ON entitlements (seen_in_run_id);
CREATE INDEX IF NOT EXISTS idx_entitlements_active_last_observed_run_id ON entitlements (last_observed_run_id) WHERE expired_at IS NULL;

-- Deduplicate existing entitlements before adding deterministic key.
DELETE FROM entitlements e
USING entitlements e2
WHERE e.id < e2.id
  AND e.app_user_id = e2.app_user_id
  AND e.kind = e2.kind
  AND e.resource = e2.resource
  AND e.permission = e2.permission;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'entitlements_dedupe_key'
  ) THEN
    ALTER TABLE entitlements
      ADD CONSTRAINT entitlements_dedupe_key UNIQUE (app_user_id, kind, resource, permission);
  END IF;
END
$$;


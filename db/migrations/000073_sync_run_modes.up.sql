ALTER TABLE sync_runs
  ADD COLUMN IF NOT EXISTS run_mode TEXT NOT NULL DEFAULT 'full';

UPDATE sync_runs
SET
  run_mode = CASE
    WHEN lower(trim(source_kind)) IN ('okta_discovery', 'entra_discovery', 'google_workspace_discovery') THEN 'discovery'
    WHEN lower(trim(source_kind)) LIKE '%\_tail' ESCAPE '\' THEN 'tail'
    WHEN lower(trim(source_kind)) = 'okta_push' THEN 'event_inbox'
    ELSE run_mode
  END,
  source_kind = CASE
    WHEN lower(trim(source_kind)) = 'okta_discovery' THEN 'okta'
    WHEN lower(trim(source_kind)) = 'entra_discovery' THEN 'entra'
    WHEN lower(trim(source_kind)) = 'google_workspace_discovery' THEN 'google_workspace'
    WHEN lower(trim(source_kind)) LIKE '%\_tail' ESCAPE '\' THEN regexp_replace(lower(trim(source_kind)), '_tail$', '')
    WHEN lower(trim(source_kind)) = 'okta_push' THEN 'okta'
    ELSE lower(trim(source_kind))
  END
WHERE lower(trim(source_kind)) IN ('okta_discovery', 'entra_discovery', 'google_workspace_discovery')
   OR lower(trim(source_kind)) LIKE '%\_tail' ESCAPE '\'
   OR lower(trim(source_kind)) = 'okta_push';

ALTER TABLE sync_runs
  DROP CONSTRAINT IF EXISTS sync_runs_run_mode_check;

ALTER TABLE sync_runs
  ADD CONSTRAINT sync_runs_run_mode_check
  CHECK (run_mode IN ('full', 'discovery', 'tail', 'event_inbox'));

CREATE INDEX IF NOT EXISTS idx_sync_runs_source_mode_finished_at_desc
  ON sync_runs (source_kind, source_name, run_mode, finished_at DESC);

CREATE INDEX IF NOT EXISTS idx_sync_runs_source_mode_status_finished_at_desc
  ON sync_runs (source_kind, source_name, run_mode, status, finished_at DESC);

DROP INDEX IF EXISTS idx_sync_runs_success_latest_by_source;

CREATE INDEX IF NOT EXISTS idx_sync_runs_success_latest_by_source
  ON sync_runs (
    (
      CASE lower(trim(source_kind))
        WHEN 'aws_identity_center' THEN 'aws'
        ELSE lower(trim(source_kind))
      END
    ),
    (trim(source_name)),
    finished_at DESC
  )
  WHERE status = 'success'
    AND run_mode = 'full'
    AND finished_at IS NOT NULL;

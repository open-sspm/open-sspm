DROP INDEX IF EXISTS idx_sync_runs_source_mode_status_finished_at_desc;
DROP INDEX IF EXISTS idx_sync_runs_source_mode_finished_at_desc;

UPDATE sync_runs
SET source_kind = CASE run_mode
  WHEN 'discovery' THEN source_kind || '_discovery'
  WHEN 'tail' THEN source_kind || '_tail'
  WHEN 'event_inbox' THEN CASE
    WHEN source_kind = 'okta' THEN 'okta_push'
    ELSE source_kind
  END
  ELSE source_kind
END
WHERE run_mode IN ('discovery', 'tail', 'event_inbox');

ALTER TABLE sync_runs
  DROP CONSTRAINT IF EXISTS sync_runs_run_mode_check;

ALTER TABLE sync_runs
  DROP COLUMN IF EXISTS run_mode;

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
    AND finished_at IS NOT NULL
    AND lower(trim(source_kind)) NOT IN ('okta_discovery', 'entra_discovery', 'google_workspace_discovery', 'okta_push');

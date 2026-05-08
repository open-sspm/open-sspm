ALTER TABLE saas_app_events
  DROP CONSTRAINT IF EXISTS saas_app_events_signal_kind_check;

DELETE FROM saas_app_events
WHERE signal_kind = 'app_assignment';

ALTER TABLE saas_app_events
  ADD CONSTRAINT saas_app_events_signal_kind_check
  CHECK (signal_kind IN ('idp_sso', 'oauth_grant'));

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
    AND lower(trim(source_kind)) NOT IN ('okta_discovery', 'entra_discovery', 'google_workspace_discovery');

CREATE INDEX IF NOT EXISTS idx_sync_runs_success_latest_by_source
  ON sync_runs (
    (
      CASE lower(trim(source_kind))
        WHEN 'okta_discovery' THEN 'okta'
        WHEN 'entra_discovery' THEN 'entra'
        WHEN 'google_workspace_discovery' THEN 'google_workspace'
        WHEN 'aws_identity_center' THEN 'aws'
        ELSE lower(trim(source_kind))
      END
    ),
    (trim(source_name)),
    finished_at DESC
  )
  WHERE status = 'success' AND finished_at IS NOT NULL;

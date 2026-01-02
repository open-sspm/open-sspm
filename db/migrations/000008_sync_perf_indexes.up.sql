-- Sync performance helper indexes

CREATE INDEX IF NOT EXISTS idx_idp_users_active_email_lower
  ON idp_users (lower(email))
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_app_users_active_email_lower
  ON app_users (lower(email))
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_app_users_active_source_kind_name
  ON app_users (source_kind, source_name)
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;


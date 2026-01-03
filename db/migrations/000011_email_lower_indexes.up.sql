-- Indexes to speed up auto-linking by email (case-insensitive joins)

CREATE INDEX IF NOT EXISTS idx_idp_users_email_lower_active
  ON idp_users (lower(email))
  WHERE expired_at IS NULL
    AND last_observed_run_id IS NOT NULL
    AND email <> '';

CREATE INDEX IF NOT EXISTS idx_app_users_source_email_lower_active
  ON app_users (source_kind, source_name, lower(email))
  WHERE expired_at IS NULL
    AND last_observed_run_id IS NOT NULL
    AND email <> '';


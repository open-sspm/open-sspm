CREATE EXTENSION IF NOT EXISTS pg_trgm;

ALTER TABLE saas_app_events
  ADD COLUMN IF NOT EXISTS has_privileged_scope BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS has_confidential_scope BOOLEAN NOT NULL DEFAULT false;

UPDATE saas_app_events
SET
  has_privileged_scope = (
    lower(scopes_json::text) LIKE '%directory.readwrite.all%'
    OR lower(scopes_json::text) LIKE '%application.readwrite.all%'
    OR lower(scopes_json::text) LIKE '%rolemanagement.readwrite.directory%'
    OR lower(scopes_json::text) LIKE '%mailboxsettings.readwrite%'
    OR lower(scopes_json::text) LIKE '%full_access_as_app%'
    OR lower(scopes_json::text) LIKE '%files.readwrite.all%'
    OR lower(scopes_json::text) LIKE '%files.readwrite%'
    OR lower(scopes_json::text) LIKE '%sites.readwrite.all%'
    OR lower(scopes_json::text) LIKE '%user.readwrite.all%'
    OR lower(scopes_json::text) LIKE '%offline_access%'
  ),
  has_confidential_scope = (
    lower(scopes_json::text) LIKE '%mail.%'
    OR lower(scopes_json::text) LIKE '%files.%'
    OR lower(scopes_json::text) LIKE '%calendar.%'
    OR lower(scopes_json::text) LIKE '%readwrite%'
    OR lower(scopes_json::text) LIKE '%sites.read%'
  )
WHERE scopes_json <> '[]'::jsonb;

ALTER TABLE credential_artifacts
  ADD COLUMN IF NOT EXISTS lineage_key TEXT NOT NULL DEFAULT '';

UPDATE credential_artifacts
SET lineage_key = md5(
  coalesce(source_kind, '') || '|' ||
  coalesce(source_name, '') || '|' ||
  coalesce(asset_ref_kind, '') || '|' ||
  coalesce(asset_ref_external_id, '') || '|' ||
  coalesce(credential_kind, '') || '|' ||
  CASE
    WHEN coalesce(display_name, '') ~ '\s*\[\d{4}\]\s*$' THEN
      'cohort:' || lower(trim(regexp_replace(coalesce(display_name, ''), '\s*\[\d{4}\]\s*$', '')))
    ELSE
      'id:' || coalesce(external_id, '') || '|' || lower(trim(coalesce(display_name, '')))
  END
)
WHERE lineage_key = '';

ALTER TABLE credential_artifacts
  ADD CONSTRAINT credential_artifacts_lineage_key_nonempty
  CHECK (lineage_key <> '');

CREATE INDEX IF NOT EXISTS idx_identities_trgm_primary_email
  ON identities USING GIN (primary_email gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_identities_trgm_display_name
  ON identities USING GIN (display_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_accounts_trgm_external_id_active
  ON accounts USING GIN (external_id gin_trgm_ops)
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_accounts_trgm_email_active
  ON accounts USING GIN (email gin_trgm_ops)
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_accounts_trgm_display_name_active
  ON accounts USING GIN (display_name gin_trgm_ops)
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_app_assets_trgm_display_name_active
  ON app_assets USING GIN (display_name gin_trgm_ops)
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_app_assets_trgm_external_id_active
  ON app_assets USING GIN (external_id gin_trgm_ops)
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_app_assets_trgm_parent_external_active
  ON app_assets USING GIN (parent_external_id gin_trgm_ops)
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_saas_apps_trgm_display_name
  ON saas_apps USING GIN (display_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_saas_apps_trgm_primary_domain
  ON saas_apps USING GIN (primary_domain gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_saas_apps_trgm_vendor_name
  ON saas_apps USING GIN (vendor_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_saas_apps_trgm_canonical_key
  ON saas_apps USING GIN (canonical_key gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_credential_artifacts_trgm_display_active
  ON credential_artifacts USING GIN (display_name gin_trgm_ops)
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_credential_artifacts_trgm_external_active
  ON credential_artifacts USING GIN (external_id gin_trgm_ops)
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_credential_artifacts_trgm_asset_ref_active
  ON credential_artifacts USING GIN (asset_ref_external_id gin_trgm_ops)
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_credential_artifacts_trgm_created_ext_active
  ON credential_artifacts USING GIN (created_by_external_id gin_trgm_ops)
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_credential_artifacts_trgm_created_name_active
  ON credential_artifacts USING GIN (created_by_display_name gin_trgm_ops)
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_credential_artifacts_trgm_approved_ext_active
  ON credential_artifacts USING GIN (approved_by_external_id gin_trgm_ops)
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_credential_artifacts_trgm_approved_name_active
  ON credential_artifacts USING GIN (approved_by_display_name gin_trgm_ops)
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_okta_apps_trgm_label_active
  ON okta_apps USING GIN (label gin_trgm_ops)
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_okta_apps_trgm_name_active
  ON okta_apps USING GIN (name gin_trgm_ops)
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_okta_apps_trgm_external_id_active
  ON okta_apps USING GIN (external_id gin_trgm_ops)
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_okta_groups_trgm_name_active
  ON okta_groups USING GIN (name gin_trgm_ops)
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_okta_groups_trgm_external_id_active
  ON okta_groups USING GIN (external_id gin_trgm_ops)
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_non_human_principals_trgm_display
  ON non_human_principals USING GIN (display_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_non_human_principals_trgm_secondary
  ON non_human_principals USING GIN (secondary_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_non_human_principals_trgm_owner_name
  ON non_human_principals USING GIN (accountable_owner_display_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_non_human_principals_trgm_owner_email
  ON non_human_principals USING GIN (accountable_owner_primary_email gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_accounts_norm_source_external_active
  ON accounts (lower(trim(source_kind)), lower(trim(source_name)), lower(trim(external_id)))
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_identity_emails_identity_id
  ON identity_emails (identity_id);

CREATE INDEX IF NOT EXISTS idx_connector_source_state_norm_source
  ON connector_source_state (lower(trim(source_kind)), lower(trim(source_name)));

CREATE INDEX IF NOT EXISTS idx_saas_app_sources_norm_source_active
  ON saas_app_sources (lower(trim(source_kind)), lower(trim(source_name)), saas_app_id)
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_saas_app_sources_expire_stale
  ON saas_app_sources (source_kind, source_name, COALESCE(last_observed_at, seen_at, created_at))
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_saas_app_events_active_app_observed
  ON saas_app_events (saas_app_id, observed_at DESC)
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_credential_artifacts_norm_source_active
  ON credential_artifacts (lower(trim(source_kind)), lower(trim(source_name)))
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_app_assets_norm_source_active
  ON app_assets (lower(trim(source_kind)), lower(trim(source_name)))
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_non_human_principals_norm_source
  ON non_human_principals (lower(trim(source_kind)), lower(trim(source_name)));

CREATE INDEX IF NOT EXISTS idx_events_discovery_projection_cursor
  ON events (source_kind, source_name, received_at ASC, id ASC)
  WHERE category LIKE 'discovery.%'
    AND trim(provider_event_id) <> '';

CREATE INDEX IF NOT EXISTS idx_entitlements_active_kind_resource_account
  ON entitlements (kind, resource, app_user_id)
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_entitlements_active_resource_account
  ON entitlements (resource, app_user_id)
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_credential_artifacts_active_lineage
  ON credential_artifacts (
    source_kind,
    source_name,
    lineage_key,
    expires_at_source DESC NULLS LAST,
    created_at DESC,
    id DESC
  )
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_sync_jobs_claim_manual_priority
  ON sync_jobs (
    lane,
    (CASE WHEN trigger_kind = 'manual' THEN 0 ELSE 1 END),
    priority DESC,
    available_at ASC,
    created_at ASC,
    id ASC
  )
  WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_sync_jobs_manual_active_status
  ON sync_jobs (trigger_kind, status)
  WHERE trigger_kind = 'manual'
    AND status IN ('pending', 'claimed', 'running');

CREATE INDEX IF NOT EXISTS idx_sync_jobs_manual_terminal_latest
  ON sync_jobs (
    (COALESCE(finished_at, updated_at, created_at)) DESC,
    created_at DESC,
    id DESC
  )
  WHERE trigger_kind = 'manual'
    AND status IN ('succeeded', 'failed');

CREATE INDEX IF NOT EXISTS idx_okta_push_inbox_queued_id
  ON okta_push_inbox (id)
  WHERE status = 'queued';

CREATE INDEX IF NOT EXISTS idx_riskpolicy_shadow_signals_projection
  ON riskpolicy_event_shadow_signals (
    evaluated_at ASC,
    event_received_at ASC,
    event_id ASC,
    signal_id ASC
  );

CREATE INDEX IF NOT EXISTS idx_non_human_access_events_admin_occurred
  ON non_human_access_events (occurred_at DESC, auth_user_id)
  WHERE auth_user_role = 'admin';

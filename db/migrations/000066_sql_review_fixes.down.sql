DROP INDEX IF EXISTS idx_non_human_access_events_admin_occurred;
DROP INDEX IF EXISTS idx_riskpolicy_shadow_signals_projection;
DROP INDEX IF EXISTS idx_okta_push_inbox_queued_id;
DROP INDEX IF EXISTS idx_sync_jobs_manual_terminal_latest;
DROP INDEX IF EXISTS idx_sync_jobs_manual_active_status;
DROP INDEX IF EXISTS idx_sync_jobs_claim_manual_priority;
DROP INDEX IF EXISTS idx_credential_artifacts_active_lineage;
DROP INDEX IF EXISTS idx_entitlements_active_resource_account;
DROP INDEX IF EXISTS idx_entitlements_active_kind_resource_account;
DROP INDEX IF EXISTS idx_events_discovery_projection_cursor;
DROP INDEX IF EXISTS idx_non_human_principals_norm_source;
DROP INDEX IF EXISTS idx_app_assets_norm_source_active;
DROP INDEX IF EXISTS idx_credential_artifacts_norm_source_active;
DROP INDEX IF EXISTS idx_saas_app_events_active_app_observed;
DROP INDEX IF EXISTS idx_saas_app_sources_expire_stale;
DROP INDEX IF EXISTS idx_saas_app_sources_norm_source_active;
DROP INDEX IF EXISTS idx_connector_source_state_norm_source;
DROP INDEX IF EXISTS idx_identity_emails_identity_id;
DROP INDEX IF EXISTS idx_accounts_norm_source_external_active;
DROP INDEX IF EXISTS idx_non_human_principals_trgm_owner_email;
DROP INDEX IF EXISTS idx_non_human_principals_trgm_owner_name;
DROP INDEX IF EXISTS idx_non_human_principals_trgm_secondary;
DROP INDEX IF EXISTS idx_non_human_principals_trgm_display;
DROP INDEX IF EXISTS idx_okta_groups_trgm_external_id_active;
DROP INDEX IF EXISTS idx_okta_groups_trgm_name_active;
DROP INDEX IF EXISTS idx_okta_apps_trgm_external_id_active;
DROP INDEX IF EXISTS idx_okta_apps_trgm_name_active;
DROP INDEX IF EXISTS idx_okta_apps_trgm_label_active;
DROP INDEX IF EXISTS idx_credential_artifacts_trgm_approved_name_active;
DROP INDEX IF EXISTS idx_credential_artifacts_trgm_approved_ext_active;
DROP INDEX IF EXISTS idx_credential_artifacts_trgm_created_name_active;
DROP INDEX IF EXISTS idx_credential_artifacts_trgm_created_ext_active;
DROP INDEX IF EXISTS idx_credential_artifacts_trgm_asset_ref_active;
DROP INDEX IF EXISTS idx_credential_artifacts_trgm_external_active;
DROP INDEX IF EXISTS idx_credential_artifacts_trgm_display_active;
DROP INDEX IF EXISTS idx_saas_apps_trgm_canonical_key;
DROP INDEX IF EXISTS idx_saas_apps_trgm_vendor_name;
DROP INDEX IF EXISTS idx_saas_apps_trgm_primary_domain;
DROP INDEX IF EXISTS idx_saas_apps_trgm_display_name;
DROP INDEX IF EXISTS idx_app_assets_trgm_parent_external_active;
DROP INDEX IF EXISTS idx_app_assets_trgm_external_id_active;
DROP INDEX IF EXISTS idx_app_assets_trgm_display_name_active;
DROP INDEX IF EXISTS idx_accounts_trgm_display_name_active;
DROP INDEX IF EXISTS idx_accounts_trgm_email_active;
DROP INDEX IF EXISTS idx_accounts_trgm_external_id_active;
DROP INDEX IF EXISTS idx_identities_trgm_display_name;
DROP INDEX IF EXISTS idx_identities_trgm_primary_email;

ALTER TABLE credential_artifacts
  DROP CONSTRAINT IF EXISTS credential_artifacts_lineage_key_nonempty;

ALTER TABLE credential_artifacts
  DROP COLUMN IF EXISTS lineage_key;

ALTER TABLE saas_app_events
  DROP COLUMN IF EXISTS has_confidential_scope,
  DROP COLUMN IF EXISTS has_privileged_scope;

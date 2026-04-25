DROP VIEW IF EXISTS non_human_principal_read_models_v;

ALTER TABLE non_human_principals
  ADD COLUMN IF NOT EXISTS risk_signals_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS policy_packs_json JSONB NOT NULL DEFAULT '[]'::jsonb;

CREATE INDEX IF NOT EXISTS idx_non_human_principals_source_risk_priority
  ON non_human_principals (source_kind, source_name, risk_level, owner_presence, governance_state, freshness_state);

CREATE OR REPLACE VIEW non_human_principal_read_models_v AS
SELECT
  nhp.principal_ref,
  nhp.identity_id,
  nhp.app_asset_id,
  nhp.principal_type,
  nhp.source_kind,
  nhp.source_name,
  nhp.display_name,
  nhp.secondary_name,
  nhp.linked_assets_count,
  nhp.linked_credentials_count,
  nhp.last_seen_at,
  nhp.activity_state,
  nhp.freshness_state,
  nhp.governance_state,
  nhp.accountable_owner_identity_id,
  nhp.accountable_owner_display_name,
  nhp.accountable_owner_primary_email,
  nhp.owner_presence,
  nhp.has_critical_credential,
  nhp.has_high_risk_credential,
  nhp.has_expired_credential,
  nhp.has_expiring_credential,
  nhp.has_unused_credential,
  nhp.has_stale_evidence,
  nhp.risk_reason_count,
  nhp.risk_level,
  nhp.risk_signals_json,
  nhp.policy_packs_json
FROM non_human_principals nhp;

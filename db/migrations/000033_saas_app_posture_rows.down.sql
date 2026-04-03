ALTER TABLE saas_apps
  ADD COLUMN IF NOT EXISTS managed_state TEXT NOT NULL DEFAULT 'unmanaged',
  ADD COLUMN IF NOT EXISTS managed_reason TEXT NOT NULL DEFAULT 'no_binding',
  ADD COLUMN IF NOT EXISTS bound_connector_kind TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS bound_connector_source_name TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS risk_score INT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS risk_level TEXT NOT NULL DEFAULT 'low',
  ADD COLUMN IF NOT EXISTS suggested_business_criticality TEXT NOT NULL DEFAULT 'unknown',
  ADD COLUMN IF NOT EXISTS suggested_data_classification TEXT NOT NULL DEFAULT 'unknown';

ALTER TABLE saas_apps
  ADD CONSTRAINT saas_apps_managed_state_check CHECK (managed_state IN ('managed', 'unmanaged')),
  ADD CONSTRAINT saas_apps_managed_reason_check CHECK (managed_reason IN ('active_binding_fresh_sync', 'no_binding', 'connector_disabled', 'connector_not_configured', 'stale_sync')),
  ADD CONSTRAINT saas_apps_risk_score_range_check CHECK (risk_score >= 0 AND risk_score <= 100),
  ADD CONSTRAINT saas_apps_risk_level_check CHECK (risk_level IN ('low', 'medium', 'high', 'critical')),
  ADD CONSTRAINT saas_apps_suggested_business_criticality_check CHECK (suggested_business_criticality IN ('unknown', 'low', 'medium', 'high', 'critical')),
  ADD CONSTRAINT saas_apps_suggested_data_classification_check CHECK (suggested_data_classification IN ('unknown', 'public', 'internal', 'confidential', 'restricted'));

CREATE INDEX IF NOT EXISTS idx_saas_apps_managed_state ON saas_apps (managed_state);
CREATE INDEX IF NOT EXISTS idx_saas_apps_risk_level_score ON saas_apps (risk_level, risk_score DESC);

DROP FUNCTION IF EXISTS saas_app_posture_rows(
  timestamptz,
  timestamptz,
  timestamptz,
  timestamptz,
  timestamptz,
  timestamptz,
  timestamptz
);

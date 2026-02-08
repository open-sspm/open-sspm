-- SaaS discovery inventory, evidence, bindings, and governance overrides.

CREATE TABLE IF NOT EXISTS saas_apps (
  id BIGSERIAL PRIMARY KEY,
  canonical_key TEXT NOT NULL UNIQUE,
  display_name TEXT NOT NULL DEFAULT '',
  primary_domain TEXT NOT NULL DEFAULT '',
  vendor_name TEXT NOT NULL DEFAULT '',
  managed_state TEXT NOT NULL DEFAULT 'unmanaged',
  managed_reason TEXT NOT NULL DEFAULT 'no_binding',
  bound_connector_kind TEXT NOT NULL DEFAULT '',
  bound_connector_source_name TEXT NOT NULL DEFAULT '',
  risk_score INT NOT NULL DEFAULT 0,
  risk_level TEXT NOT NULL DEFAULT 'low',
  suggested_business_criticality TEXT NOT NULL DEFAULT 'unknown',
  suggested_data_classification TEXT NOT NULL DEFAULT 'unknown',
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT saas_apps_managed_state_check CHECK (managed_state IN ('managed', 'unmanaged')),
  CONSTRAINT saas_apps_managed_reason_check CHECK (managed_reason IN ('active_binding_fresh_sync', 'no_binding', 'connector_disabled', 'connector_not_configured', 'stale_sync')),
  CONSTRAINT saas_apps_risk_score_range_check CHECK (risk_score >= 0 AND risk_score <= 100),
  CONSTRAINT saas_apps_risk_level_check CHECK (risk_level IN ('low', 'medium', 'high', 'critical')),
  CONSTRAINT saas_apps_suggested_business_criticality_check CHECK (suggested_business_criticality IN ('unknown', 'low', 'medium', 'high', 'critical')),
  CONSTRAINT saas_apps_suggested_data_classification_check CHECK (suggested_data_classification IN ('unknown', 'public', 'internal', 'confidential', 'restricted'))
);

CREATE INDEX IF NOT EXISTS idx_saas_apps_primary_domain ON saas_apps (primary_domain);
CREATE INDEX IF NOT EXISTS idx_saas_apps_managed_state ON saas_apps (managed_state);
CREATE INDEX IF NOT EXISTS idx_saas_apps_risk_level_score ON saas_apps (risk_level, risk_score DESC);
CREATE INDEX IF NOT EXISTS idx_saas_apps_last_seen_at ON saas_apps (last_seen_at DESC);

CREATE TABLE IF NOT EXISTS saas_app_sources (
  id BIGSERIAL PRIMARY KEY,
  saas_app_id BIGINT NOT NULL REFERENCES saas_apps(id) ON DELETE CASCADE,
  source_kind TEXT NOT NULL,
  source_name TEXT NOT NULL,
  source_app_id TEXT NOT NULL,
  source_app_name TEXT NOT NULL DEFAULT '',
  source_app_domain TEXT NOT NULL DEFAULT '',
  seen_in_run_id BIGINT REFERENCES sync_runs(id),
  seen_at TIMESTAMPTZ,
  last_observed_run_id BIGINT REFERENCES sync_runs(id),
  last_observed_at TIMESTAMPTZ,
  expired_at TIMESTAMPTZ,
  expired_run_id BIGINT REFERENCES sync_runs(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (source_kind, source_name, source_app_id)
);

CREATE INDEX IF NOT EXISTS idx_saas_app_sources_saas_app_id ON saas_app_sources (saas_app_id);
CREATE INDEX IF NOT EXISTS idx_saas_app_sources_source ON saas_app_sources (source_kind, source_name);
CREATE INDEX IF NOT EXISTS idx_saas_app_sources_source_domain ON saas_app_sources (source_kind, source_name, source_app_domain);
CREATE INDEX IF NOT EXISTS idx_saas_app_sources_active_source ON saas_app_sources (source_kind, source_name)
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_saas_app_sources_active_last_observed_run_id ON saas_app_sources (last_observed_run_id)
  WHERE expired_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_saas_app_sources_seen_in_run_id ON saas_app_sources (seen_in_run_id);

CREATE TABLE IF NOT EXISTS saas_app_events (
  id BIGSERIAL PRIMARY KEY,
  saas_app_id BIGINT NOT NULL REFERENCES saas_apps(id) ON DELETE CASCADE,
  source_kind TEXT NOT NULL,
  source_name TEXT NOT NULL,
  signal_kind TEXT NOT NULL,
  event_external_id TEXT NOT NULL,
  source_app_id TEXT NOT NULL DEFAULT '',
  source_app_name TEXT NOT NULL DEFAULT '',
  source_app_domain TEXT NOT NULL DEFAULT '',
  actor_external_id TEXT NOT NULL DEFAULT '',
  actor_email TEXT NOT NULL DEFAULT '',
  actor_display_name TEXT NOT NULL DEFAULT '',
  observed_at TIMESTAMPTZ NOT NULL,
  scopes_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  seen_in_run_id BIGINT REFERENCES sync_runs(id),
  seen_at TIMESTAMPTZ,
  last_observed_run_id BIGINT REFERENCES sync_runs(id),
  last_observed_at TIMESTAMPTZ,
  expired_at TIMESTAMPTZ,
  expired_run_id BIGINT REFERENCES sync_runs(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT saas_app_events_signal_kind_check CHECK (signal_kind IN ('idp_sso', 'oauth_grant')),
  UNIQUE (source_kind, source_name, signal_kind, event_external_id)
);

CREATE INDEX IF NOT EXISTS idx_saas_app_events_saas_app_id_observed_at_desc ON saas_app_events (saas_app_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_saas_app_events_source ON saas_app_events (source_kind, source_name, signal_kind);
CREATE INDEX IF NOT EXISTS idx_saas_app_events_actor_email ON saas_app_events (actor_email);
CREATE INDEX IF NOT EXISTS idx_saas_app_events_active_source ON saas_app_events (source_kind, source_name)
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_saas_app_events_active_last_observed_run_id ON saas_app_events (last_observed_run_id)
  WHERE expired_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_saas_app_events_seen_in_run_id ON saas_app_events (seen_in_run_id);

CREATE TABLE IF NOT EXISTS saas_app_bindings (
  id BIGSERIAL PRIMARY KEY,
  saas_app_id BIGINT NOT NULL REFERENCES saas_apps(id) ON DELETE CASCADE,
  connector_kind TEXT NOT NULL,
  connector_source_name TEXT NOT NULL,
  binding_source TEXT NOT NULL,
  confidence REAL NOT NULL DEFAULT 0,
  is_primary BOOLEAN NOT NULL DEFAULT false,
  created_by_auth_user_id BIGINT REFERENCES auth_users(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT saas_app_bindings_binding_source_check CHECK (binding_source IN ('auto', 'manual')),
  CONSTRAINT saas_app_bindings_confidence_range_check CHECK (confidence >= 0 AND confidence <= 1),
  UNIQUE (saas_app_id, connector_kind, connector_source_name)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_saas_app_bindings_primary_per_app
  ON saas_app_bindings (saas_app_id)
  WHERE is_primary;

CREATE INDEX IF NOT EXISTS idx_saas_app_bindings_connector ON saas_app_bindings (connector_kind, connector_source_name);
CREATE INDEX IF NOT EXISTS idx_saas_app_bindings_source ON saas_app_bindings (binding_source);

CREATE TABLE IF NOT EXISTS saas_app_governance_overrides (
  saas_app_id BIGINT PRIMARY KEY REFERENCES saas_apps(id) ON DELETE CASCADE,
  owner_identity_id BIGINT REFERENCES identities(id) ON DELETE SET NULL,
  business_criticality TEXT NOT NULL DEFAULT 'unknown',
  data_classification TEXT NOT NULL DEFAULT 'unknown',
  notes TEXT NOT NULL DEFAULT '',
  updated_by_auth_user_id BIGINT REFERENCES auth_users(id) ON DELETE SET NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT saas_app_governance_business_criticality_check CHECK (business_criticality IN ('unknown', 'low', 'medium', 'high', 'critical')),
  CONSTRAINT saas_app_governance_data_classification_check CHECK (data_classification IN ('unknown', 'public', 'internal', 'confidential', 'restricted'))
);

CREATE INDEX IF NOT EXISTS idx_saas_app_governance_owner_identity_id
  ON saas_app_governance_overrides (owner_identity_id);

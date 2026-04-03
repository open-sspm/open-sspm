CREATE OR REPLACE FUNCTION saas_app_posture_rows(
  okta_fresh_after timestamptz,
  entra_fresh_after timestamptz,
  google_workspace_fresh_after timestamptz,
  github_fresh_after timestamptz,
  datadog_fresh_after timestamptz,
  aws_fresh_after timestamptz,
  default_fresh_after timestamptz
)
RETURNS TABLE (
  id bigint,
  canonical_key text,
  display_name text,
  primary_domain text,
  vendor_name text,
  first_seen_at timestamptz,
  last_seen_at timestamptz,
  created_at timestamptz,
  updated_at timestamptz,
  owner_identity_id bigint,
  actors_30d bigint,
  has_privileged_scope boolean,
  has_confidential_scope boolean,
  bound_connector_kind text,
  bound_connector_source_name text,
  connector_enabled boolean,
  connector_configured boolean,
  last_success_at timestamptz,
  suggested_business_criticality text,
  suggested_data_classification text,
  effective_business_criticality text,
  effective_data_classification text,
  managed_state text,
  managed_reason text,
  risk_score integer,
  risk_level text
)
LANGUAGE sql
STABLE
AS $$
WITH posture_inputs AS (
  SELECT
    spi.*,
    CASE spi.bound_connector_kind
      WHEN 'okta' THEN okta_fresh_after
      WHEN 'entra' THEN entra_fresh_after
      WHEN 'google_workspace' THEN google_workspace_fresh_after
      WHEN 'github' THEN github_fresh_after
      WHEN 'datadog' THEN datadog_fresh_after
      WHEN 'aws_identity_center' THEN aws_fresh_after
      ELSE default_fresh_after
    END AS fresh_after
  FROM saas_app_posture_inputs_v spi
),
posture_state AS (
  SELECT
    pi.*,
    CASE
      WHEN pi.bound_connector_kind = '' OR pi.bound_connector_source_name = '' THEN 'unmanaged'
      WHEN NOT pi.connector_configured THEN 'unmanaged'
      WHEN NOT pi.connector_enabled THEN 'unmanaged'
      WHEN pi.last_success_at IS NULL THEN 'unmanaged'
      WHEN pi.last_success_at < pi.fresh_after THEN 'unmanaged'
      ELSE 'managed'
    END AS managed_state,
    CASE
      WHEN pi.bound_connector_kind = '' OR pi.bound_connector_source_name = '' THEN 'no_binding'
      WHEN NOT pi.connector_configured THEN 'connector_not_configured'
      WHEN NOT pi.connector_enabled THEN 'connector_disabled'
      WHEN pi.last_success_at IS NULL THEN 'stale_sync'
      WHEN pi.last_success_at < pi.fresh_after THEN 'stale_sync'
      ELSE 'active_binding_fresh_sync'
    END AS managed_reason
  FROM posture_inputs pi
),
posture_with_risk AS (
  SELECT
    ps.*,
    LEAST(100,
      CASE WHEN ps.managed_state <> 'managed' THEN 45 ELSE 0 END
      + CASE WHEN ps.has_privileged_scope THEN 20 ELSE 0 END
      + CASE WHEN ps.owner_identity_id = 0 THEN 15 ELSE 0 END
      + CASE WHEN ps.actors_30d >= 50 THEN 10 ELSE 0 END
      + CASE WHEN ps.managed_state <> 'managed' AND ps.effective_business_criticality IN ('high', 'critical') THEN 10 ELSE 0 END
      + CASE WHEN ps.managed_state <> 'managed' AND ps.effective_data_classification IN ('confidential', 'restricted') THEN 5 ELSE 0 END
    )::int AS risk_score
  FROM posture_state ps
)
SELECT
  pwr.id,
  pwr.canonical_key,
  pwr.display_name,
  pwr.primary_domain,
  pwr.vendor_name,
  pwr.first_seen_at,
  pwr.last_seen_at,
  pwr.created_at,
  pwr.updated_at,
  pwr.owner_identity_id,
  pwr.actors_30d,
  pwr.has_privileged_scope,
  pwr.has_confidential_scope,
  pwr.bound_connector_kind,
  pwr.bound_connector_source_name,
  pwr.connector_enabled,
  pwr.connector_configured,
  pwr.last_success_at,
  pwr.suggested_business_criticality,
  pwr.suggested_data_classification,
  pwr.effective_business_criticality,
  pwr.effective_data_classification,
  pwr.managed_state,
  pwr.managed_reason,
  pwr.risk_score,
  CASE
    WHEN pwr.risk_score >= 80 THEN 'critical'
    WHEN pwr.risk_score >= 60 THEN 'high'
    WHEN pwr.risk_score >= 30 THEN 'medium'
    ELSE 'low'
  END AS risk_level
FROM posture_with_risk pwr;
$$;

DROP INDEX IF EXISTS idx_saas_apps_managed_state;
DROP INDEX IF EXISTS idx_saas_apps_risk_level_score;

ALTER TABLE saas_apps
  DROP CONSTRAINT IF EXISTS saas_apps_managed_state_check,
  DROP CONSTRAINT IF EXISTS saas_apps_managed_reason_check,
  DROP CONSTRAINT IF EXISTS saas_apps_risk_score_range_check,
  DROP CONSTRAINT IF EXISTS saas_apps_risk_level_check,
  DROP CONSTRAINT IF EXISTS saas_apps_suggested_business_criticality_check,
  DROP CONSTRAINT IF EXISTS saas_apps_suggested_data_classification_check;

ALTER TABLE saas_apps
  DROP COLUMN IF EXISTS managed_state,
  DROP COLUMN IF EXISTS managed_reason,
  DROP COLUMN IF EXISTS bound_connector_kind,
  DROP COLUMN IF EXISTS bound_connector_source_name,
  DROP COLUMN IF EXISTS risk_score,
  DROP COLUMN IF EXISTS risk_level,
  DROP COLUMN IF EXISTS suggested_business_criticality,
  DROP COLUMN IF EXISTS suggested_data_classification;

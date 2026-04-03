CREATE VIEW saas_app_posture_inputs_v AS
WITH connector_states AS (
  SELECT
    lower(trim(cc.kind)) AS kind,
    cc.enabled,
    CASE lower(trim(cc.kind))
      WHEN 'okta' THEN trim(COALESCE(cc.config->>'domain', ''))
      WHEN 'entra' THEN trim(COALESCE(cc.config->>'tenant_id', ''))
      WHEN 'google_workspace' THEN trim(COALESCE(cc.config->>'customer_id', ''))
      WHEN 'github' THEN trim(COALESCE(cc.config->>'org', ''))
      WHEN 'datadog' THEN trim(COALESCE(cc.config->>'site', ''))
      WHEN 'aws_identity_center' THEN COALESCE(
        NULLIF(trim(COALESCE(cc.config->>'name', '')), ''),
        trim(COALESCE(cc.config->>'region', ''))
      )
      ELSE ''
    END AS source_name,
    CASE lower(trim(cc.kind))
      WHEN 'okta' THEN
        trim(COALESCE(cc.config->>'domain', '')) <> ''
        AND trim(COALESCE(cc.config->>'token', '')) <> ''
      WHEN 'entra' THEN
        trim(COALESCE(cc.config->>'tenant_id', '')) <> ''
        AND trim(COALESCE(cc.config->>'client_id', '')) <> ''
        AND trim(COALESCE(cc.config->>'client_secret', '')) <> ''
      WHEN 'google_workspace' THEN
        trim(COALESCE(cc.config->>'customer_id', '')) <> ''
        AND trim(COALESCE(cc.config->>'delegated_admin_email', '')) <> ''
        AND CASE lower(COALESCE(NULLIF(trim(cc.config->>'auth_type'), ''), 'service_account_json'))
          WHEN 'service_account_json' THEN trim(COALESCE(cc.config->>'service_account_json', '')) <> ''
          WHEN 'adc' THEN trim(COALESCE(cc.config->>'service_account_email', '')) <> ''
          ELSE false
        END
      WHEN 'github' THEN
        trim(COALESCE(cc.config->>'org', '')) <> ''
        AND trim(COALESCE(cc.config->>'token', '')) <> ''
      WHEN 'datadog' THEN
        trim(COALESCE(cc.config->>'site', '')) <> ''
        AND trim(COALESCE(cc.config->>'api_key', '')) <> ''
        AND trim(COALESCE(cc.config->>'app_key', '')) <> ''
      WHEN 'aws_identity_center' THEN
        trim(COALESCE(cc.config->>'region', '')) <> ''
        AND CASE lower(COALESCE(NULLIF(trim(cc.config->>'auth_type'), ''), 'default_chain'))
          WHEN 'default_chain' THEN true
          WHEN 'access_key' THEN
            trim(COALESCE(cc.config->>'access_key_id', '')) <> ''
            AND trim(COALESCE(cc.config->>'secret_access_key', '')) <> ''
          ELSE false
        END
      ELSE false
    END AS configured
  FROM connector_configs cc
),
last_success_by_source AS (
  SELECT
    CASE lower(trim(r.source_kind))
      WHEN 'okta_discovery' THEN 'okta'
      WHEN 'entra_discovery' THEN 'entra'
      WHEN 'google_workspace_discovery' THEN 'google_workspace'
      WHEN 'aws' THEN 'aws_identity_center'
      ELSE lower(trim(r.source_kind))
    END AS source_kind,
    trim(r.source_name) AS source_name,
    max(r.finished_at)::timestamptz AS last_success_at
  FROM sync_runs r
  WHERE r.status = 'success'
    AND r.finished_at IS NOT NULL
  GROUP BY 1, trim(r.source_name)
)
SELECT
  sa.id,
  sa.canonical_key,
  sa.display_name,
  sa.primary_domain,
  sa.vendor_name,
  sa.first_seen_at,
  sa.last_seen_at,
  sa.created_at,
  sa.updated_at,
  COALESCE(go.owner_identity_id, 0)::bigint AS owner_identity_id,
  COALESCE(actor_stats.actors_30d, 0)::bigint AS actors_30d,
  COALESCE(scope_flags.has_privileged_scope, false)::boolean AS has_privileged_scope,
  COALESCE(scope_flags.has_confidential_scope, false)::boolean AS has_confidential_scope,
  COALESCE(pb.connector_kind, '')::text AS bound_connector_kind,
  COALESCE(pb.connector_source_name, '')::text AS bound_connector_source_name,
  COALESCE(cs.enabled, false)::boolean AS connector_enabled,
  COALESCE(cs.configured, false)::boolean AS connector_configured,
  ls.last_success_at::timestamptz AS last_success_at,
  CASE
    WHEN COALESCE(actor_stats.actors_30d, 0) >= 200 THEN 'critical'
    WHEN COALESCE(actor_stats.actors_30d, 0) >= 50 OR COALESCE(scope_flags.has_privileged_scope, false) THEN 'high'
    WHEN COALESCE(actor_stats.actors_30d, 0) >= 10 THEN 'medium'
    ELSE 'low'
  END AS suggested_business_criticality,
  CASE
    WHEN COALESCE(scope_flags.has_privileged_scope, false) THEN 'restricted'
    WHEN COALESCE(scope_flags.has_confidential_scope, false) THEN 'confidential'
    ELSE 'internal'
  END AS suggested_data_classification,
  CASE lower(trim(COALESCE(go.business_criticality, '')))
    WHEN 'low' THEN 'low'
    WHEN 'medium' THEN 'medium'
    WHEN 'high' THEN 'high'
    WHEN 'critical' THEN 'critical'
    ELSE CASE
      WHEN COALESCE(actor_stats.actors_30d, 0) >= 200 THEN 'critical'
      WHEN COALESCE(actor_stats.actors_30d, 0) >= 50 OR COALESCE(scope_flags.has_privileged_scope, false) THEN 'high'
      WHEN COALESCE(actor_stats.actors_30d, 0) >= 10 THEN 'medium'
      ELSE 'low'
    END
  END AS effective_business_criticality,
  CASE lower(trim(COALESCE(go.data_classification, '')))
    WHEN 'public' THEN 'public'
    WHEN 'internal' THEN 'internal'
    WHEN 'confidential' THEN 'confidential'
    WHEN 'restricted' THEN 'restricted'
    ELSE CASE
      WHEN COALESCE(scope_flags.has_privileged_scope, false) THEN 'restricted'
      WHEN COALESCE(scope_flags.has_confidential_scope, false) THEN 'confidential'
      ELSE 'internal'
    END
  END AS effective_data_classification
FROM saas_apps sa
LEFT JOIN saas_app_governance_overrides go ON go.saas_app_id = sa.id
LEFT JOIN LATERAL (
  SELECT
    count(DISTINCT COALESCE(NULLIF(trim(e.actor_external_id), ''), NULLIF(lower(trim(e.actor_email)), ''))) AS actors_30d
  FROM saas_app_events e
  WHERE e.saas_app_id = sa.id
    AND e.expired_at IS NULL
    AND e.last_observed_run_id IS NOT NULL
    AND e.observed_at >= now() - interval '30 days'
) actor_stats ON TRUE
LEFT JOIN LATERAL (
  SELECT
    bool_or(
      lower(e.scopes_json::text) LIKE '%directory.readwrite.all%'
      OR lower(e.scopes_json::text) LIKE '%application.readwrite.all%'
      OR lower(e.scopes_json::text) LIKE '%rolemanagement.readwrite.directory%'
      OR lower(e.scopes_json::text) LIKE '%mailboxsettings.readwrite%'
      OR lower(e.scopes_json::text) LIKE '%full_access_as_app%'
      OR lower(e.scopes_json::text) LIKE '%files.readwrite.all%'
      OR lower(e.scopes_json::text) LIKE '%files.readwrite%'
      OR lower(e.scopes_json::text) LIKE '%sites.readwrite.all%'
      OR lower(e.scopes_json::text) LIKE '%user.readwrite.all%'
      OR lower(e.scopes_json::text) LIKE '%offline_access%'
    ) FILTER (WHERE e.signal_kind = 'oauth_grant') AS has_privileged_scope,
    bool_or(
      lower(e.scopes_json::text) LIKE '%mail.%'
      OR lower(e.scopes_json::text) LIKE '%files.%'
      OR lower(e.scopes_json::text) LIKE '%calendar.%'
      OR lower(e.scopes_json::text) LIKE '%readwrite%'
      OR lower(e.scopes_json::text) LIKE '%sites.read%'
    ) FILTER (WHERE e.signal_kind = 'oauth_grant') AS has_confidential_scope
  FROM saas_app_events e
  WHERE e.saas_app_id = sa.id
    AND e.expired_at IS NULL
    AND e.last_observed_run_id IS NOT NULL
) scope_flags ON TRUE
LEFT JOIN LATERAL (
  SELECT
    lower(trim(b.connector_kind)) AS connector_kind,
    trim(b.connector_source_name) AS connector_source_name
  FROM saas_app_bindings b
  WHERE b.saas_app_id = sa.id
    AND b.is_primary
  ORDER BY b.id ASC
  LIMIT 1
) pb ON TRUE
LEFT JOIN connector_states cs
  ON cs.kind = COALESCE(pb.connector_kind, '')
 AND lower(trim(cs.source_name)) = lower(COALESCE(pb.connector_source_name, ''))
LEFT JOIN last_success_by_source ls
  ON ls.source_kind = COALESCE(pb.connector_kind, '')
 AND lower(trim(ls.source_name)) = lower(COALESCE(pb.connector_source_name, ''));

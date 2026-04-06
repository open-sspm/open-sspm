CREATE TABLE IF NOT EXISTS governance_subject_overrides (
  subject_kind TEXT NOT NULL,
  subject_id BIGINT NOT NULL,
  governance_state TEXT NOT NULL DEFAULT 'unreviewed',
  owner_identity_id BIGINT REFERENCES identities(id) ON DELETE SET NULL,
  business_criticality TEXT NOT NULL DEFAULT 'unknown',
  data_classification TEXT NOT NULL DEFAULT 'unknown',
  ticket_ref TEXT NOT NULL DEFAULT '',
  notes TEXT NOT NULL DEFAULT '',
  updated_by_auth_user_id BIGINT REFERENCES auth_users(id) ON DELETE SET NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (subject_kind, subject_id),
  CONSTRAINT governance_subject_overrides_subject_kind_check CHECK (
    subject_kind IN ('app_asset', 'saas_app')
  ),
  CONSTRAINT governance_subject_overrides_subject_id_check CHECK (subject_id > 0),
  CONSTRAINT governance_subject_overrides_state_check CHECK (
    governance_state IN ('unreviewed', 'in_review', 'approved', 'action_required', 'ticketed')
  ),
  CONSTRAINT governance_subject_overrides_business_criticality_check CHECK (
    business_criticality IN ('unknown', 'low', 'medium', 'high', 'critical')
  ),
  CONSTRAINT governance_subject_overrides_data_classification_check CHECK (
    data_classification IN ('unknown', 'public', 'internal', 'confidential', 'restricted')
  )
);

CREATE INDEX IF NOT EXISTS idx_governance_subject_overrides_kind_state
  ON governance_subject_overrides (subject_kind, governance_state);

CREATE INDEX IF NOT EXISTS idx_governance_subject_overrides_owner_identity_id
  ON governance_subject_overrides (owner_identity_id);

INSERT INTO governance_subject_overrides (
  subject_kind,
  subject_id,
  governance_state,
  owner_identity_id,
  ticket_ref,
  notes,
  updated_by_auth_user_id,
  updated_at
)
SELECT
  'app_asset'::text AS subject_kind,
  cag.app_asset_id AS subject_id,
  CASE cag.review_state
    WHEN 'under_review' THEN 'in_review'
    WHEN 'sanctioned' THEN 'approved'
    WHEN 'needs_revocation' THEN 'action_required'
    WHEN 'ticketed' THEN 'ticketed'
    ELSE 'unreviewed'
  END AS governance_state,
  cag.owner_identity_id,
  cag.ticket_ref,
  cag.notes,
  cag.updated_by_auth_user_id,
  cag.updated_at
FROM connected_app_governance cag
ON CONFLICT (subject_kind, subject_id) DO UPDATE SET
  governance_state = EXCLUDED.governance_state,
  owner_identity_id = EXCLUDED.owner_identity_id,
  ticket_ref = EXCLUDED.ticket_ref,
  notes = EXCLUDED.notes,
  updated_by_auth_user_id = EXCLUDED.updated_by_auth_user_id,
  updated_at = EXCLUDED.updated_at;

INSERT INTO governance_subject_overrides (
  subject_kind,
  subject_id,
  governance_state,
  owner_identity_id,
  business_criticality,
  data_classification,
  notes,
  updated_by_auth_user_id,
  updated_at
)
SELECT
  'saas_app'::text AS subject_kind,
  go.saas_app_id AS subject_id,
  'unreviewed'::text AS governance_state,
  go.owner_identity_id,
  go.business_criticality,
  go.data_classification,
  go.notes,
  go.updated_by_auth_user_id,
  go.updated_at
FROM saas_app_governance_overrides go
ON CONFLICT (subject_kind, subject_id) DO UPDATE SET
  governance_state = EXCLUDED.governance_state,
  owner_identity_id = EXCLUDED.owner_identity_id,
  business_criticality = EXCLUDED.business_criticality,
  data_classification = EXCLUDED.data_classification,
  notes = EXCLUDED.notes,
  updated_by_auth_user_id = EXCLUDED.updated_by_auth_user_id,
  updated_at = EXCLUDED.updated_at;

DROP FUNCTION IF EXISTS saas_app_posture_rows(
  timestamptz,
  timestamptz,
  timestamptz,
  timestamptz,
  timestamptz,
  timestamptz,
  timestamptz
);

DROP VIEW IF EXISTS saas_app_posture_inputs_v;

CREATE VIEW saas_app_posture_inputs_v AS
WITH secret_presence AS (
  SELECT
    lower(trim(cs.kind)) AS kind,
    bool_or(cs.secret_name = 'token') AS has_token,
    bool_or(cs.secret_name = 'client_secret') AS has_client_secret,
    bool_or(cs.secret_name = 'service_account_json') AS has_service_account_json,
    bool_or(cs.secret_name = 'api_key') AS has_api_key,
    bool_or(cs.secret_name = 'app_key') AS has_app_key,
    bool_or(cs.secret_name = 'secret_access_key') AS has_secret_access_key
  FROM connector_secrets cs
  GROUP BY lower(trim(cs.kind))
),
connector_states AS (
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
        AND COALESCE(sp.has_token, false)
      WHEN 'entra' THEN
        trim(COALESCE(cc.config->>'tenant_id', '')) <> ''
        AND trim(COALESCE(cc.config->>'client_id', '')) <> ''
        AND COALESCE(sp.has_client_secret, false)
      WHEN 'google_workspace' THEN
        trim(COALESCE(cc.config->>'customer_id', '')) <> ''
        AND trim(COALESCE(cc.config->>'delegated_admin_email', '')) <> ''
        AND CASE lower(COALESCE(NULLIF(trim(cc.config->>'auth_type'), ''), 'service_account_json'))
          WHEN 'service_account_json' THEN COALESCE(sp.has_service_account_json, false)
          WHEN 'adc' THEN trim(COALESCE(cc.config->>'service_account_email', '')) <> ''
          ELSE false
        END
      WHEN 'github' THEN
        trim(COALESCE(cc.config->>'org', '')) <> ''
        AND COALESCE(sp.has_token, false)
      WHEN 'datadog' THEN
        trim(COALESCE(cc.config->>'site', '')) <> ''
        AND COALESCE(sp.has_api_key, false)
        AND COALESCE(sp.has_app_key, false)
      WHEN 'aws_identity_center' THEN
        trim(COALESCE(cc.config->>'region', '')) <> ''
        AND CASE lower(COALESCE(NULLIF(trim(cc.config->>'auth_type'), ''), 'default_chain'))
          WHEN 'default_chain' THEN true
          WHEN 'access_key' THEN
            trim(COALESCE(cc.config->>'access_key_id', '')) <> ''
            AND COALESCE(sp.has_secret_access_key, false)
          ELSE false
        END
      ELSE false
    END AS configured
  FROM connector_configs cc
  LEFT JOIN secret_presence sp
    ON sp.kind = lower(trim(cc.kind))
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
  END AS effective_data_classification,
  COALESCE(go.governance_state, 'unreviewed')::text AS governance_state,
  COALESCE(go.ticket_ref, '')::text AS ticket_ref,
  COALESCE(go.notes, '')::text AS notes
FROM saas_apps sa
LEFT JOIN governance_subject_overrides go
  ON go.subject_kind = 'saas_app'
 AND go.subject_id = sa.id
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

CREATE FUNCTION saas_app_posture_rows(
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
  governance_state text,
  ticket_ref text,
  notes text,
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
  pwr.governance_state,
  pwr.ticket_ref,
  pwr.notes,
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

CREATE VIEW app_asset_posture_inputs_v AS
SELECT
  aa.id,
  aa.source_kind,
  aa.source_name,
  aa.asset_kind,
  aa.external_id,
  aa.parent_external_id,
  aa.display_name,
  aa.status,
  aa.created_at_source,
  aa.updated_at_source,
  aa.raw_json,
  aa.seen_in_run_id,
  aa.seen_at,
  aa.last_observed_run_id,
  aa.last_observed_at,
  aa.expired_at,
  aa.expired_run_id,
  aa.created_at,
  aa.updated_at,
  COALESCE(go.governance_state, 'unreviewed')::text AS governance_state,
  COALESCE(go.ticket_ref, '')::text AS ticket_ref,
  COALESCE(go.notes, '')::text AS notes,
  COALESCE(go.owner_identity_id, 0)::bigint AS governance_owner_identity_id,
  COALESCE(owner.display_name, '')::text AS governance_owner_display_name,
  COALESCE(owner.primary_email, '')::text AS governance_owner_primary_email,
  COALESCE(owner.kind, '')::text AS governance_owner_kind,
  COALESCE(owner_counts.owner_count, 0)::bigint AS owner_count,
  COALESCE(grant_counts.grant_count, 0)::bigint AS grant_count,
  COALESCE(grant_counts.actor_count, 0)::bigint AS actor_count,
  COALESCE(discovery_counts.discovery_source_count, 0)::bigint AS discovery_source_count,
  COALESCE(discovery_counts.discovery_event_count_30d, 0)::bigint AS discovery_event_count_30d,
  COALESCE(discovery_counts.last_evidence_at, aa.last_observed_at)::timestamptz AS evidence_last_seen_at,
  'unknown'::text AS suggested_business_criticality,
  'unknown'::text AS suggested_data_classification,
  CASE lower(trim(COALESCE(go.business_criticality, '')))
    WHEN 'low' THEN 'low'
    WHEN 'medium' THEN 'medium'
    WHEN 'high' THEN 'high'
    WHEN 'critical' THEN 'critical'
    ELSE 'unknown'
  END AS effective_business_criticality,
  CASE lower(trim(COALESCE(go.data_classification, '')))
    WHEN 'public' THEN 'public'
    WHEN 'internal' THEN 'internal'
    WHEN 'confidential' THEN 'confidential'
    WHEN 'restricted' THEN 'restricted'
    ELSE 'unknown'
  END AS effective_data_classification
FROM app_assets aa
LEFT JOIN governance_subject_overrides go
  ON go.subject_kind = 'app_asset'
 AND go.subject_id = aa.id
LEFT JOIN identities owner ON owner.id = go.owner_identity_id
LEFT JOIN LATERAL (
  SELECT count(*)::bigint AS owner_count
  FROM app_asset_owners aao
  WHERE aao.app_asset_id = aa.id
    AND aao.expired_at IS NULL
    AND aao.last_observed_run_id IS NOT NULL
) owner_counts ON TRUE
LEFT JOIN LATERAL (
  SELECT
    count(*)::bigint AS grant_count,
    count(
      DISTINCT COALESCE(
        NULLIF(trim(ca.created_by_external_id), ''),
        NULLIF(lower(trim(ca.created_by_display_name)), '')
      )
    )::bigint AS actor_count
  FROM credential_artifacts ca
  WHERE ca.source_kind = aa.source_kind
    AND ca.source_name = aa.source_name
    AND ca.asset_ref_kind = aa.asset_kind
    AND ca.asset_ref_external_id = (aa.asset_kind || ':' || aa.external_id)
    AND ca.expired_at IS NULL
    AND ca.last_observed_run_id IS NOT NULL
) grant_counts ON TRUE
LEFT JOIN LATERAL (
  SELECT
    source_counts.discovery_source_count,
    event_counts.discovery_event_count_30d,
    NULLIF(
      GREATEST(
        COALESCE(source_counts.last_source_seen_at, '-infinity'::timestamptz),
        COALESCE(event_counts.last_event_at, '-infinity'::timestamptz)
      ),
      '-infinity'::timestamptz
    )::timestamptz AS last_evidence_at
  FROM (
    SELECT
      count(*)::bigint AS discovery_source_count,
      NULLIF(
        GREATEST(
          COALESCE(max(sas.last_observed_at), '-infinity'::timestamptz),
          COALESCE(max(sas.seen_at), '-infinity'::timestamptz)
        ),
        '-infinity'::timestamptz
      )::timestamptz AS last_source_seen_at
    FROM saas_app_sources sas
    WHERE sas.source_kind = aa.source_kind
      AND sas.source_name = aa.source_name
      AND sas.source_app_id = aa.external_id
      AND sas.expired_at IS NULL
      AND sas.last_observed_run_id IS NOT NULL
  ) source_counts
  CROSS JOIN (
    SELECT
      count(*) FILTER (
        WHERE e.observed_at >= now() - interval '30 days'
      )::bigint AS discovery_event_count_30d,
      max(e.observed_at)::timestamptz AS last_event_at
    FROM saas_app_events e
    WHERE e.source_kind = aa.source_kind
      AND e.source_name = aa.source_name
      AND e.source_app_id = aa.external_id
      AND e.expired_at IS NULL
      AND e.last_observed_run_id IS NOT NULL
  ) event_counts
) discovery_counts ON TRUE
WHERE aa.expired_at IS NULL
  AND aa.last_observed_run_id IS NOT NULL;

CREATE FUNCTION app_asset_posture_rows(
  evaluated_at timestamptz
)
RETURNS TABLE (
  id bigint,
  source_kind text,
  source_name text,
  asset_kind text,
  external_id text,
  parent_external_id text,
  display_name text,
  status text,
  created_at_source timestamptz,
  updated_at_source timestamptz,
  raw_json jsonb,
  seen_in_run_id bigint,
  seen_at timestamptz,
  last_observed_run_id bigint,
  last_observed_at timestamptz,
  expired_at timestamptz,
  expired_run_id bigint,
  created_at timestamptz,
  updated_at timestamptz,
  governance_state text,
  ticket_ref text,
  notes text,
  governance_owner_identity_id bigint,
  governance_owner_display_name text,
  governance_owner_primary_email text,
  governance_owner_kind text,
  owner_count bigint,
  grant_count bigint,
  actor_count bigint,
  discovery_source_count bigint,
  discovery_event_count_30d bigint,
  evidence_last_seen_at timestamptz,
  suggested_business_criticality text,
  suggested_data_classification text,
  effective_business_criticality text,
  effective_data_classification text,
  evidence_freshness text,
  evidence_confidence text,
  evidence_confidence_reason text
)
LANGUAGE sql
STABLE
AS $$
WITH posture_inputs AS (
  SELECT
    api.*,
    (
      1
      + CASE WHEN api.owner_count > 0 OR api.governance_owner_identity_id > 0 THEN 1 ELSE 0 END
      + CASE WHEN api.discovery_source_count > 0 THEN 1 ELSE 0 END
    ) AS evidence_signal_count
  FROM app_asset_posture_inputs_v api
)
SELECT
  pi.id,
  pi.source_kind,
  pi.source_name,
  pi.asset_kind,
  pi.external_id,
  pi.parent_external_id,
  pi.display_name,
  pi.status,
  pi.created_at_source,
  pi.updated_at_source,
  pi.raw_json,
  pi.seen_in_run_id,
  pi.seen_at,
  pi.last_observed_run_id,
  pi.last_observed_at,
  pi.expired_at,
  pi.expired_run_id,
  pi.created_at,
  pi.updated_at,
  pi.governance_state,
  pi.ticket_ref,
  pi.notes,
  pi.governance_owner_identity_id,
  pi.governance_owner_display_name,
  pi.governance_owner_primary_email,
  pi.governance_owner_kind,
  pi.owner_count,
  pi.grant_count,
  pi.actor_count,
  pi.discovery_source_count,
  pi.discovery_event_count_30d,
  pi.evidence_last_seen_at,
  pi.suggested_business_criticality,
  pi.suggested_data_classification,
  pi.effective_business_criticality,
  pi.effective_data_classification,
  CASE
    WHEN pi.evidence_last_seen_at IS NULL THEN 'unknown'
    WHEN pi.evidence_last_seen_at >= evaluated_at - interval '7 days' THEN 'fresh'
    WHEN pi.evidence_last_seen_at >= evaluated_at - interval '30 days' THEN 'aging'
    ELSE 'stale'
  END AS evidence_freshness,
  CASE pi.evidence_signal_count
    WHEN 3 THEN 'high'
    WHEN 2 THEN 'medium'
    ELSE 'low'
  END AS evidence_confidence,
  CASE pi.evidence_signal_count
    WHEN 3 THEN 'Inventory, ownership, and discovery evidence all line up.'
    WHEN 2 THEN 'Multiple evidence paths are available, but attribution is still partial.'
    ELSE 'This record currently relies on a single evidence path.'
  END AS evidence_confidence_reason
FROM posture_inputs pi;
$$;

DROP VIEW IF EXISTS connected_app_summaries_v;

DROP TABLE IF EXISTS connected_app_governance;

DROP TABLE IF EXISTS saas_app_governance_overrides;

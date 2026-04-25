-- name: DeleteConnectorSourceStateAll :exec
DELETE FROM connector_source_state;

-- name: UpsertConnectorSourceState :exec
INSERT INTO connector_source_state (
  source_kind,
  source_name,
  enabled,
  configured,
  discovery_enabled,
  last_success_at,
  fresh_until_at,
  updated_at
)
VALUES (
  sqlc.arg(source_kind)::text,
  sqlc.arg(source_name)::text,
  sqlc.arg(enabled)::bool,
  sqlc.arg(configured)::bool,
  sqlc.arg(discovery_enabled)::bool,
  sqlc.narg(last_success_at)::timestamptz,
  sqlc.narg(fresh_until_at)::timestamptz,
  now()
)
ON CONFLICT (source_kind, source_name) DO UPDATE SET
  enabled = EXCLUDED.enabled,
  configured = EXCLUDED.configured,
  discovery_enabled = EXCLUDED.discovery_enabled,
  last_success_at = EXCLUDED.last_success_at,
  fresh_until_at = EXCLUDED.fresh_until_at,
  updated_at = now();

-- name: ListLatestSuccessfulSyncRunsBySource :many
WITH normalized AS (
  SELECT
    CASE lower(trim(r.source_kind))
      WHEN 'aws_identity_center' THEN 'aws'
      ELSE lower(trim(r.source_kind))
    END::text AS source_kind,
    trim(r.source_name)::text AS source_name,
    r.finished_at
  FROM sync_runs r
  WHERE r.status = 'success'
    AND r.finished_at IS NOT NULL
    AND lower(trim(r.source_kind)) NOT IN ('okta_discovery', 'entra_discovery', 'google_workspace_discovery')
)
SELECT source_kind, source_name, finished_at::timestamptz AS last_success_at
FROM (
  SELECT DISTINCT ON (source_kind, source_name)
    source_kind,
    source_name,
    finished_at
  FROM normalized
  ORDER BY source_kind, source_name, finished_at DESC
) latest
ORDER BY source_kind, source_name;

-- name: StoredReadModelsNeedRebuild :one
SELECT (
  EXISTS (
    SELECT 1
    FROM saas_apps sa
    WHERE sa.projection_refreshed_at IS NULL
  )
  OR EXISTS (
    SELECT 1
    FROM saas_apps sa
    LEFT JOIN saas_app_risk_read_models risk
      ON risk.saas_app_id = sa.id
    WHERE risk.saas_app_id IS NULL
  )
  OR EXISTS (
    SELECT 1
    FROM app_assets aa
    WHERE aa.projection_refreshed_at IS NULL
  )
  OR EXISTS (
    SELECT 1
    FROM credential_artifacts ca
    LEFT JOIN credential_artifact_risk_read_models risk
      ON risk.credential_artifact_id = ca.id
    WHERE ca.expired_at IS NULL
      AND ca.last_observed_run_id IS NOT NULL
      AND (
        risk.credential_artifact_id IS NULL
        OR risk.policy_packs_json = '[]'::jsonb
      )
  )
  OR EXISTS (
    SELECT 1
    FROM non_human_principals nhp
    WHERE nhp.projection_refreshed_at IS NULL
      OR nhp.policy_packs_json = '[]'::jsonb
  )
  OR (
    NOT EXISTS (
      SELECT 1
      FROM non_human_principals
    )
    AND (
      EXISTS (
        SELECT 1
        FROM identities i
        WHERE i.kind IN ('service', 'bot')
      )
      OR EXISTS (
        SELECT 1
        FROM app_assets aa
        WHERE aa.expired_at IS NULL
          AND aa.last_observed_run_id IS NOT NULL
      )
    )
  )
)::bool AS needs_rebuild;

-- name: ListAllSaaSAppRiskInputs :many
SELECT
  pr.id::bigint AS saas_app_id,
  pr.canonical_key::text AS canonical_key,
  pr.display_name::text AS display_name,
  pr.primary_domain::text AS primary_domain,
  pr.vendor_name::text AS vendor_name,
  pr.bound_connector_kind::text AS source_kind,
  pr.bound_connector_source_name::text AS source_name,
  pr.actors_30d::bigint AS actors_30d,
  pr.has_privileged_scope::boolean AS has_privileged_scope,
  pr.has_confidential_scope::boolean AS has_confidential_scope,
  pr.managed_state::text AS managed_state,
  pr.managed_reason::text AS managed_reason,
  pr.owner_identity_id::bigint AS owner_identity_id,
  pr.governance_state::text AS governance_state,
  pr.review_disposition::text AS review_disposition,
  pr.follow_up_due_date::date AS follow_up_due_date,
  COALESCE(NULLIF(trim(go.business_criticality), ''), 'unknown')::text AS configured_business_criticality,
  COALESCE(NULLIF(trim(go.data_classification), ''), 'unknown')::text AS configured_data_classification,
  pr.connector_configured::boolean AS connector_binding_configured,
  pr.connector_enabled::boolean AS connector_binding_enabled,
  COALESCE(pr.fresh_until_at < now(), false)::boolean AS connector_binding_stale,
  (pr.managed_state = 'managed')::boolean AS connector_binding_healthy
FROM discovery_app_read_models_v pr
-- Re-read governance overrides directly because the risk evaluator needs raw
-- configured values; discovery_app_read_models_v exposes resolved effective values.
LEFT JOIN governance_subject_overrides go
  ON go.subject_kind = 'saas_app'
 AND go.subject_id = pr.id
ORDER BY pr.id ASC;

-- name: ListSaaSAppRiskInputsBySource :many
SELECT
  pr.id::bigint AS saas_app_id,
  pr.canonical_key::text AS canonical_key,
  pr.display_name::text AS display_name,
  pr.primary_domain::text AS primary_domain,
  pr.vendor_name::text AS vendor_name,
  pr.bound_connector_kind::text AS source_kind,
  pr.bound_connector_source_name::text AS source_name,
  pr.actors_30d::bigint AS actors_30d,
  pr.has_privileged_scope::boolean AS has_privileged_scope,
  pr.has_confidential_scope::boolean AS has_confidential_scope,
  pr.managed_state::text AS managed_state,
  pr.managed_reason::text AS managed_reason,
  pr.owner_identity_id::bigint AS owner_identity_id,
  pr.governance_state::text AS governance_state,
  pr.review_disposition::text AS review_disposition,
  pr.follow_up_due_date::date AS follow_up_due_date,
  COALESCE(NULLIF(trim(go.business_criticality), ''), 'unknown')::text AS configured_business_criticality,
  COALESCE(NULLIF(trim(go.data_classification), ''), 'unknown')::text AS configured_data_classification,
  pr.connector_configured::boolean AS connector_binding_configured,
  pr.connector_enabled::boolean AS connector_binding_enabled,
  COALESCE(pr.fresh_until_at < now(), false)::boolean AS connector_binding_stale,
  (pr.managed_state = 'managed')::boolean AS connector_binding_healthy
FROM discovery_app_read_models_v pr
-- Re-read governance overrides directly because the risk evaluator needs raw
-- configured values; discovery_app_read_models_v exposes resolved effective values.
LEFT JOIN governance_subject_overrides go
  ON go.subject_kind = 'saas_app'
 AND go.subject_id = pr.id
WHERE EXISTS (
  SELECT 1
  FROM saas_app_sources sas
  WHERE sas.saas_app_id = pr.id
    AND lower(trim(sas.source_kind)) = lower(trim(sqlc.arg(source_kind)::text))
    AND lower(trim(sas.source_name)) = lower(trim(sqlc.arg(source_name)::text))
)
ORDER BY pr.id ASC;

-- name: GetSaaSAppRiskInputByID :one
SELECT
  pr.id::bigint AS saas_app_id,
  pr.canonical_key::text AS canonical_key,
  pr.display_name::text AS display_name,
  pr.primary_domain::text AS primary_domain,
  pr.vendor_name::text AS vendor_name,
  pr.bound_connector_kind::text AS source_kind,
  pr.bound_connector_source_name::text AS source_name,
  pr.actors_30d::bigint AS actors_30d,
  pr.has_privileged_scope::boolean AS has_privileged_scope,
  pr.has_confidential_scope::boolean AS has_confidential_scope,
  pr.managed_state::text AS managed_state,
  pr.managed_reason::text AS managed_reason,
  pr.owner_identity_id::bigint AS owner_identity_id,
  pr.governance_state::text AS governance_state,
  pr.review_disposition::text AS review_disposition,
  pr.follow_up_due_date::date AS follow_up_due_date,
  COALESCE(NULLIF(trim(go.business_criticality), ''), 'unknown')::text AS configured_business_criticality,
  COALESCE(NULLIF(trim(go.data_classification), ''), 'unknown')::text AS configured_data_classification,
  pr.connector_configured::boolean AS connector_binding_configured,
  pr.connector_enabled::boolean AS connector_binding_enabled,
  COALESCE(pr.fresh_until_at < now(), false)::boolean AS connector_binding_stale,
  (pr.managed_state = 'managed')::boolean AS connector_binding_healthy
FROM discovery_app_read_models_v pr
-- Re-read governance overrides directly because the risk evaluator needs raw
-- configured values; discovery_app_read_models_v exposes resolved effective values.
LEFT JOIN governance_subject_overrides go
  ON go.subject_kind = 'saas_app'
 AND go.subject_id = pr.id
WHERE pr.id = sqlc.arg(saas_app_id)::bigint;

-- name: UpsertSaaSAppRiskReadModelsBulk :execrows
WITH input AS (
  SELECT
    i,
    (sqlc.arg(saas_app_ids)::bigint[])[i] AS saas_app_id,
    (sqlc.arg(risk_scores)::int[])[i] AS risk_score,
    (sqlc.arg(risk_levels)::text[])[i] AS risk_level,
    (sqlc.arg(risk_ranks)::int[])[i] AS risk_rank,
    (sqlc.arg(suggested_business_criticalities)::text[])[i] AS suggested_business_criticality,
    (sqlc.arg(suggested_data_classifications)::text[])[i] AS suggested_data_classification,
    (sqlc.arg(effective_business_criticalities)::text[])[i] AS effective_business_criticality,
    (sqlc.arg(effective_data_classifications)::text[])[i] AS effective_data_classification,
    (sqlc.arg(policy_packs_jsons)::jsonb[])[i] AS policy_packs_json
  FROM generate_subscripts(sqlc.arg(saas_app_ids)::bigint[], 1) AS s(i)
)
INSERT INTO saas_app_risk_read_models (
  saas_app_id,
  risk_score,
  risk_level,
  risk_rank,
  suggested_business_criticality,
  suggested_data_classification,
  effective_business_criticality,
  effective_data_classification,
  policy_packs_json,
  projection_refreshed_at
)
SELECT
  input.saas_app_id,
  input.risk_score,
  input.risk_level,
  input.risk_rank,
  input.suggested_business_criticality,
  input.suggested_data_classification,
  input.effective_business_criticality,
  input.effective_data_classification,
  input.policy_packs_json,
  now()
FROM input
ON CONFLICT (saas_app_id) DO UPDATE SET
  risk_score = EXCLUDED.risk_score,
  risk_level = EXCLUDED.risk_level,
  risk_rank = EXCLUDED.risk_rank,
  suggested_business_criticality = EXCLUDED.suggested_business_criticality,
  suggested_data_classification = EXCLUDED.suggested_data_classification,
  effective_business_criticality = EXCLUDED.effective_business_criticality,
  effective_data_classification = EXCLUDED.effective_data_classification,
  policy_packs_json = EXCLUDED.policy_packs_json,
  projection_refreshed_at = now();

-- name: DeleteAllCredentialArtifactRiskReadModels :exec
DELETE FROM credential_artifact_risk_read_models;

-- name: DeleteCredentialArtifactRiskReadModelsBySource :exec
DELETE FROM credential_artifact_risk_read_models risk
USING credential_artifacts ca
WHERE risk.credential_artifact_id = ca.id
  AND lower(trim(ca.source_kind)) = lower(trim(sqlc.arg(source_kind)::text))
  AND lower(trim(ca.source_name)) = lower(trim(sqlc.arg(source_name)::text));

-- name: ListAllCredentialArtifactRiskInputs :many
SELECT
  ca.id::bigint AS credential_artifact_id,
  ca.source_kind::text AS source_kind,
  ca.source_name::text AS source_name,
  ca.credential_kind::text AS credential_kind,
  ca.status::text AS status,
  ca.expires_at_source::timestamptz AS expires_at_source,
  ca.last_used_at_source::timestamptz AS last_used_at_source,
  ca.created_at_source::timestamptz AS created_at_source,
  ca.created_by_external_id::text AS created_by_external_id,
  ca.created_by_display_name::text AS created_by_display_name,
  ca.approved_by_external_id::text AS approved_by_external_id,
  ca.approved_by_display_name::text AS approved_by_display_name,
  ca.asset_ref_kind::text AS asset_ref_kind,
  ca.asset_ref_external_id::text AS asset_ref_external_id,
  ca.scope_json::jsonb AS scope_json
FROM credential_artifacts ca
WHERE ca.expired_at IS NULL
  AND ca.last_observed_run_id IS NOT NULL
ORDER BY ca.id ASC;

-- name: ListCredentialArtifactRiskInputsBySource :many
SELECT
  ca.id::bigint AS credential_artifact_id,
  ca.source_kind::text AS source_kind,
  ca.source_name::text AS source_name,
  ca.credential_kind::text AS credential_kind,
  ca.status::text AS status,
  ca.expires_at_source::timestamptz AS expires_at_source,
  ca.last_used_at_source::timestamptz AS last_used_at_source,
  ca.created_at_source::timestamptz AS created_at_source,
  ca.created_by_external_id::text AS created_by_external_id,
  ca.created_by_display_name::text AS created_by_display_name,
  ca.approved_by_external_id::text AS approved_by_external_id,
  ca.approved_by_display_name::text AS approved_by_display_name,
  ca.asset_ref_kind::text AS asset_ref_kind,
  ca.asset_ref_external_id::text AS asset_ref_external_id,
  ca.scope_json::jsonb AS scope_json
FROM credential_artifacts ca
WHERE ca.expired_at IS NULL
  AND ca.last_observed_run_id IS NOT NULL
  AND lower(trim(ca.source_kind)) = lower(trim(sqlc.arg(source_kind)::text))
  AND lower(trim(ca.source_name)) = lower(trim(sqlc.arg(source_name)::text))
ORDER BY ca.id ASC;

-- name: UpsertCredentialArtifactRiskReadModelsBulk :execrows
WITH input AS (
  SELECT
    i,
    (sqlc.arg(credential_artifact_ids)::bigint[])[i] AS credential_artifact_id,
    (sqlc.arg(risk_levels)::text[])[i] AS risk_level,
    (sqlc.arg(risk_ranks)::int[])[i] AS risk_rank,
    (sqlc.arg(risk_signals_jsons)::jsonb[])[i] AS risk_signals_json,
    (sqlc.arg(policy_packs_jsons)::jsonb[])[i] AS policy_packs_json
  FROM generate_subscripts(sqlc.arg(credential_artifact_ids)::bigint[], 1) AS s(i)
)
INSERT INTO credential_artifact_risk_read_models (
  credential_artifact_id,
  risk_level,
  risk_rank,
  risk_signals_json,
  policy_packs_json,
  projection_refreshed_at
)
SELECT
  input.credential_artifact_id,
  input.risk_level,
  input.risk_rank,
  input.risk_signals_json,
  input.policy_packs_json,
  now()
FROM input
ON CONFLICT (credential_artifact_id) DO UPDATE SET
  risk_level = EXCLUDED.risk_level,
  risk_rank = EXCLUDED.risk_rank,
  risk_signals_json = EXCLUDED.risk_signals_json,
  policy_packs_json = EXCLUDED.policy_packs_json,
  projection_refreshed_at = now();

-- name: RefreshAllSaaSAppReadModels :execrows
WITH actor_stats AS (
  SELECT
    e.saas_app_id AS saas_app_id,
    count(
      DISTINCT COALESCE(
        NULLIF(trim(e.actor_external_id), ''),
        NULLIF(lower(trim(e.actor_email)), '')
      )
    )::bigint AS actors_30d
  FROM saas_app_events e
  WHERE e.expired_at IS NULL
    AND e.last_observed_run_id IS NOT NULL
    AND e.observed_at >= now() - interval '30 days'
  GROUP BY e.saas_app_id
),
scope_flags AS (
  SELECT
    e.saas_app_id AS saas_app_id,
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
  WHERE e.expired_at IS NULL
    AND e.last_observed_run_id IS NOT NULL
  GROUP BY e.saas_app_id
),
merged AS (
  SELECT
    sa.id,
    COALESCE(ast.actors_30d, 0)::bigint AS actors_30d,
    COALESCE(sf.has_privileged_scope, false)::boolean AS has_privileged_scope,
    COALESCE(sf.has_confidential_scope, false)::boolean AS has_confidential_scope
  FROM saas_apps sa
  LEFT JOIN actor_stats ast
    ON ast.saas_app_id = sa.id
  LEFT JOIN scope_flags sf
    ON sf.saas_app_id = sa.id
)
UPDATE saas_apps sa
SET
  actors_30d = merged.actors_30d,
  has_privileged_scope = merged.has_privileged_scope,
  has_confidential_scope = merged.has_confidential_scope,
  projection_refreshed_at = now()
FROM merged
WHERE sa.id = merged.id;

-- name: RefreshSaaSAppReadModelsBySource :execrows
WITH affected_apps AS (
  SELECT DISTINCT sas.saas_app_id AS id
  FROM saas_app_sources sas
  WHERE lower(trim(sas.source_kind)) = lower(trim(sqlc.arg(source_kind)::text))
    AND lower(trim(sas.source_name)) = lower(trim(sqlc.arg(source_name)::text))
),
actor_stats AS (
  SELECT
    e.saas_app_id AS saas_app_id,
    count(
      DISTINCT COALESCE(
        NULLIF(trim(e.actor_external_id), ''),
        NULLIF(lower(trim(e.actor_email)), '')
      )
    )::bigint AS actors_30d
  FROM saas_app_events e
  JOIN affected_apps aa
    ON aa.id = e.saas_app_id
  WHERE e.expired_at IS NULL
    AND e.last_observed_run_id IS NOT NULL
    AND e.observed_at >= now() - interval '30 days'
  GROUP BY e.saas_app_id
),
scope_flags AS (
  SELECT
    e.saas_app_id AS saas_app_id,
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
  JOIN affected_apps aa
    ON aa.id = e.saas_app_id
  WHERE e.expired_at IS NULL
    AND e.last_observed_run_id IS NOT NULL
  GROUP BY e.saas_app_id
),
merged AS (
  SELECT
    aa.id,
    COALESCE(ast.actors_30d, 0)::bigint AS actors_30d,
    COALESCE(sf.has_privileged_scope, false)::boolean AS has_privileged_scope,
    COALESCE(sf.has_confidential_scope, false)::boolean AS has_confidential_scope
  FROM affected_apps aa
  LEFT JOIN actor_stats ast
    ON ast.saas_app_id = aa.id
  LEFT JOIN scope_flags sf
    ON sf.saas_app_id = aa.id
)
UPDATE saas_apps sa
SET
  actors_30d = merged.actors_30d,
  has_privileged_scope = merged.has_privileged_scope,
  has_confidential_scope = merged.has_confidential_scope,
  projection_refreshed_at = now()
FROM merged
WHERE sa.id = merged.id;

-- name: RefreshAllAppAssetReadModels :execrows
WITH assets AS (
  SELECT
    aa.id,
    aa.source_kind,
    aa.source_name,
    aa.asset_kind,
    aa.external_id,
    aa.last_observed_at
  FROM app_assets aa
),
owner_counts AS (
  SELECT
    aao.app_asset_id AS app_asset_id,
    count(*)::bigint AS owner_count
  FROM app_asset_owners aao
  WHERE aao.expired_at IS NULL
    AND aao.last_observed_run_id IS NOT NULL
  GROUP BY aao.app_asset_id
),
grant_counts AS (
  SELECT
    aa.id,
    count(ca.id)::bigint AS grant_count,
    count(
      DISTINCT COALESCE(
        NULLIF(trim(ca.created_by_external_id), ''),
        NULLIF(lower(trim(ca.created_by_display_name)), '')
      )
    )::bigint AS actor_count
  FROM assets aa
  LEFT JOIN credential_artifacts ca
    ON ca.source_kind = aa.source_kind
   AND ca.source_name = aa.source_name
   AND ca.asset_ref_kind = aa.asset_kind
   AND ca.asset_ref_external_id = (aa.asset_kind || ':' || aa.external_id)
   AND ca.expired_at IS NULL
   AND ca.last_observed_run_id IS NOT NULL
  GROUP BY aa.id
),
discovery_source_stats AS (
  SELECT
    aa.id,
    count(sas.id)::bigint AS discovery_source_count,
    NULLIF(
      GREATEST(
        COALESCE(max(sas.last_observed_at), '-infinity'::timestamptz),
        COALESCE(max(sas.seen_at), '-infinity'::timestamptz)
      ),
      '-infinity'::timestamptz
    )::timestamptz AS last_source_seen_at
  FROM assets aa
  LEFT JOIN saas_app_sources sas
    ON sas.source_kind = aa.source_kind
   AND sas.source_name = aa.source_name
   AND sas.source_app_id = aa.external_id
   AND sas.expired_at IS NULL
   AND sas.last_observed_run_id IS NOT NULL
  GROUP BY aa.id
),
discovery_event_stats AS (
  SELECT
    aa.id,
    count(e.id) FILTER (
      WHERE e.observed_at >= now() - interval '30 days'
    )::bigint AS discovery_event_count_30d,
    max(e.observed_at)::timestamptz AS last_event_at
  FROM assets aa
  LEFT JOIN saas_app_events e
    ON e.source_kind = aa.source_kind
   AND e.source_name = aa.source_name
   AND e.source_app_id = aa.external_id
   AND e.expired_at IS NULL
   AND e.last_observed_run_id IS NOT NULL
  GROUP BY aa.id
),
merged AS (
  SELECT
    aa.id,
    COALESCE(oc.owner_count, 0)::bigint AS owner_count,
    COALESCE(gc.grant_count, 0)::bigint AS grant_count,
    COALESCE(gc.actor_count, 0)::bigint AS actor_count,
    COALESCE(dss.discovery_source_count, 0)::bigint AS discovery_source_count,
    COALESCE(des.discovery_event_count_30d, 0)::bigint AS discovery_event_count_30d,
    COALESCE(
      NULLIF(
        GREATEST(
          COALESCE(dss.last_source_seen_at, '-infinity'::timestamptz),
          COALESCE(des.last_event_at, '-infinity'::timestamptz)
        ),
        '-infinity'::timestamptz
      )::timestamptz,
      aa.last_observed_at
    )::timestamptz AS evidence_last_seen_at
  FROM assets aa
  LEFT JOIN owner_counts oc
    ON oc.app_asset_id = aa.id
  LEFT JOIN grant_counts gc
    ON gc.id = aa.id
  LEFT JOIN discovery_source_stats dss
    ON dss.id = aa.id
  LEFT JOIN discovery_event_stats des
    ON des.id = aa.id
)
UPDATE app_assets aa
SET
  owner_count = merged.owner_count,
  grant_count = merged.grant_count,
  actor_count = merged.actor_count,
  discovery_source_count = merged.discovery_source_count,
  discovery_event_count_30d = merged.discovery_event_count_30d,
  evidence_last_seen_at = merged.evidence_last_seen_at,
  projection_refreshed_at = now()
FROM merged
WHERE aa.id = merged.id;

-- name: DeleteAllNonHumanPrincipalReadModels :exec
DELETE FROM non_human_principals;

-- name: DeleteNonHumanPrincipalPolicyReadModelsBySource :exec
WITH requested_source AS (
  SELECT
    lower(trim(sqlc.arg(source_kind)::text)) AS source_kind,
    lower(trim(sqlc.arg(source_name)::text)) AS source_name
),
affected_identity_ids AS (
  SELECT DISTINCT i.id AS identity_id
  FROM identities i
  JOIN identity_accounts ia ON ia.identity_id = i.id
  JOIN accounts a ON a.id = ia.account_id
  JOIN requested_source rs
    ON rs.source_kind <> ''
   AND rs.source_name <> ''
   AND lower(trim(a.source_kind)) = rs.source_kind
   AND lower(trim(a.source_name)) = rs.source_name
  WHERE i.kind IN ('service', 'bot')
  UNION
  SELECT DISTINCT nhp.identity_id
  FROM non_human_principals nhp
  JOIN requested_source rs
    ON rs.source_kind <> ''
   AND rs.source_name <> ''
   AND lower(trim(nhp.source_kind)) = rs.source_kind
   AND lower(trim(nhp.source_name)) = rs.source_name
  WHERE nhp.identity_id > 0
),
affected_asset_ids AS (
  SELECT DISTINCT aa.id AS app_asset_id
  FROM app_assets aa
  JOIN requested_source rs
    ON rs.source_kind <> ''
   AND rs.source_name <> ''
   AND lower(trim(aa.source_kind)) = rs.source_kind
   AND lower(trim(aa.source_name)) = rs.source_name
  UNION
  SELECT DISTINCT nhp.app_asset_id
  FROM non_human_principals nhp
  JOIN requested_source rs
    ON rs.source_kind <> ''
   AND rs.source_name <> ''
   AND lower(trim(nhp.source_kind)) = rs.source_kind
   AND lower(trim(nhp.source_name)) = rs.source_name
  WHERE nhp.app_asset_id > 0
),
affected_principal_refs AS (
  SELECT 'identity-' || ai.identity_id::text AS principal_ref
  FROM affected_identity_ids ai
  UNION
  SELECT 'app-asset-' || aa.app_asset_id::text AS principal_ref
  FROM affected_asset_ids aa
)
DELETE FROM non_human_principals nhp
USING affected_principal_refs apr
WHERE nhp.principal_ref = apr.principal_ref;

-- name: ListAllNonHumanPrincipalRiskInputs :many
SELECT
  pr.principal_ref::text AS principal_ref,
  pr.identity_id::bigint AS identity_id,
  pr.app_asset_id::bigint AS app_asset_id,
  pr.principal_type::text AS principal_type,
  pr.source_kind::text AS source_kind,
  pr.source_name::text AS source_name,
  pr.display_name::text AS display_name,
  pr.secondary_name::text AS secondary_name,
  pr.secondary_name::text AS primary_email,
  pr.linked_assets_count::bigint AS linked_assets_count,
  pr.linked_credentials_count::bigint AS linked_credentials_count,
  pr.last_seen_at::timestamptz AS last_seen_at,
  pr.activity_state::text AS activity_state,
  pr.freshness_state::text AS freshness_state,
  pr.governance_state::text AS governance_state,
  pr.accountable_owner_identity_id::bigint AS accountable_owner_identity_id,
  pr.accountable_owner_display_name::text AS accountable_owner_display_name,
  pr.accountable_owner_primary_email::text AS accountable_owner_primary_email,
  pr.owner_presence::text AS owner_presence,
  pr.has_critical_credential::boolean AS has_critical_credential,
  pr.has_high_risk_credential::boolean AS has_high_risk_credential,
  pr.has_expired_credential::boolean AS has_expired_credential,
  pr.has_expiring_credential::boolean AS has_expiring_credential,
  pr.has_unused_credential::boolean AS has_unused_credential,
  pr.has_stale_evidence::boolean AS has_stale_evidence
FROM non_human_principal_projection_v pr
ORDER BY pr.principal_ref;

-- name: ListNonHumanPrincipalRiskInputsBySource :many
WITH requested_source AS (
  SELECT
    lower(trim(sqlc.arg(source_kind)::text)) AS source_kind,
    lower(trim(sqlc.arg(source_name)::text)) AS source_name
),
affected_identity_ids AS (
  SELECT DISTINCT i.id AS identity_id
  FROM identities i
  JOIN identity_accounts ia ON ia.identity_id = i.id
  JOIN accounts a ON a.id = ia.account_id
  JOIN requested_source rs
    ON rs.source_kind <> ''
   AND rs.source_name <> ''
   AND lower(trim(a.source_kind)) = rs.source_kind
   AND lower(trim(a.source_name)) = rs.source_name
  WHERE i.kind IN ('service', 'bot')
  UNION
  SELECT DISTINCT nhp.identity_id
  FROM non_human_principals nhp
  JOIN requested_source rs
    ON rs.source_kind <> ''
   AND rs.source_name <> ''
   AND lower(trim(nhp.source_kind)) = rs.source_kind
   AND lower(trim(nhp.source_name)) = rs.source_name
  WHERE nhp.identity_id > 0
),
affected_asset_ids AS (
  SELECT DISTINCT aa.id AS app_asset_id
  FROM app_assets aa
  JOIN requested_source rs
    ON rs.source_kind <> ''
   AND rs.source_name <> ''
   AND lower(trim(aa.source_kind)) = rs.source_kind
   AND lower(trim(aa.source_name)) = rs.source_name
  UNION
  SELECT DISTINCT nhp.app_asset_id
  FROM non_human_principals nhp
  JOIN requested_source rs
    ON rs.source_kind <> ''
   AND rs.source_name <> ''
   AND lower(trim(nhp.source_kind)) = rs.source_kind
   AND lower(trim(nhp.source_name)) = rs.source_name
  WHERE nhp.app_asset_id > 0
),
affected_principal_refs AS (
  SELECT 'identity-' || ai.identity_id::text AS principal_ref
  FROM affected_identity_ids ai
  UNION
  SELECT 'app-asset-' || aa.app_asset_id::text AS principal_ref
  FROM affected_asset_ids aa
)
SELECT
  pr.principal_ref::text AS principal_ref,
  pr.identity_id::bigint AS identity_id,
  pr.app_asset_id::bigint AS app_asset_id,
  pr.principal_type::text AS principal_type,
  pr.source_kind::text AS source_kind,
  pr.source_name::text AS source_name,
  pr.display_name::text AS display_name,
  pr.secondary_name::text AS secondary_name,
  pr.secondary_name::text AS primary_email,
  pr.linked_assets_count::bigint AS linked_assets_count,
  pr.linked_credentials_count::bigint AS linked_credentials_count,
  pr.last_seen_at::timestamptz AS last_seen_at,
  pr.activity_state::text AS activity_state,
  pr.freshness_state::text AS freshness_state,
  pr.governance_state::text AS governance_state,
  pr.accountable_owner_identity_id::bigint AS accountable_owner_identity_id,
  pr.accountable_owner_display_name::text AS accountable_owner_display_name,
  pr.accountable_owner_primary_email::text AS accountable_owner_primary_email,
  pr.owner_presence::text AS owner_presence,
  pr.has_critical_credential::boolean AS has_critical_credential,
  pr.has_high_risk_credential::boolean AS has_high_risk_credential,
  pr.has_expired_credential::boolean AS has_expired_credential,
  pr.has_expiring_credential::boolean AS has_expiring_credential,
  pr.has_unused_credential::boolean AS has_unused_credential,
  pr.has_stale_evidence::boolean AS has_stale_evidence
FROM non_human_principal_projection_v pr
JOIN affected_principal_refs apr
  ON apr.principal_ref = pr.principal_ref
ORDER BY pr.principal_ref;

-- name: UpsertNonHumanPrincipalReadModelsBulk :execrows
WITH input AS (
  SELECT
    i,
    (sqlc.arg(principal_refs)::text[])[i] AS principal_ref,
    (sqlc.arg(identity_ids)::bigint[])[i] AS identity_id,
    (sqlc.arg(app_asset_ids)::bigint[])[i] AS app_asset_id,
    (sqlc.arg(principal_types)::text[])[i] AS principal_type,
    (sqlc.arg(source_kinds)::text[])[i] AS source_kind,
    (sqlc.arg(source_names)::text[])[i] AS source_name,
    (sqlc.arg(display_names)::text[])[i] AS display_name,
    (sqlc.arg(secondary_names)::text[])[i] AS secondary_name,
    (sqlc.arg(linked_assets_counts)::bigint[])[i] AS linked_assets_count,
    (sqlc.arg(linked_credentials_counts)::bigint[])[i] AS linked_credentials_count,
    (sqlc.arg(last_seen_ats)::timestamptz[])[i] AS last_seen_at,
    (sqlc.arg(activity_states)::text[])[i] AS activity_state,
    (sqlc.arg(freshness_states)::text[])[i] AS freshness_state,
    (sqlc.arg(governance_states)::text[])[i] AS governance_state,
    (sqlc.arg(accountable_owner_identity_ids)::bigint[])[i] AS accountable_owner_identity_id,
    (sqlc.arg(accountable_owner_display_names)::text[])[i] AS accountable_owner_display_name,
    (sqlc.arg(accountable_owner_primary_emails)::text[])[i] AS accountable_owner_primary_email,
    (sqlc.arg(owner_presences)::text[])[i] AS owner_presence,
    (sqlc.arg(has_critical_credentials)::boolean[])[i] AS has_critical_credential,
    (sqlc.arg(has_high_risk_credentials)::boolean[])[i] AS has_high_risk_credential,
    (sqlc.arg(has_expired_credentials)::boolean[])[i] AS has_expired_credential,
    (sqlc.arg(has_expiring_credentials)::boolean[])[i] AS has_expiring_credential,
    (sqlc.arg(has_unused_credentials)::boolean[])[i] AS has_unused_credential,
    (sqlc.arg(has_stale_evidences)::boolean[])[i] AS has_stale_evidence,
    (sqlc.arg(risk_reason_counts)::int[])[i] AS risk_reason_count,
    (sqlc.arg(risk_levels)::text[])[i] AS risk_level,
    (sqlc.arg(risk_signals_jsons)::jsonb[])[i] AS risk_signals_json,
    (sqlc.arg(policy_packs_jsons)::jsonb[])[i] AS policy_packs_json
  FROM generate_subscripts(sqlc.arg(principal_refs)::text[], 1) AS s(i)
)
INSERT INTO non_human_principals (
  principal_ref,
  identity_id,
  app_asset_id,
  principal_type,
  source_kind,
  source_name,
  display_name,
  secondary_name,
  linked_assets_count,
  linked_credentials_count,
  last_seen_at,
  activity_state,
  freshness_state,
  governance_state,
  accountable_owner_identity_id,
  accountable_owner_display_name,
  accountable_owner_primary_email,
  owner_presence,
  has_critical_credential,
  has_high_risk_credential,
  has_expired_credential,
  has_expiring_credential,
  has_unused_credential,
  has_stale_evidence,
  risk_reason_count,
  risk_level,
  risk_signals_json,
  policy_packs_json,
  projection_refreshed_at
)
SELECT
  input.principal_ref,
  input.identity_id,
  input.app_asset_id,
  input.principal_type,
  input.source_kind,
  input.source_name,
  input.display_name,
  input.secondary_name,
  input.linked_assets_count,
  input.linked_credentials_count,
  input.last_seen_at,
  input.activity_state,
  input.freshness_state,
  input.governance_state,
  input.accountable_owner_identity_id,
  input.accountable_owner_display_name,
  input.accountable_owner_primary_email,
  input.owner_presence,
  input.has_critical_credential,
  input.has_high_risk_credential,
  input.has_expired_credential,
  input.has_expiring_credential,
  input.has_unused_credential,
  input.has_stale_evidence,
  input.risk_reason_count,
  input.risk_level,
  input.risk_signals_json,
  input.policy_packs_json,
  now()
FROM input
ON CONFLICT (principal_ref) DO UPDATE SET
  identity_id = EXCLUDED.identity_id,
  app_asset_id = EXCLUDED.app_asset_id,
  principal_type = EXCLUDED.principal_type,
  source_kind = EXCLUDED.source_kind,
  source_name = EXCLUDED.source_name,
  display_name = EXCLUDED.display_name,
  secondary_name = EXCLUDED.secondary_name,
  linked_assets_count = EXCLUDED.linked_assets_count,
  linked_credentials_count = EXCLUDED.linked_credentials_count,
  last_seen_at = EXCLUDED.last_seen_at,
  activity_state = EXCLUDED.activity_state,
  freshness_state = EXCLUDED.freshness_state,
  governance_state = EXCLUDED.governance_state,
  accountable_owner_identity_id = EXCLUDED.accountable_owner_identity_id,
  accountable_owner_display_name = EXCLUDED.accountable_owner_display_name,
  accountable_owner_primary_email = EXCLUDED.accountable_owner_primary_email,
  owner_presence = EXCLUDED.owner_presence,
  has_critical_credential = EXCLUDED.has_critical_credential,
  has_high_risk_credential = EXCLUDED.has_high_risk_credential,
  has_expired_credential = EXCLUDED.has_expired_credential,
  has_expiring_credential = EXCLUDED.has_expiring_credential,
  has_unused_credential = EXCLUDED.has_unused_credential,
  has_stale_evidence = EXCLUDED.has_stale_evidence,
  risk_reason_count = EXCLUDED.risk_reason_count,
  risk_level = EXCLUDED.risk_level,
  risk_signals_json = EXCLUDED.risk_signals_json,
  policy_packs_json = EXCLUDED.policy_packs_json,
  projection_refreshed_at = now();

-- name: RefreshAllNonHumanPrincipalReadModels :execrows
WITH cleared AS (
  DELETE FROM non_human_principals
)
INSERT INTO non_human_principals (
  principal_ref,
  identity_id,
  app_asset_id,
  principal_type,
  source_kind,
  source_name,
  display_name,
  secondary_name,
  linked_assets_count,
  linked_credentials_count,
  last_seen_at,
  activity_state,
  freshness_state,
  governance_state,
  accountable_owner_identity_id,
  accountable_owner_display_name,
  accountable_owner_primary_email,
  owner_presence,
  has_critical_credential,
  has_high_risk_credential,
  has_expired_credential,
  has_expiring_credential,
  has_unused_credential,
  has_stale_evidence,
  risk_reason_count,
  risk_level,
  projection_refreshed_at
)
SELECT
  pr.principal_ref,
  pr.identity_id,
  pr.app_asset_id,
  pr.principal_type,
  pr.source_kind,
  pr.source_name,
  pr.display_name,
  pr.secondary_name,
  pr.linked_assets_count,
  pr.linked_credentials_count,
  pr.last_seen_at,
  pr.activity_state,
  pr.freshness_state,
  pr.governance_state,
  pr.accountable_owner_identity_id,
  pr.accountable_owner_display_name,
  pr.accountable_owner_primary_email,
  pr.owner_presence,
  pr.has_critical_credential,
  pr.has_high_risk_credential,
  pr.has_expired_credential,
  pr.has_expiring_credential,
  pr.has_unused_credential,
  pr.has_stale_evidence,
  pr.risk_reason_count,
  pr.risk_level,
  now()
FROM non_human_principal_projection_v pr;

-- name: RefreshNonHumanPrincipalReadModelsBySource :execrows
WITH requested_source AS (
  SELECT
    lower(trim(sqlc.arg(source_kind)::text)) AS source_kind,
    lower(trim(sqlc.arg(source_name)::text)) AS source_name
),
affected_identity_ids AS (
  SELECT DISTINCT i.id AS identity_id
  FROM identities i
  JOIN identity_accounts ia ON ia.identity_id = i.id
  JOIN accounts a ON a.id = ia.account_id
  JOIN requested_source rs
    ON rs.source_kind <> ''
   AND rs.source_name <> ''
   AND lower(trim(a.source_kind)) = rs.source_kind
   AND lower(trim(a.source_name)) = rs.source_name
  WHERE i.kind IN ('service', 'bot')
  UNION
  SELECT DISTINCT nhp.identity_id
  FROM non_human_principals nhp
  JOIN requested_source rs
    ON rs.source_kind <> ''
   AND rs.source_name <> ''
   AND lower(trim(nhp.source_kind)) = rs.source_kind
   AND lower(trim(nhp.source_name)) = rs.source_name
  WHERE nhp.identity_id > 0
),
affected_asset_ids AS (
  SELECT DISTINCT aa.id AS app_asset_id
  FROM app_assets aa
  JOIN requested_source rs
    ON rs.source_kind <> ''
   AND rs.source_name <> ''
   AND lower(trim(aa.source_kind)) = rs.source_kind
   AND lower(trim(aa.source_name)) = rs.source_name
  UNION
  SELECT DISTINCT nhp.app_asset_id
  FROM non_human_principals nhp
  JOIN requested_source rs
    ON rs.source_kind <> ''
   AND rs.source_name <> ''
   AND lower(trim(nhp.source_kind)) = rs.source_kind
   AND lower(trim(nhp.source_name)) = rs.source_name
  WHERE nhp.app_asset_id > 0
),
affected_principal_refs AS (
  SELECT 'identity-' || ai.identity_id::text AS principal_ref
  FROM affected_identity_ids ai
  UNION
  SELECT 'app-asset-' || aa.app_asset_id::text AS principal_ref
  FROM affected_asset_ids aa
),
cleared AS (
  DELETE FROM non_human_principals nhp
  USING affected_principal_refs apr
  WHERE nhp.principal_ref = apr.principal_ref
)
INSERT INTO non_human_principals (
  principal_ref,
  identity_id,
  app_asset_id,
  principal_type,
  source_kind,
  source_name,
  display_name,
  secondary_name,
  linked_assets_count,
  linked_credentials_count,
  last_seen_at,
  activity_state,
  freshness_state,
  governance_state,
  accountable_owner_identity_id,
  accountable_owner_display_name,
  accountable_owner_primary_email,
  owner_presence,
  has_critical_credential,
  has_high_risk_credential,
  has_expired_credential,
  has_expiring_credential,
  has_unused_credential,
  has_stale_evidence,
  risk_reason_count,
  risk_level,
  projection_refreshed_at
)
SELECT
  pr.principal_ref,
  pr.identity_id,
  pr.app_asset_id,
  pr.principal_type,
  pr.source_kind,
  pr.source_name,
  pr.display_name,
  pr.secondary_name,
  pr.linked_assets_count,
  pr.linked_credentials_count,
  pr.last_seen_at,
  pr.activity_state,
  pr.freshness_state,
  pr.governance_state,
  pr.accountable_owner_identity_id,
  pr.accountable_owner_display_name,
  pr.accountable_owner_primary_email,
  pr.owner_presence,
  pr.has_critical_credential,
  pr.has_high_risk_credential,
  pr.has_expired_credential,
  pr.has_expiring_credential,
  pr.has_unused_credential,
  pr.has_stale_evidence,
  pr.risk_reason_count,
  pr.risk_level,
  now()
FROM non_human_principal_projection_v pr
JOIN affected_principal_refs apr
  ON apr.principal_ref = pr.principal_ref;

-- name: RefreshAppAssetReadModelsBySource :execrows
WITH assets AS (
  SELECT
    aa.id,
    aa.source_kind,
    aa.source_name,
    aa.asset_kind,
    aa.external_id,
    aa.last_observed_at
  FROM app_assets aa
  WHERE lower(trim(aa.source_kind)) = lower(trim(sqlc.arg(source_kind)::text))
    AND lower(trim(aa.source_name)) = lower(trim(sqlc.arg(source_name)::text))
),
owner_counts AS (
  SELECT
    aao.app_asset_id AS app_asset_id,
    count(*)::bigint AS owner_count
  FROM app_asset_owners aao
  JOIN assets aa
    ON aa.id = aao.app_asset_id
  WHERE aao.expired_at IS NULL
    AND aao.last_observed_run_id IS NOT NULL
  GROUP BY aao.app_asset_id
),
grant_counts AS (
  SELECT
    aa.id,
    count(ca.id)::bigint AS grant_count,
    count(
      DISTINCT COALESCE(
        NULLIF(trim(ca.created_by_external_id), ''),
        NULLIF(lower(trim(ca.created_by_display_name)), '')
      )
    )::bigint AS actor_count
  FROM assets aa
  LEFT JOIN credential_artifacts ca
    ON ca.source_kind = aa.source_kind
   AND ca.source_name = aa.source_name
   AND ca.asset_ref_kind = aa.asset_kind
   AND ca.asset_ref_external_id = (aa.asset_kind || ':' || aa.external_id)
   AND ca.expired_at IS NULL
   AND ca.last_observed_run_id IS NOT NULL
  GROUP BY aa.id
),
discovery_source_stats AS (
  SELECT
    aa.id,
    count(sas.id)::bigint AS discovery_source_count,
    NULLIF(
      GREATEST(
        COALESCE(max(sas.last_observed_at), '-infinity'::timestamptz),
        COALESCE(max(sas.seen_at), '-infinity'::timestamptz)
      ),
      '-infinity'::timestamptz
    )::timestamptz AS last_source_seen_at
  FROM assets aa
  LEFT JOIN saas_app_sources sas
    ON sas.source_kind = aa.source_kind
   AND sas.source_name = aa.source_name
   AND sas.source_app_id = aa.external_id
   AND sas.expired_at IS NULL
   AND sas.last_observed_run_id IS NOT NULL
  GROUP BY aa.id
),
discovery_event_stats AS (
  SELECT
    aa.id,
    count(e.id) FILTER (
      WHERE e.observed_at >= now() - interval '30 days'
    )::bigint AS discovery_event_count_30d,
    max(e.observed_at)::timestamptz AS last_event_at
  FROM assets aa
  LEFT JOIN saas_app_events e
    ON e.source_kind = aa.source_kind
   AND e.source_name = aa.source_name
   AND e.source_app_id = aa.external_id
   AND e.expired_at IS NULL
   AND e.last_observed_run_id IS NOT NULL
  GROUP BY aa.id
),
merged AS (
  SELECT
    aa.id,
    COALESCE(oc.owner_count, 0)::bigint AS owner_count,
    COALESCE(gc.grant_count, 0)::bigint AS grant_count,
    COALESCE(gc.actor_count, 0)::bigint AS actor_count,
    COALESCE(dss.discovery_source_count, 0)::bigint AS discovery_source_count,
    COALESCE(des.discovery_event_count_30d, 0)::bigint AS discovery_event_count_30d,
    COALESCE(
      NULLIF(
        GREATEST(
          COALESCE(dss.last_source_seen_at, '-infinity'::timestamptz),
          COALESCE(des.last_event_at, '-infinity'::timestamptz)
        ),
        '-infinity'::timestamptz
      )::timestamptz,
      aa.last_observed_at
    )::timestamptz AS evidence_last_seen_at
  FROM assets aa
  LEFT JOIN owner_counts oc
    ON oc.app_asset_id = aa.id
  LEFT JOIN grant_counts gc
    ON gc.id = aa.id
  LEFT JOIN discovery_source_stats dss
    ON dss.id = aa.id
  LEFT JOIN discovery_event_stats des
    ON des.id = aa.id
)
UPDATE app_assets aa
SET
  owner_count = merged.owner_count,
  grant_count = merged.grant_count,
  actor_count = merged.actor_count,
  discovery_source_count = merged.discovery_source_count,
  discovery_event_count_30d = merged.discovery_event_count_30d,
  evidence_last_seen_at = merged.evidence_last_seen_at,
  projection_refreshed_at = now()
FROM merged
WHERE aa.id = merged.id;

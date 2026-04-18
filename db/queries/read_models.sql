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
    FROM app_assets aa
    WHERE aa.projection_refreshed_at IS NULL
  )
  OR EXISTS (
    SELECT 1
    FROM non_human_principals nhp
    WHERE nhp.projection_refreshed_at IS NULL
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

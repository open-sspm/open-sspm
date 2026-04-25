-- name: InsertNonHumanAccessEvent :exec
INSERT INTO non_human_access_events (
  auth_user_id,
  auth_user_role,
  event_kind,
  principal_ref,
  target_kind,
  target_ref,
  filter_signature,
  occurred_at,
  created_at
)
VALUES (
  sqlc.arg(auth_user_id)::bigint,
  lower(trim(sqlc.arg(auth_user_role)::text)),
  trim(sqlc.arg(event_kind)::text),
  COALESCE(NULLIF(trim(sqlc.arg(principal_ref)::text), ''), ''),
  COALESCE(NULLIF(trim(sqlc.arg(target_kind)::text), ''), ''),
  COALESCE(NULLIF(trim(sqlc.arg(target_ref)::text), ''), ''),
  COALESCE(NULLIF(trim(sqlc.arg(filter_signature)::text), ''), ''),
  sqlc.arg(occurred_at)::timestamptz,
  now()
);

-- name: CountConfiguredNonHumanPrincipalOwnerCoverage :one
SELECT
  count(*)::bigint AS principal_count,
  count(*) FILTER (WHERE pr.owner_presence = 'owned')::bigint AS with_owner_count
FROM non_human_principal_read_models_v pr
JOIN connector_source_state css
  ON css.source_kind = pr.source_kind
 AND lower(trim(css.source_name)) = lower(trim(pr.source_name))
WHERE css.configured = true;

-- name: CountConfiguredNonHumanHighRiskCredentialAttribution :one
WITH linked_credentials AS (
  SELECT
    ca.id::bigint AS credential_id,
    COALESCE(risk.risk_rank, 1)::int AS risk_rank,
    (
      pr.owner_presence = 'owned'
      OR NULLIF(trim(ca.created_by_display_name), '') IS NOT NULL
      OR NULLIF(trim(ca.created_by_external_id), '') IS NOT NULL
      OR NULLIF(trim(ca.approved_by_display_name), '') IS NOT NULL
      OR NULLIF(trim(ca.approved_by_external_id), '') IS NOT NULL
    )::boolean AS has_attribution
  FROM non_human_principal_asset_links_v nhpal
  JOIN non_human_principal_read_models_v pr
    ON pr.principal_ref = nhpal.principal_ref
  JOIN connector_source_state css
    ON css.source_kind = pr.source_kind
   AND lower(trim(css.source_name)) = lower(trim(pr.source_name))
   AND css.configured = true
  JOIN app_assets aa
    ON aa.id = nhpal.app_asset_id
  JOIN non_human_app_asset_credential_refs_v nhac
    ON nhac.app_asset_id = aa.id
  JOIN credential_artifacts ca
    ON ca.source_kind = nhac.source_kind
   AND ca.source_name = nhac.source_name
   AND ca.asset_ref_kind = nhac.asset_ref_kind
   AND ca.asset_ref_external_id = nhac.asset_ref_external_id
   AND ca.expired_at IS NULL
   AND ca.last_observed_run_id IS NOT NULL
  LEFT JOIN credential_artifact_risk_read_models risk
    ON risk.credential_artifact_id = ca.id
),
deduped AS (
  SELECT
    lc.credential_id,
    max(lc.risk_rank)::int AS risk_rank,
    bool_or(lc.has_attribution)::boolean AS has_attribution
  FROM linked_credentials lc
  GROUP BY lc.credential_id
)
SELECT
  count(*) FILTER (WHERE d.risk_rank >= 3)::bigint AS high_risk_credential_count,
  count(*) FILTER (WHERE d.risk_rank >= 3 AND d.has_attribution)::bigint AS high_risk_with_attribution_count
FROM deduped d;

-- name: CountNonHumanAccessWeeklyAdminReviewSessions :one
SELECT count(DISTINCT e.auth_user_id)::bigint
FROM non_human_access_events e
WHERE e.auth_user_role = 'admin'
  AND e.auth_user_id IS NOT NULL
  AND e.event_kind IN ('inventory_view', 'filter_use', 'detail_open', 'outbound_click')
  AND e.occurred_at >= sqlc.arg(since)::timestamptz;

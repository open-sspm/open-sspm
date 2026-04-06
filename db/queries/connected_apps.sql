-- name: CountAppAssetGovernanceBySourceAndQueryAndState :one
SELECT count(*)
FROM connected_app_read_models_v pr
WHERE pr.source_kind = sqlc.arg(source_kind)::text
  AND pr.source_name = sqlc.arg(source_name)::text
  AND pr.asset_kind = sqlc.arg(asset_kind)::text
  AND (
    sqlc.arg(governance_state)::text = ''
    OR pr.governance_state = sqlc.arg(governance_state)::text
  )
  AND (
    sqlc.arg(query)::text = ''
    OR pr.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR pr.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
  );

-- name: CountAppAssetGovernanceGroupedByState :many
SELECT
  pr.governance_state::text AS governance_state,
  count(*)::bigint AS app_count
FROM connected_app_read_models_v pr
WHERE pr.source_kind = sqlc.arg(source_kind)::text
  AND pr.source_name = sqlc.arg(source_name)::text
  AND pr.asset_kind = sqlc.arg(asset_kind)::text
  AND (
    sqlc.arg(query)::text = ''
    OR pr.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR pr.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
GROUP BY pr.governance_state
ORDER BY CASE pr.governance_state
  WHEN 'action_required' THEN 0
  WHEN 'in_review' THEN 1
  WHEN 'unreviewed' THEN 2
  WHEN 'ticketed' THEN 3
  WHEN 'approved' THEN 4
  ELSE 5
END;

-- name: ListAppAssetGovernancePageBySourceAndQueryAndState :many
SELECT
  pr.id::bigint AS id,
  pr.source_kind::text AS source_kind,
  pr.source_name::text AS source_name,
  pr.asset_kind::text AS asset_kind,
  pr.external_id::text AS external_id,
  pr.parent_external_id::text AS parent_external_id,
  pr.display_name::text AS display_name,
  pr.status::text AS status,
  pr.created_at_source::timestamptz AS created_at_source,
  pr.updated_at_source::timestamptz AS updated_at_source,
  pr.raw_json::jsonb AS raw_json,
  pr.seen_in_run_id::bigint AS seen_in_run_id,
  pr.seen_at::timestamptz AS seen_at,
  pr.last_observed_run_id::bigint AS last_observed_run_id,
  pr.last_observed_at::timestamptz AS last_observed_at,
  pr.expired_at::timestamptz AS expired_at,
  pr.expired_run_id::bigint AS expired_run_id,
  pr.created_at::timestamptz AS created_at,
  pr.updated_at::timestamptz AS updated_at,
  pr.governance_state::text AS governance_state,
  pr.ticket_ref::text AS ticket_ref,
  pr.notes::text AS notes,
  pr.governance_owner_identity_id::bigint AS governance_owner_identity_id,
  pr.governance_owner_display_name::text AS governance_owner_display_name,
  pr.governance_owner_primary_email::text AS governance_owner_primary_email,
  pr.governance_owner_kind::text AS governance_owner_kind,
  pr.owner_count::bigint AS owner_count,
  pr.grant_count::bigint AS grant_count,
  pr.actor_count::bigint AS actor_count,
  pr.discovery_source_count::bigint AS discovery_source_count,
  pr.discovery_event_count_30d::bigint AS discovery_event_count_30d,
  pr.evidence_last_seen_at::timestamptz AS evidence_last_seen_at,
  pr.suggested_business_criticality::text AS suggested_business_criticality,
  pr.suggested_data_classification::text AS suggested_data_classification,
  pr.effective_business_criticality::text AS effective_business_criticality,
  pr.effective_data_classification::text AS effective_data_classification,
  pr.evidence_freshness::text AS evidence_freshness,
  pr.evidence_confidence::text AS evidence_confidence,
  pr.evidence_confidence_reason::text AS evidence_confidence_reason
FROM connected_app_read_models_v pr
WHERE pr.source_kind = sqlc.arg(source_kind)::text
  AND pr.source_name = sqlc.arg(source_name)::text
  AND pr.asset_kind = sqlc.arg(asset_kind)::text
  AND (
    sqlc.arg(governance_state)::text = ''
    OR pr.governance_state = sqlc.arg(governance_state)::text
  )
  AND (
    sqlc.arg(query)::text = ''
    OR pr.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR pr.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
ORDER BY
  CASE pr.governance_state
    WHEN 'action_required' THEN 0
    WHEN 'in_review' THEN 1
    WHEN 'unreviewed' THEN 2
    WHEN 'ticketed' THEN 3
    WHEN 'approved' THEN 4
    ELSE 5
  END ASC,
  pr.discovery_event_count_30d DESC,
  pr.evidence_last_seen_at DESC,
  lower(COALESCE(NULLIF(trim(pr.display_name), ''), pr.external_id)) ASC,
  pr.id ASC
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: GetAppAssetPostureByID :one
SELECT
  pr.id::bigint AS id,
  pr.source_kind::text AS source_kind,
  pr.source_name::text AS source_name,
  pr.asset_kind::text AS asset_kind,
  pr.external_id::text AS external_id,
  pr.parent_external_id::text AS parent_external_id,
  pr.display_name::text AS display_name,
  pr.status::text AS status,
  pr.created_at_source::timestamptz AS created_at_source,
  pr.updated_at_source::timestamptz AS updated_at_source,
  pr.raw_json::jsonb AS raw_json,
  pr.seen_in_run_id::bigint AS seen_in_run_id,
  pr.seen_at::timestamptz AS seen_at,
  pr.last_observed_run_id::bigint AS last_observed_run_id,
  pr.last_observed_at::timestamptz AS last_observed_at,
  pr.expired_at::timestamptz AS expired_at,
  pr.expired_run_id::bigint AS expired_run_id,
  pr.created_at::timestamptz AS created_at,
  pr.updated_at::timestamptz AS updated_at,
  pr.governance_state::text AS governance_state,
  pr.ticket_ref::text AS ticket_ref,
  pr.notes::text AS notes,
  pr.governance_owner_identity_id::bigint AS governance_owner_identity_id,
  pr.governance_owner_display_name::text AS governance_owner_display_name,
  pr.governance_owner_primary_email::text AS governance_owner_primary_email,
  pr.governance_owner_kind::text AS governance_owner_kind,
  pr.owner_count::bigint AS owner_count,
  pr.grant_count::bigint AS grant_count,
  pr.actor_count::bigint AS actor_count,
  pr.discovery_source_count::bigint AS discovery_source_count,
  pr.discovery_event_count_30d::bigint AS discovery_event_count_30d,
  pr.evidence_last_seen_at::timestamptz AS evidence_last_seen_at,
  pr.suggested_business_criticality::text AS suggested_business_criticality,
  pr.suggested_data_classification::text AS suggested_data_classification,
  pr.effective_business_criticality::text AS effective_business_criticality,
  pr.effective_data_classification::text AS effective_data_classification,
  pr.evidence_freshness::text AS evidence_freshness,
  pr.evidence_confidence::text AS evidence_confidence,
  pr.evidence_confidence_reason::text AS evidence_confidence_reason
FROM connected_app_read_models_v pr
WHERE pr.id = sqlc.arg(id)::bigint;

-- name: UpsertAppAssetGovernance :one
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
VALUES (
  'app_asset',
  sqlc.arg(app_asset_id)::bigint,
  sqlc.arg(governance_state)::text,
  sqlc.narg(owner_identity_id)::bigint,
  sqlc.arg(ticket_ref)::text,
  sqlc.arg(notes)::text,
  sqlc.narg(updated_by_auth_user_id)::bigint,
  now()
)
ON CONFLICT (subject_kind, subject_id) DO UPDATE SET
  governance_state = EXCLUDED.governance_state,
  owner_identity_id = EXCLUDED.owner_identity_id,
  ticket_ref = EXCLUDED.ticket_ref,
  notes = EXCLUDED.notes,
  updated_by_auth_user_id = EXCLUDED.updated_by_auth_user_id,
  updated_at = now()
RETURNING *;

-- name: ListAppAssetDiscoverySourcesBySourceAppID :many
WITH scoped_sources AS (
  SELECT *
  FROM saas_app_sources sas
  WHERE sas.source_kind = sqlc.arg(source_kind)::text
    AND sas.source_name = sqlc.arg(source_name)::text
    AND sas.source_app_id = sqlc.arg(source_app_id)::text
    AND sas.expired_at IS NULL
    AND sas.last_observed_run_id IS NOT NULL
)
SELECT
  sas.*,
  pr.canonical_key::text AS canonical_key,
  pr.display_name::text AS discovery_display_name,
  pr.primary_domain::text AS discovery_primary_domain,
  pr.vendor_name::text AS discovery_vendor_name,
  pr.managed_state::text AS discovery_managed_state,
  pr.risk_level::text AS discovery_risk_level
FROM scoped_sources sas
JOIN discovery_app_read_models_v pr ON pr.id = sas.saas_app_id
ORDER BY sas.last_observed_at DESC, sas.id DESC;

-- name: ListAppAssetDiscoveryEventsBySourceAppID :many
SELECT *
FROM saas_app_events e
WHERE e.source_kind = sqlc.arg(source_kind)::text
  AND e.source_name = sqlc.arg(source_name)::text
  AND e.source_app_id = sqlc.arg(source_app_id)::text
  AND e.expired_at IS NULL
  AND e.last_observed_run_id IS NOT NULL
ORDER BY e.observed_at DESC, e.id DESC
LIMIT sqlc.arg(limit_rows)::int;

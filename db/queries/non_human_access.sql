-- name: CountNonHumanPrincipalsByFilters :one
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
)
SELECT count(*)
FROM non_human_principal_read_models_v pr
JOIN configured_sources cs
  ON cs.source_kind = pr.source_kind
 AND cs.source_name = pr.source_name
WHERE (
    sqlc.arg(query)::text = ''
    OR pr.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR pr.secondary_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR pr.accountable_owner_display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR pr.accountable_owner_primary_email ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
  AND (
    sqlc.arg(source_kind)::text = ''
    OR pr.source_kind = sqlc.arg(source_kind)::text
  )
  AND (
    sqlc.arg(source_name)::text = ''
    OR pr.source_name = sqlc.arg(source_name)::text
  )
  AND (
    sqlc.arg(principal_type)::text = ''
    OR pr.principal_type = sqlc.arg(principal_type)::text
  )
  AND (
    sqlc.arg(owner_presence)::text = ''
    OR pr.owner_presence = sqlc.arg(owner_presence)::text
  )
  AND (
    sqlc.arg(governance_state)::text = ''
    OR pr.governance_state = sqlc.arg(governance_state)::text
  )
  AND (
    sqlc.arg(risk_level)::text = ''
    OR pr.risk_level = sqlc.arg(risk_level)::text
  )
  AND (
    sqlc.arg(activity_state)::text = ''
    OR pr.activity_state = sqlc.arg(activity_state)::text
  )
  AND (
    sqlc.arg(freshness_state)::text = ''
    OR pr.freshness_state = sqlc.arg(freshness_state)::text
  );

-- name: ListNonHumanPrincipalsPageByFilters :many
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
),
base AS (
  SELECT pr.*
  FROM non_human_principal_read_models_v pr
  JOIN configured_sources cs
    ON cs.source_kind = pr.source_kind
   AND cs.source_name = pr.source_name
  WHERE (
      sqlc.arg(query)::text = ''
      OR pr.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
      OR pr.secondary_name ILIKE ('%' || sqlc.arg(query)::text || '%')
      OR pr.accountable_owner_display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
      OR pr.accountable_owner_primary_email ILIKE ('%' || sqlc.arg(query)::text || '%')
    )
    AND (
      sqlc.arg(source_kind)::text = ''
      OR pr.source_kind = sqlc.arg(source_kind)::text
    )
    AND (
      sqlc.arg(source_name)::text = ''
      OR pr.source_name = sqlc.arg(source_name)::text
    )
    AND (
      sqlc.arg(principal_type)::text = ''
      OR pr.principal_type = sqlc.arg(principal_type)::text
    )
    AND (
      sqlc.arg(owner_presence)::text = ''
      OR pr.owner_presence = sqlc.arg(owner_presence)::text
    )
    AND (
      sqlc.arg(governance_state)::text = ''
      OR pr.governance_state = sqlc.arg(governance_state)::text
    )
    AND (
      sqlc.arg(risk_level)::text = ''
      OR pr.risk_level = sqlc.arg(risk_level)::text
    )
    AND (
      sqlc.arg(activity_state)::text = ''
      OR pr.activity_state = sqlc.arg(activity_state)::text
    )
    AND (
      sqlc.arg(freshness_state)::text = ''
      OR pr.freshness_state = sqlc.arg(freshness_state)::text
    )
)
SELECT
  b.principal_ref,
  b.identity_id,
  b.app_asset_id,
  b.principal_type,
  b.source_kind,
  b.source_name,
  b.display_name,
  b.secondary_name,
  b.linked_assets_count,
  b.linked_credentials_count,
  b.last_seen_at,
  b.activity_state,
  b.freshness_state,
  b.governance_state,
  b.accountable_owner_identity_id,
  b.accountable_owner_display_name,
  b.accountable_owner_primary_email,
  b.owner_presence,
  b.has_critical_credential,
  b.has_high_risk_credential,
  b.has_expired_credential,
  b.has_expiring_credential,
  b.has_unused_credential,
  b.has_stale_evidence,
  b.risk_reason_count,
  b.risk_level
FROM base b
ORDER BY
  CASE
    WHEN sqlc.arg(sort_by)::text = '' THEN
      CASE b.risk_level
        WHEN 'critical' THEN 0
        WHEN 'high' THEN 1
        WHEN 'medium' THEN 2
        ELSE 3
      END
  END ASC,
  CASE WHEN sqlc.arg(sort_by)::text = '' THEN CASE b.owner_presence WHEN 'unknown' THEN 0 ELSE 1 END END ASC,
  CASE
    WHEN sqlc.arg(sort_by)::text = '' THEN
      CASE b.governance_state
        WHEN 'action_required' THEN 0
        WHEN 'in_review' THEN 1
        WHEN 'unreviewed' THEN 2
        WHEN 'ticketed' THEN 3
        ELSE 4
      END
  END ASC,
  CASE WHEN sqlc.arg(sort_by)::text = '' THEN b.last_seen_at END ASC NULLS FIRST,
  CASE WHEN sqlc.arg(sort_by)::text = '' THEN lower(COALESCE(NULLIF(trim(b.display_name), ''), b.principal_ref)) END ASC,

  CASE
    WHEN sqlc.arg(sort_by)::text = 'principal'
      AND sqlc.arg(sort_dir)::text = 'asc'
    THEN lower(COALESCE(NULLIF(trim(b.display_name), ''), b.principal_ref))
  END ASC,
  CASE
    WHEN sqlc.arg(sort_by)::text = 'principal'
      AND sqlc.arg(sort_dir)::text = 'desc'
    THEN lower(COALESCE(NULLIF(trim(b.display_name), ''), b.principal_ref))
  END DESC,

  CASE
    WHEN sqlc.arg(sort_by)::text = 'principal_type'
      AND sqlc.arg(sort_dir)::text = 'asc'
    THEN lower(b.principal_type)
  END ASC,
  CASE
    WHEN sqlc.arg(sort_by)::text = 'principal_type'
      AND sqlc.arg(sort_dir)::text = 'desc'
    THEN lower(b.principal_type)
  END DESC,

  CASE
    WHEN sqlc.arg(sort_by)::text = 'source'
      AND sqlc.arg(sort_dir)::text = 'asc'
    THEN lower(b.source_kind)
  END ASC,
  CASE
    WHEN sqlc.arg(sort_by)::text = 'source'
      AND sqlc.arg(sort_dir)::text = 'desc'
    THEN lower(b.source_kind)
  END DESC,
  CASE
    WHEN sqlc.arg(sort_by)::text = 'source'
      AND sqlc.arg(sort_dir)::text = 'asc'
    THEN lower(b.source_name)
  END ASC,
  CASE
    WHEN sqlc.arg(sort_by)::text = 'source'
      AND sqlc.arg(sort_dir)::text = 'desc'
    THEN lower(b.source_name)
  END DESC,

  CASE
    WHEN sqlc.arg(sort_by)::text = 'owner'
      AND sqlc.arg(sort_dir)::text = 'asc'
    THEN CASE b.owner_presence WHEN 'unknown' THEN 0 ELSE 1 END
  END ASC,
  CASE
    WHEN sqlc.arg(sort_by)::text = 'owner'
      AND sqlc.arg(sort_dir)::text = 'desc'
    THEN CASE b.owner_presence WHEN 'unknown' THEN 0 ELSE 1 END
  END DESC,
  CASE
    WHEN sqlc.arg(sort_by)::text = 'owner'
      AND sqlc.arg(sort_dir)::text = 'asc'
    THEN lower(COALESCE(NULLIF(trim(b.accountable_owner_display_name), ''), NULLIF(trim(b.accountable_owner_primary_email), ''), 'unknown'))
  END ASC,
  CASE
    WHEN sqlc.arg(sort_by)::text = 'owner'
      AND sqlc.arg(sort_dir)::text = 'desc'
    THEN lower(COALESCE(NULLIF(trim(b.accountable_owner_display_name), ''), NULLIF(trim(b.accountable_owner_primary_email), ''), 'unknown'))
  END DESC,

  CASE
    WHEN sqlc.arg(sort_by)::text = 'governance'
      AND sqlc.arg(sort_dir)::text = 'asc'
    THEN CASE b.governance_state
      WHEN 'action_required' THEN 0
      WHEN 'in_review' THEN 1
      WHEN 'unreviewed' THEN 2
      WHEN 'ticketed' THEN 3
      ELSE 4
    END
  END ASC,
  CASE
    WHEN sqlc.arg(sort_by)::text = 'governance'
      AND sqlc.arg(sort_dir)::text = 'desc'
    THEN CASE b.governance_state
      WHEN 'action_required' THEN 0
      WHEN 'in_review' THEN 1
      WHEN 'unreviewed' THEN 2
      WHEN 'ticketed' THEN 3
      ELSE 4
    END
  END DESC,

  CASE
    WHEN sqlc.arg(sort_by)::text = 'risk'
      AND sqlc.arg(sort_dir)::text = 'asc'
    THEN CASE b.risk_level
      WHEN 'critical' THEN 3
      WHEN 'high' THEN 2
      WHEN 'medium' THEN 1
      ELSE 0
    END
  END ASC,
  CASE
    WHEN sqlc.arg(sort_by)::text = 'risk'
      AND sqlc.arg(sort_dir)::text = 'desc'
    THEN CASE b.risk_level
      WHEN 'critical' THEN 3
      WHEN 'high' THEN 2
      WHEN 'medium' THEN 1
      ELSE 0
    END
  END DESC,

  CASE
    WHEN sqlc.arg(sort_by)::text = 'last_seen'
      AND sqlc.arg(sort_dir)::text = 'asc'
    THEN b.last_seen_at
  END ASC NULLS FIRST,
  CASE
    WHEN sqlc.arg(sort_by)::text = 'last_seen'
      AND sqlc.arg(sort_dir)::text = 'desc'
    THEN b.last_seen_at
  END DESC NULLS LAST,

  CASE
    WHEN sqlc.arg(sort_by)::text = 'freshness'
      AND sqlc.arg(sort_dir)::text = 'asc'
    THEN CASE b.freshness_state
      WHEN 'current' THEN 0
      WHEN 'unknown' THEN 1
      ELSE 2
    END
  END ASC,
  CASE
    WHEN sqlc.arg(sort_by)::text = 'freshness'
      AND sqlc.arg(sort_dir)::text = 'desc'
    THEN CASE b.freshness_state
      WHEN 'stale' THEN 2
      WHEN 'unknown' THEN 1
      ELSE 0
    END
  END DESC,

  lower(COALESCE(NULLIF(trim(b.display_name), ''), b.principal_ref)) ASC,
  b.principal_ref ASC
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: GetNonHumanPrincipalByRef :one
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
  pr.risk_level
FROM non_human_principal_read_models_v pr
WHERE pr.principal_ref = sqlc.arg(principal_ref)::text;

-- name: ListNonHumanPrincipalAssetsByRef :many
SELECT
  pr.id::bigint AS id,
  pr.source_kind::text AS source_kind,
  pr.source_name::text AS source_name,
  pr.asset_kind::text AS asset_kind,
  pr.external_id::text AS external_id,
  pr.parent_external_id::text AS parent_external_id,
  pr.display_name::text AS display_name,
  pr.status::text AS status,
  pr.governance_state::text AS governance_state,
  pr.governance_owner_identity_id::bigint AS governance_owner_identity_id,
  pr.governance_owner_display_name::text AS governance_owner_display_name,
  pr.governance_owner_primary_email::text AS governance_owner_primary_email,
  pr.grant_count::bigint AS grant_count,
  pr.evidence_last_seen_at::timestamptz AS evidence_last_seen_at,
  pr.evidence_freshness::text AS evidence_freshness,
  pr.evidence_confidence::text AS evidence_confidence,
  pr.evidence_confidence_reason::text AS evidence_confidence_reason
FROM non_human_principal_asset_links_v nhpal
JOIN connected_app_read_models_v pr ON pr.id = nhpal.app_asset_id
WHERE nhpal.principal_ref = sqlc.arg(principal_ref)::text
ORDER BY
  CASE pr.governance_state
    WHEN 'action_required' THEN 0
    WHEN 'in_review' THEN 1
    WHEN 'unreviewed' THEN 2
    WHEN 'ticketed' THEN 3
    ELSE 4
  END ASC,
  lower(COALESCE(NULLIF(trim(pr.display_name), ''), pr.external_id)) ASC,
  pr.id ASC;

-- name: ListNonHumanPrincipalCredentialsByRef :many
WITH credential_matches AS (
  SELECT DISTINCT ON (ca.id)
    ca.id::bigint AS id,
    ca.source_kind::text AS source_kind,
    ca.source_name::text AS source_name,
    ca.credential_kind::text AS credential_kind,
    ca.external_id::text AS external_id,
    COALESCE(NULLIF(trim(ca.display_name), ''), ca.external_id)::text AS display_name,
    ca.status::text AS status,
    credential_artifact_risk_level(
      ca.status,
      ca.credential_kind,
      ca.expires_at_source,
      ca.last_used_at_source,
      ca.created_by_external_id,
      ca.approved_by_external_id,
      now()
    )::text AS risk_level,
    ca.expires_at_source::timestamptz AS expires_at_source,
    ca.last_used_at_source::timestamptz AS last_used_at_source,
    ca.created_by_display_name::text AS created_by_display_name,
    ca.created_by_external_id::text AS created_by_external_id,
    ca.approved_by_display_name::text AS approved_by_display_name,
    ca.approved_by_external_id::text AS approved_by_external_id,
    aa.id::bigint AS app_asset_id,
    COALESCE(NULLIF(trim(aa.display_name), ''), aa.external_id)::text AS app_asset_display_name
  FROM non_human_principal_asset_links_v nhpal
  JOIN app_assets aa ON aa.id = nhpal.app_asset_id
  JOIN non_human_app_asset_credential_refs_v nhac
    ON nhac.app_asset_id = aa.id
  JOIN credential_artifacts ca
    ON ca.source_kind = nhac.source_kind
   AND ca.source_name = nhac.source_name
   AND ca.asset_ref_kind = nhac.asset_ref_kind
   AND ca.asset_ref_external_id = nhac.asset_ref_external_id
   AND ca.expired_at IS NULL
   AND ca.last_observed_run_id IS NOT NULL
  WHERE nhpal.principal_ref = sqlc.arg(principal_ref)::text
  ORDER BY
    ca.id,
    CASE nhac.asset_ref_kind
      WHEN aa.asset_kind THEN 0
      ELSE 1
    END ASC,
    length(nhac.asset_ref_external_id) DESC
)
SELECT
  cm.id,
  cm.source_kind,
  cm.source_name,
  cm.credential_kind,
  cm.external_id,
  cm.display_name,
  cm.status,
  cm.risk_level,
  cm.expires_at_source,
  cm.last_used_at_source,
  cm.created_by_display_name,
  cm.created_by_external_id,
  cm.approved_by_display_name,
  cm.approved_by_external_id,
  cm.app_asset_id,
  cm.app_asset_display_name
FROM credential_matches cm
ORDER BY
  CASE cm.risk_level
    WHEN 'critical' THEN 0
    WHEN 'high' THEN 1
    WHEN 'medium' THEN 2
    ELSE 3
  END ASC,
  cm.expires_at_source ASC NULLS LAST,
  lower(COALESCE(NULLIF(trim(cm.display_name), ''), cm.external_id)) ASC,
  cm.id ASC;

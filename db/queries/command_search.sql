-- name: SearchIdentitiesForCommand :many
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
),
matching_accounts AS (
  SELECT
    ia.identity_id,
    a.id AS account_id,
    a.source_kind,
    a.source_name,
    a.external_id,
    lower(trim(COALESCE(NULLIF(a.status, ''), NULLIF(a.raw_json->>'status', ''), 'unknown'))) AS normalized_status,
    CASE
      WHEN lower(trim(COALESCE(NULLIF(i.display_name, ''), ''))) = lower(trim(sqlc.arg(query)::text))
        OR lower(trim(COALESCE(NULLIF(i.primary_email, ''), ''))) = lower(trim(sqlc.arg(query)::text))
        OR lower(trim(a.external_id)) = lower(trim(sqlc.arg(query)::text))
      THEN 0
      WHEN lower(COALESCE(NULLIF(trim(i.display_name), ''), NULLIF(trim(i.primary_email), ''), '')) LIKE lower(trim(sqlc.arg(query)::text)) || '%'
        OR lower(trim(a.external_id)) LIKE lower(trim(sqlc.arg(query)::text)) || '%'
      THEN 1
      ELSE 2
    END AS match_rank
  FROM identities i
  JOIN identity_accounts ia ON ia.identity_id = i.id
  JOIN accounts a ON a.id = ia.account_id
  JOIN configured_sources cs
    ON cs.source_kind = a.source_kind
   AND cs.source_name = a.source_name
  WHERE a.expired_at IS NULL
    AND a.last_observed_run_id IS NOT NULL
    AND (
      i.primary_email ILIKE ('%' || sqlc.arg(query)::text || '%')
      OR i.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
      OR a.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    )
),
aggregates AS (
  SELECT
    ma.identity_id,
    MIN(ma.match_rank)::bigint AS match_rank,
    BOOL_OR(ma.normalized_status IN ('active', 'enabled')) AS has_active,
    BOOL_OR(ma.normalized_status IN ('suspended', 'disabled', 'inactive', 'locked')) AS has_suspended
  FROM matching_accounts ma
  GROUP BY ma.identity_id
),
primary_source AS (
  SELECT DISTINCT ON (ma.identity_id)
    ma.identity_id,
    ma.source_kind,
    ma.source_name
  FROM matching_accounts ma
  LEFT JOIN identity_source_settings iss
    ON iss.source_kind = ma.source_kind
   AND iss.source_name = ma.source_name
   AND iss.is_authoritative
  ORDER BY
    ma.identity_id,
    (iss.is_authoritative IS NOT TRUE),
    ma.match_rank,
    ma.account_id
)
SELECT
  i.id,
  COALESCE(i.display_name, '') AS display_name,
  COALESCE(i.primary_email, '') AS primary_email,
  COALESCE(i.kind, '') AS identity_type,
  COALESCE(ps.source_kind, '') AS source_kind,
  COALESCE(ps.source_name, '') AS source_name,
  CASE
    WHEN ag.has_active THEN 'active'
    WHEN ag.has_suspended THEN 'suspended'
    ELSE 'unknown'
  END AS status
FROM aggregates ag
JOIN identities i ON i.id = ag.identity_id
LEFT JOIN primary_source ps ON ps.identity_id = ag.identity_id
ORDER BY
  ag.match_rank ASC,
  ag.has_active DESC,
  lower(COALESCE(NULLIF(trim(i.display_name), ''), NULLIF(trim(i.primary_email), ''), 'identity ' || i.id::text)) ASC,
  i.id ASC
LIMIT sqlc.arg(limit_rows)::int;

-- name: SearchAppAssetsForCommand :many
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
)
SELECT
  aa.id,
  aa.source_kind,
  aa.source_name,
  aa.asset_kind,
  COALESCE(NULLIF(trim(aa.display_name), ''), aa.external_id)::text AS display_name,
  aa.external_id,
  COALESCE(aa.status, '')::text AS status
FROM app_assets aa
JOIN configured_sources cs
  ON cs.source_kind = aa.source_kind
 AND cs.source_name = aa.source_name
WHERE aa.expired_at IS NULL
  AND aa.last_observed_run_id IS NOT NULL
  AND (
    aa.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR aa.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR aa.parent_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
  AND NOT (
    sqlc.arg(exclude_google_workspace_oauth_client)::bool
    AND aa.source_kind = 'google_workspace'
    AND aa.asset_kind = 'google_oauth_client'
  )
ORDER BY
  CASE
    WHEN lower(COALESCE(NULLIF(trim(aa.display_name), ''), aa.external_id)) = lower(trim(sqlc.arg(query)::text))
      OR lower(aa.external_id) = lower(trim(sqlc.arg(query)::text))
      OR lower(COALESCE(aa.parent_external_id, '')) = lower(trim(sqlc.arg(query)::text))
    THEN 0
    WHEN lower(COALESCE(NULLIF(trim(aa.display_name), ''), aa.external_id)) LIKE lower(trim(sqlc.arg(query)::text)) || '%'
      OR lower(aa.external_id) LIKE lower(trim(sqlc.arg(query)::text)) || '%'
      OR lower(COALESCE(aa.parent_external_id, '')) LIKE lower(trim(sqlc.arg(query)::text)) || '%'
    THEN 1
    ELSE 2
  END ASC,
  lower(COALESCE(NULLIF(trim(aa.display_name), ''), aa.external_id)) ASC,
  aa.id ASC
LIMIT sqlc.arg(limit_rows)::int;

-- name: SearchDiscoveryAppsForCommand :many
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
),
scoped_app_ids AS (
  SELECT DISTINCT sas.saas_app_id
  FROM saas_app_sources sas
  JOIN configured_sources cs
    ON lower(trim(cs.source_kind)) = lower(trim(sas.source_kind))
   AND lower(trim(cs.source_name)) = lower(trim(sas.source_name))
  WHERE sas.expired_at IS NULL
    AND sas.last_observed_run_id IS NOT NULL
),
posture_rows AS (
  SELECT *
  FROM saas_app_posture_rows(
    sqlc.arg(okta_fresh_after)::timestamptz,
    sqlc.arg(entra_fresh_after)::timestamptz,
    sqlc.arg(google_workspace_fresh_after)::timestamptz,
    sqlc.arg(github_fresh_after)::timestamptz,
    sqlc.arg(datadog_fresh_after)::timestamptz,
    sqlc.arg(aws_fresh_after)::timestamptz,
    sqlc.arg(default_fresh_after)::timestamptz
  ) AS pr
)
SELECT
  pr.id::bigint AS id,
  COALESCE(NULLIF(trim(pr.display_name), ''), pr.canonical_key)::text AS display_name,
  COALESCE(pr.primary_domain, '')::text AS primary_domain,
  COALESCE(pr.vendor_name, '')::text AS vendor_name,
  pr.managed_state::text AS managed_state,
  pr.risk_level::text AS risk_level,
  pr.risk_score::int AS risk_score,
  pr.last_seen_at::timestamptz AS last_seen_at
FROM posture_rows pr
JOIN scoped_app_ids sai ON sai.saas_app_id = pr.id
WHERE (
    pr.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR pr.primary_domain ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR pr.vendor_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR pr.canonical_key ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
ORDER BY
  CASE
    WHEN lower(COALESCE(NULLIF(trim(pr.display_name), ''), pr.canonical_key)) = lower(trim(sqlc.arg(query)::text))
      OR lower(COALESCE(pr.primary_domain, '')) = lower(trim(sqlc.arg(query)::text))
      OR lower(COALESCE(pr.vendor_name, '')) = lower(trim(sqlc.arg(query)::text))
      OR lower(pr.canonical_key) = lower(trim(sqlc.arg(query)::text))
    THEN 0
    WHEN lower(COALESCE(NULLIF(trim(pr.display_name), ''), pr.canonical_key)) LIKE lower(trim(sqlc.arg(query)::text)) || '%'
      OR lower(COALESCE(pr.primary_domain, '')) LIKE lower(trim(sqlc.arg(query)::text)) || '%'
      OR lower(COALESCE(pr.vendor_name, '')) LIKE lower(trim(sqlc.arg(query)::text)) || '%'
      OR lower(pr.canonical_key) LIKE lower(trim(sqlc.arg(query)::text)) || '%'
    THEN 1
    ELSE 2
  END ASC,
  pr.risk_score DESC,
  pr.last_seen_at DESC,
  lower(COALESCE(NULLIF(trim(pr.display_name), ''), pr.canonical_key)) ASC,
  pr.id ASC
LIMIT sqlc.arg(limit_rows)::int;

-- name: SearchOktaAppsForCommand :many
SELECT
  oa.external_id,
  COALESCE(NULLIF(trim(oa.label), ''), oa.external_id)::text AS label,
  COALESCE(oa.name, '')::text AS name,
  COALESCE(oa.status, '')::text AS status,
  COALESCE(m.integration_kind, '')::text AS integration_kind
FROM okta_apps oa
LEFT JOIN integration_okta_app_map m ON m.okta_app_external_id = oa.external_id
WHERE oa.expired_at IS NULL
  AND oa.last_observed_run_id IS NOT NULL
  AND (
    oa.label ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR oa.name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR oa.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
ORDER BY
  CASE
    WHEN lower(COALESCE(NULLIF(trim(oa.label), ''), oa.external_id)) = lower(trim(sqlc.arg(query)::text))
      OR lower(COALESCE(oa.name, '')) = lower(trim(sqlc.arg(query)::text))
      OR lower(oa.external_id) = lower(trim(sqlc.arg(query)::text))
    THEN 0
    WHEN lower(COALESCE(NULLIF(trim(oa.label), ''), oa.external_id)) LIKE lower(trim(sqlc.arg(query)::text)) || '%'
      OR lower(COALESCE(oa.name, '')) LIKE lower(trim(sqlc.arg(query)::text)) || '%'
      OR lower(oa.external_id) LIKE lower(trim(sqlc.arg(query)::text)) || '%'
    THEN 1
    ELSE 2
  END ASC,
  (NULLIF(trim(m.integration_kind), '') IS NULL) ASC,
  lower(COALESCE(NULLIF(trim(oa.label), ''), NULLIF(trim(oa.name), ''), oa.external_id)) ASC,
  lower(COALESCE(NULLIF(trim(oa.name), ''), oa.external_id)) ASC,
  oa.external_id ASC
LIMIT sqlc.arg(limit_rows)::int;

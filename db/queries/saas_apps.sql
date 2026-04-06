-- name: UpsertSaaSAppsBulk :execrows
WITH input AS (
  SELECT
    i,
    (sqlc.arg(canonical_keys)::text[])[i] AS canonical_key,
    (sqlc.arg(display_names)::text[])[i] AS display_name,
    (sqlc.arg(primary_domains)::text[])[i] AS primary_domain,
    (sqlc.arg(vendor_names)::text[])[i] AS vendor_name,
    (sqlc.arg(first_seen_ats)::timestamptz[])[i] AS first_seen_at,
    (sqlc.arg(last_seen_ats)::timestamptz[])[i] AS last_seen_at
  FROM generate_subscripts(sqlc.arg(canonical_keys)::text[], 1) AS s(i)
),
dedup AS (
  SELECT DISTINCT ON (canonical_key)
    canonical_key,
    display_name,
    primary_domain,
    vendor_name,
    first_seen_at,
    last_seen_at
  FROM input
  ORDER BY canonical_key, i DESC
)
INSERT INTO saas_apps (
  canonical_key,
  display_name,
  primary_domain,
  vendor_name,
  first_seen_at,
  last_seen_at,
  updated_at
)
SELECT
  d.canonical_key,
  d.display_name,
  d.primary_domain,
  d.vendor_name,
  COALESCE(d.first_seen_at, now()),
  COALESCE(d.last_seen_at, now()),
  now()
FROM dedup d
ON CONFLICT (canonical_key) DO UPDATE SET
  display_name = CASE
    WHEN trim(EXCLUDED.display_name) <> '' THEN EXCLUDED.display_name
    ELSE saas_apps.display_name
  END,
  primary_domain = CASE
    WHEN trim(EXCLUDED.primary_domain) <> '' THEN EXCLUDED.primary_domain
    ELSE saas_apps.primary_domain
  END,
  vendor_name = CASE
    WHEN trim(EXCLUDED.vendor_name) <> '' THEN EXCLUDED.vendor_name
    ELSE saas_apps.vendor_name
  END,
  first_seen_at = LEAST(saas_apps.first_seen_at, COALESCE(EXCLUDED.first_seen_at, saas_apps.first_seen_at)),
  last_seen_at = GREATEST(saas_apps.last_seen_at, COALESCE(EXCLUDED.last_seen_at, saas_apps.last_seen_at)),
  updated_at = now();

-- name: CountSaaSAppsByFilters :one
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
  JOIN configured_sources cfg
    ON lower(trim(cfg.source_kind)) = lower(trim(sas.source_kind))
   AND lower(trim(cfg.source_name)) = lower(trim(sas.source_name))
  WHERE sas.expired_at IS NULL
    AND sas.last_observed_run_id IS NOT NULL
    AND (
      sqlc.arg(source_kind)::text = ''
      OR lower(trim(sas.source_kind)) = lower(trim(sqlc.arg(source_kind)::text))
    )
    AND (
      sqlc.arg(source_name)::text = ''
      OR lower(trim(sas.source_name)) = lower(trim(sqlc.arg(source_name)::text))
    )
)
SELECT count(*)
FROM saas_app_posture_rows(
  sqlc.arg(okta_fresh_after)::timestamptz,
  sqlc.arg(entra_fresh_after)::timestamptz,
  sqlc.arg(google_workspace_fresh_after)::timestamptz,
  sqlc.arg(github_fresh_after)::timestamptz,
  sqlc.arg(datadog_fresh_after)::timestamptz,
  sqlc.arg(aws_fresh_after)::timestamptz,
  sqlc.arg(default_fresh_after)::timestamptz
) AS pr
JOIN scoped_app_ids sai ON sai.saas_app_id = pr.id
WHERE (
    sqlc.arg(query)::text = ''
    OR pr.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR pr.primary_domain ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR pr.vendor_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR pr.canonical_key ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
  AND (
    sqlc.arg(managed_state)::text = ''
    OR pr.managed_state = sqlc.arg(managed_state)::text
  )
  AND (
    sqlc.arg(risk_level)::text = ''
    OR pr.risk_level = sqlc.arg(risk_level)::text
  );

-- name: ListSaaSAppsPageByFilters :many
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
  JOIN configured_sources cfg
    ON lower(trim(cfg.source_kind)) = lower(trim(sas.source_kind))
   AND lower(trim(cfg.source_name)) = lower(trim(sas.source_name))
  WHERE sas.expired_at IS NULL
    AND sas.last_observed_run_id IS NOT NULL
    AND (
      sqlc.arg(source_kind)::text = ''
      OR lower(trim(sas.source_kind)) = lower(trim(sqlc.arg(source_kind)::text))
    )
    AND (
      sqlc.arg(source_name)::text = ''
      OR lower(trim(sas.source_name)) = lower(trim(sqlc.arg(source_name)::text))
    )
)
SELECT
  pr.id::bigint AS id,
  pr.canonical_key::text AS canonical_key,
  pr.display_name::text AS display_name,
  pr.primary_domain::text AS primary_domain,
  pr.vendor_name::text AS vendor_name,
  pr.managed_state::text AS managed_state,
  pr.managed_reason::text AS managed_reason,
  pr.bound_connector_kind::text AS bound_connector_kind,
  pr.bound_connector_source_name::text AS bound_connector_source_name,
  pr.risk_score::int AS risk_score,
  pr.risk_level::text AS risk_level,
  pr.suggested_business_criticality::text AS suggested_business_criticality,
  pr.suggested_data_classification::text AS suggested_data_classification,
  pr.first_seen_at::timestamptz AS first_seen_at,
  pr.last_seen_at::timestamptz AS last_seen_at,
  pr.created_at::timestamptz AS created_at,
  pr.updated_at::timestamptz AS updated_at,
  COALESCE(owner.display_name, '') AS owner_display_name,
  COALESCE(owner.primary_email, '') AS owner_primary_email,
  pr.actors_30d::bigint AS actors_30d
FROM saas_app_posture_rows(
  sqlc.arg(okta_fresh_after)::timestamptz,
  sqlc.arg(entra_fresh_after)::timestamptz,
  sqlc.arg(google_workspace_fresh_after)::timestamptz,
  sqlc.arg(github_fresh_after)::timestamptz,
  sqlc.arg(datadog_fresh_after)::timestamptz,
  sqlc.arg(aws_fresh_after)::timestamptz,
  sqlc.arg(default_fresh_after)::timestamptz
) AS pr
JOIN scoped_app_ids sai ON sai.saas_app_id = pr.id
LEFT JOIN identities owner ON owner.id = NULLIF(pr.owner_identity_id, 0)
WHERE (
    sqlc.arg(query)::text = ''
    OR pr.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR pr.primary_domain ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR pr.vendor_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR pr.canonical_key ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
  AND (
    sqlc.arg(managed_state)::text = ''
    OR pr.managed_state = sqlc.arg(managed_state)::text
  )
  AND (
    sqlc.arg(risk_level)::text = ''
    OR pr.risk_level = sqlc.arg(risk_level)::text
  )
ORDER BY
  pr.risk_score DESC,
  pr.last_seen_at DESC,
  lower(COALESCE(NULLIF(trim(pr.display_name), ''), pr.canonical_key)) ASC,
  pr.id ASC
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: GetSaaSAppByID :one
SELECT
  pr.id::bigint AS id,
  pr.canonical_key::text AS canonical_key,
  pr.display_name::text AS display_name,
  pr.primary_domain::text AS primary_domain,
  pr.vendor_name::text AS vendor_name,
  pr.managed_state::text AS managed_state,
  pr.managed_reason::text AS managed_reason,
  pr.bound_connector_kind::text AS bound_connector_kind,
  pr.bound_connector_source_name::text AS bound_connector_source_name,
  pr.risk_score::int AS risk_score,
  pr.risk_level::text AS risk_level,
  pr.suggested_business_criticality::text AS suggested_business_criticality,
  pr.suggested_data_classification::text AS suggested_data_classification,
  pr.first_seen_at::timestamptz AS first_seen_at,
  pr.last_seen_at::timestamptz AS last_seen_at,
  pr.created_at::timestamptz AS created_at,
  pr.updated_at::timestamptz AS updated_at
FROM saas_app_posture_rows(
  sqlc.arg(okta_fresh_after)::timestamptz,
  sqlc.arg(entra_fresh_after)::timestamptz,
  sqlc.arg(google_workspace_fresh_after)::timestamptz,
  sqlc.arg(github_fresh_after)::timestamptz,
  sqlc.arg(datadog_fresh_after)::timestamptz,
  sqlc.arg(aws_fresh_after)::timestamptz,
  sqlc.arg(default_fresh_after)::timestamptz
) AS pr
WHERE pr.id = sqlc.arg(id)::bigint;

-- name: ListSaaSAppHotspots :many
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
  JOIN configured_sources cfg
    ON lower(trim(cfg.source_kind)) = lower(trim(sas.source_kind))
   AND lower(trim(cfg.source_name)) = lower(trim(sas.source_name))
  WHERE sas.expired_at IS NULL
    AND sas.last_observed_run_id IS NOT NULL
    AND (
      sqlc.arg(source_kind)::text = ''
      OR lower(trim(sas.source_kind)) = lower(trim(sqlc.arg(source_kind)::text))
    )
    AND (
      sqlc.arg(source_name)::text = ''
      OR lower(trim(sas.source_name)) = lower(trim(sqlc.arg(source_name)::text))
    )
)
SELECT
  pr.id::bigint AS id,
  pr.canonical_key::text AS canonical_key,
  pr.display_name::text AS display_name,
  pr.primary_domain::text AS primary_domain,
  pr.vendor_name::text AS vendor_name,
  pr.managed_state::text AS managed_state,
  pr.managed_reason::text AS managed_reason,
  pr.bound_connector_kind::text AS bound_connector_kind,
  pr.bound_connector_source_name::text AS bound_connector_source_name,
  pr.risk_score::int AS risk_score,
  pr.risk_level::text AS risk_level,
  pr.suggested_business_criticality::text AS suggested_business_criticality,
  pr.suggested_data_classification::text AS suggested_data_classification,
  pr.first_seen_at::timestamptz AS first_seen_at,
  pr.last_seen_at::timestamptz AS last_seen_at,
  pr.created_at::timestamptz AS created_at,
  pr.updated_at::timestamptz AS updated_at,
  COALESCE(owner.display_name, '') AS owner_display_name,
  COALESCE(owner.primary_email, '') AS owner_primary_email,
  pr.actors_30d::bigint AS actors_30d
FROM saas_app_posture_rows(
  sqlc.arg(okta_fresh_after)::timestamptz,
  sqlc.arg(entra_fresh_after)::timestamptz,
  sqlc.arg(google_workspace_fresh_after)::timestamptz,
  sqlc.arg(github_fresh_after)::timestamptz,
  sqlc.arg(datadog_fresh_after)::timestamptz,
  sqlc.arg(aws_fresh_after)::timestamptz,
  sqlc.arg(default_fresh_after)::timestamptz
) AS pr
JOIN scoped_app_ids sai ON sai.saas_app_id = pr.id
LEFT JOIN identities owner ON owner.id = NULLIF(pr.owner_identity_id, 0)
WHERE pr.risk_score >= 60
ORDER BY pr.risk_score DESC, pr.last_seen_at DESC, pr.id ASC
LIMIT sqlc.arg(limit_rows)::int;

-- name: CountSaaSAppsGroupedByManagedState :many
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
  JOIN configured_sources cfg
    ON lower(trim(cfg.source_kind)) = lower(trim(sas.source_kind))
   AND lower(trim(cfg.source_name)) = lower(trim(sas.source_name))
  WHERE sas.expired_at IS NULL
    AND sas.last_observed_run_id IS NOT NULL
)
SELECT pr.managed_state::text AS managed_state, count(*) AS app_count
FROM saas_app_posture_rows(
  sqlc.arg(okta_fresh_after)::timestamptz,
  sqlc.arg(entra_fresh_after)::timestamptz,
  sqlc.arg(google_workspace_fresh_after)::timestamptz,
  sqlc.arg(github_fresh_after)::timestamptz,
  sqlc.arg(datadog_fresh_after)::timestamptz,
  sqlc.arg(aws_fresh_after)::timestamptz,
  sqlc.arg(default_fresh_after)::timestamptz
) AS pr
JOIN scoped_app_ids sai ON sai.saas_app_id = pr.id
GROUP BY 1
ORDER BY 1;

-- name: CountSaaSAppsGroupedByRiskLevel :many
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
  JOIN configured_sources cfg
    ON lower(trim(cfg.source_kind)) = lower(trim(sas.source_kind))
   AND lower(trim(cfg.source_name)) = lower(trim(sas.source_name))
  WHERE sas.expired_at IS NULL
    AND sas.last_observed_run_id IS NOT NULL
)
SELECT pr.risk_level::text AS risk_level, count(*) AS app_count
FROM saas_app_posture_rows(
  sqlc.arg(okta_fresh_after)::timestamptz,
  sqlc.arg(entra_fresh_after)::timestamptz,
  sqlc.arg(google_workspace_fresh_after)::timestamptz,
  sqlc.arg(github_fresh_after)::timestamptz,
  sqlc.arg(datadog_fresh_after)::timestamptz,
  sqlc.arg(aws_fresh_after)::timestamptz,
  sqlc.arg(default_fresh_after)::timestamptz
) AS pr
JOIN scoped_app_ids sai ON sai.saas_app_id = pr.id
GROUP BY 1
ORDER BY 1;

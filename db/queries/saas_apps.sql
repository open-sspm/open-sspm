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

-- name: ListSaaSAppsByCanonicalKeys :many
SELECT *
FROM saas_apps
WHERE canonical_key = ANY(sqlc.arg(canonical_keys)::text[])
ORDER BY id ASC;

-- name: CountSaaSAppsByFilters :one
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
)
SELECT count(*)
FROM saas_apps sa
WHERE EXISTS (
  SELECT 1
  FROM saas_app_sources sas
  JOIN configured_sources cs
    ON cs.source_kind = sas.source_kind
   AND cs.source_name = sas.source_name
  WHERE sas.saas_app_id = sa.id
    AND sas.expired_at IS NULL
    AND sas.last_observed_run_id IS NOT NULL
    AND (
      sqlc.arg(source_kind)::text = ''
      OR sas.source_kind = sqlc.arg(source_kind)::text
    )
    AND (
      sqlc.arg(source_name)::text = ''
      OR sas.source_name = sqlc.arg(source_name)::text
    )
)
  AND (
    sqlc.arg(managed_state)::text = ''
    OR sa.managed_state = sqlc.arg(managed_state)::text
  )
  AND (
    sqlc.arg(risk_level)::text = ''
    OR sa.risk_level = sqlc.arg(risk_level)::text
  )
  AND (
    sqlc.arg(query)::text = ''
    OR sa.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR sa.primary_domain ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR sa.vendor_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR sa.canonical_key ILIKE ('%' || sqlc.arg(query)::text || '%')
  );

-- name: ListSaaSAppsPageByFilters :many
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
)
SELECT
  sa.*,
  go.owner_identity_id,
  COALESCE(owner.display_name, '') AS owner_display_name,
  COALESCE(owner.primary_email, '') AS owner_primary_email,
  COALESCE(actor_stats.actors_30d, 0)::bigint AS actors_30d
FROM saas_apps sa
LEFT JOIN saas_app_governance_overrides go ON go.saas_app_id = sa.id
LEFT JOIN identities owner ON owner.id = go.owner_identity_id
LEFT JOIN LATERAL (
  SELECT
    count(DISTINCT COALESCE(NULLIF(trim(e.actor_external_id), ''), NULLIF(lower(trim(e.actor_email)), ''))) AS actors_30d
  FROM saas_app_events e
  WHERE e.saas_app_id = sa.id
    AND e.expired_at IS NULL
    AND e.last_observed_run_id IS NOT NULL
    AND e.observed_at >= now() - interval '30 days'
) actor_stats ON TRUE
WHERE EXISTS (
  SELECT 1
  FROM saas_app_sources sas
  JOIN configured_sources cs
    ON cs.source_kind = sas.source_kind
   AND cs.source_name = sas.source_name
  WHERE sas.saas_app_id = sa.id
    AND sas.expired_at IS NULL
    AND sas.last_observed_run_id IS NOT NULL
    AND (
      sqlc.arg(source_kind)::text = ''
      OR sas.source_kind = sqlc.arg(source_kind)::text
    )
    AND (
      sqlc.arg(source_name)::text = ''
      OR sas.source_name = sqlc.arg(source_name)::text
    )
)
  AND (
    sqlc.arg(managed_state)::text = ''
    OR sa.managed_state = sqlc.arg(managed_state)::text
  )
  AND (
    sqlc.arg(risk_level)::text = ''
    OR sa.risk_level = sqlc.arg(risk_level)::text
  )
  AND (
    sqlc.arg(query)::text = ''
    OR sa.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR sa.primary_domain ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR sa.vendor_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR sa.canonical_key ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
ORDER BY
  sa.risk_score DESC,
  sa.last_seen_at DESC,
  lower(COALESCE(NULLIF(trim(sa.display_name), ''), sa.canonical_key)) ASC,
  sa.id ASC
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: GetSaaSAppByID :one
SELECT *
FROM saas_apps
WHERE id = $1;

-- name: ListSaaSAppHotspots :many
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
)
SELECT
  sa.*,
  go.owner_identity_id,
  COALESCE(owner.display_name, '') AS owner_display_name,
  COALESCE(owner.primary_email, '') AS owner_primary_email,
  COALESCE(actor_stats.actors_30d, 0)::bigint AS actors_30d
FROM saas_apps sa
LEFT JOIN saas_app_governance_overrides go ON go.saas_app_id = sa.id
LEFT JOIN identities owner ON owner.id = go.owner_identity_id
LEFT JOIN LATERAL (
  SELECT
    count(DISTINCT COALESCE(NULLIF(trim(e.actor_external_id), ''), NULLIF(lower(trim(e.actor_email)), ''))) AS actors_30d
  FROM saas_app_events e
  WHERE e.saas_app_id = sa.id
    AND e.expired_at IS NULL
    AND e.last_observed_run_id IS NOT NULL
    AND e.observed_at >= now() - interval '30 days'
) actor_stats ON TRUE
WHERE sa.risk_score >= 60
  AND EXISTS (
    SELECT 1
    FROM saas_app_sources sas
    JOIN configured_sources cs
      ON cs.source_kind = sas.source_kind
     AND cs.source_name = sas.source_name
    WHERE sas.saas_app_id = sa.id
      AND sas.expired_at IS NULL
      AND sas.last_observed_run_id IS NOT NULL
      AND (
        sqlc.arg(source_kind)::text = ''
        OR sas.source_kind = sqlc.arg(source_kind)::text
      )
      AND (
        sqlc.arg(source_name)::text = ''
        OR sas.source_name = sqlc.arg(source_name)::text
      )
  )
ORDER BY sa.risk_score DESC, sa.last_seen_at DESC, sa.id ASC
LIMIT sqlc.arg(limit_rows)::int;

-- name: CountSaaSAppsGroupedByManagedState :many
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
)
SELECT sa.managed_state, count(*) AS app_count
FROM saas_apps sa
WHERE EXISTS (
  SELECT 1
  FROM saas_app_sources sas
  JOIN configured_sources cs
    ON cs.source_kind = sas.source_kind
   AND cs.source_name = sas.source_name
  WHERE sas.saas_app_id = sa.id
    AND sas.expired_at IS NULL
    AND sas.last_observed_run_id IS NOT NULL
)
GROUP BY sa.managed_state
ORDER BY sa.managed_state;

-- name: CountSaaSAppsGroupedByRiskLevel :many
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
)
SELECT sa.risk_level, count(*) AS app_count
FROM saas_apps sa
WHERE EXISTS (
  SELECT 1
  FROM saas_app_sources sas
  JOIN configured_sources cs
    ON cs.source_kind = sas.source_kind
   AND cs.source_name = sas.source_name
  WHERE sas.saas_app_id = sa.id
    AND sas.expired_at IS NULL
    AND sas.last_observed_run_id IS NOT NULL
)
GROUP BY sa.risk_level
ORDER BY sa.risk_level;

-- name: ListSaaSAppPostureInputs :many
WITH active_events AS (
  SELECT e.*
  FROM saas_app_events e
  WHERE e.expired_at IS NULL
    AND e.last_observed_run_id IS NOT NULL
),
actor_counts AS (
  SELECT
    e.saas_app_id,
    count(DISTINCT COALESCE(NULLIF(trim(e.actor_external_id), ''), NULLIF(lower(trim(e.actor_email)), ''))) FILTER (WHERE e.observed_at >= now() - interval '30 days') AS actors_30d
  FROM active_events e
  GROUP BY e.saas_app_id
),
scope_flags AS (
  SELECT
    e.saas_app_id,
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
  FROM active_events e
  GROUP BY e.saas_app_id
),
primary_binding AS (
  SELECT
    b.saas_app_id,
    b.connector_kind,
    b.connector_source_name,
    b.binding_source,
    b.confidence
  FROM saas_app_bindings b
  WHERE b.is_primary
)
SELECT
  sa.id,
  COALESCE(go.owner_identity_id, 0)::bigint AS owner_identity_id,
  COALESCE(go.business_criticality, 'unknown')::text AS business_criticality,
  COALESCE(go.data_classification, 'unknown')::text AS data_classification,
  COALESCE(ac.actors_30d, 0)::bigint AS actors_30d,
  COALESCE(sf.has_privileged_scope, false)::boolean AS has_privileged_scope,
  COALESCE(sf.has_confidential_scope, false)::boolean AS has_confidential_scope,
  COALESCE(pb.connector_kind, '')::text AS binding_connector_kind,
  COALESCE(pb.connector_source_name, '')::text AS binding_connector_source_name,
  COALESCE(pb.binding_source, '')::text AS binding_source,
  COALESCE(pb.confidence, 0)::real AS binding_confidence
FROM saas_apps sa
LEFT JOIN saas_app_governance_overrides go ON go.saas_app_id = sa.id
LEFT JOIN actor_counts ac ON ac.saas_app_id = sa.id
LEFT JOIN scope_flags sf ON sf.saas_app_id = sa.id
LEFT JOIN primary_binding pb ON pb.saas_app_id = sa.id
WHERE EXISTS (
  SELECT 1
  FROM saas_app_sources sas
  WHERE sas.saas_app_id = sa.id
    AND sas.expired_at IS NULL
    AND sas.last_observed_run_id IS NOT NULL
)
ORDER BY sa.id ASC;

-- name: UpdateSaaSAppPosture :exec
UPDATE saas_apps
SET
  managed_state = sqlc.arg(managed_state)::text,
  managed_reason = sqlc.arg(managed_reason)::text,
  bound_connector_kind = sqlc.arg(bound_connector_kind)::text,
  bound_connector_source_name = sqlc.arg(bound_connector_source_name)::text,
  risk_score = sqlc.arg(risk_score)::int,
  risk_level = sqlc.arg(risk_level)::text,
  suggested_business_criticality = sqlc.arg(suggested_business_criticality)::text,
  suggested_data_classification = sqlc.arg(suggested_data_classification)::text,
  updated_at = now()
WHERE id = sqlc.arg(id)::bigint;

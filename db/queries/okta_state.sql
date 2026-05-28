-- Okta app and group state projections.

-- name: UpsertOktaGroupsBulk :execrows
WITH input AS (
  SELECT
    i,
    (sqlc.arg(external_ids)::text[])[i] AS external_id,
    (sqlc.arg(names)::text[])[i] AS name,
    (sqlc.arg(types)::text[])[i] AS type,
    (sqlc.arg(raw_jsons)::jsonb[])[i] AS raw_json
  FROM generate_subscripts(sqlc.arg(external_ids)::text[], 1) AS s(i)
),
dedup AS (
  SELECT DISTINCT ON (external_id)
    external_id,
    name,
    type,
    raw_json
  FROM input
  ORDER BY external_id, i DESC
)
INSERT INTO okta_groups (
  source_kind,
  source_name,
  external_id,
  name,
  type,
  raw_json,
  seen_in_run_id,
  seen_at,
  updated_at
)
SELECT
  sqlc.arg(source_kind)::text,
  sqlc.arg(source_name)::text,
  input.external_id,
  input.name,
  input.type,
  input.raw_json,
  sqlc.arg(seen_in_run_id)::bigint,
  now(),
  now()
FROM dedup input
ON CONFLICT (source_kind, source_name, external_id) DO UPDATE SET
  name = CASE
    WHEN trim(EXCLUDED.name) <> '' THEN EXCLUDED.name
    ELSE okta_groups.name
  END,
  type = CASE
    WHEN trim(EXCLUDED.type) <> '' THEN EXCLUDED.type
    ELSE okta_groups.type
  END,
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  updated_at = now()
;

-- name: UpsertOktaAppsBulk :execrows
WITH input AS (
  SELECT
    i,
    (sqlc.arg(external_ids)::text[])[i] AS external_id,
    (sqlc.arg(labels)::text[])[i] AS label,
    (sqlc.arg(names)::text[])[i] AS name,
    (sqlc.arg(statuses)::text[])[i] AS status,
    (sqlc.arg(sign_on_modes)::text[])[i] AS sign_on_mode,
    (sqlc.arg(raw_jsons)::jsonb[])[i] AS raw_json
  FROM generate_subscripts(sqlc.arg(external_ids)::text[], 1) AS s(i)
),
dedup AS (
  SELECT DISTINCT ON (external_id)
    external_id,
    label,
    name,
    status,
    sign_on_mode,
    raw_json
  FROM input
  ORDER BY external_id, i DESC
)
INSERT INTO okta_apps (
  source_kind,
  source_name,
  external_id,
  label,
  name,
  status,
  sign_on_mode,
  raw_json,
  seen_in_run_id,
  seen_at,
  updated_at
)
SELECT
  sqlc.arg(source_kind)::text,
  sqlc.arg(source_name)::text,
  input.external_id,
  input.label,
  input.name,
  input.status,
  input.sign_on_mode,
  input.raw_json,
  sqlc.arg(seen_in_run_id)::bigint,
  now(),
  now()
FROM dedup input
ON CONFLICT (source_kind, source_name, external_id) DO UPDATE SET
  label = CASE
    WHEN trim(EXCLUDED.label) <> '' THEN EXCLUDED.label
    ELSE okta_apps.label
  END,
  name = CASE
    WHEN trim(EXCLUDED.name) <> '' THEN EXCLUDED.name
    ELSE okta_apps.name
  END,
  status = CASE
    WHEN trim(EXCLUDED.status) <> '' THEN EXCLUDED.status
    ELSE okta_apps.status
  END,
  sign_on_mode = CASE
    WHEN trim(EXCLUDED.sign_on_mode) <> '' THEN EXCLUDED.sign_on_mode
    ELSE okta_apps.sign_on_mode
  END,
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  updated_at = now()
;

-- name: CountOktaApps :one
SELECT count(*)
FROM okta_apps
WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

-- name: CountOktaAppsFiltered :one
SELECT count(*)
FROM okta_apps oa
LEFT JOIN integration_okta_app_map m
  ON m.okta_source_kind = oa.source_kind
 AND m.okta_source_name = oa.source_name
 AND m.okta_app_external_id = oa.external_id
WHERE
  oa.expired_at IS NULL
  AND oa.last_observed_run_id IS NOT NULL
  AND (
    sqlc.arg(query)::text = ''
    OR oa.label ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR oa.name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR oa.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
  AND (
    sqlc.arg(status_filter)::text = ''
    OR UPPER(TRIM(oa.status)) = UPPER(TRIM(sqlc.arg(status_filter)::text))
  )
  AND (
    sqlc.arg(integration_filter)::text = ''
    OR (sqlc.arg(integration_filter)::text = 'connected' AND m.integration_kind IS NOT NULL)
    OR (sqlc.arg(integration_filter)::text = 'not_connected' AND m.integration_kind IS NULL)
  );

-- name: ListOktaAppsPageFiltered :many
SELECT
  oa.source_name,
  oa.external_id,
  oa.label,
  oa.name,
  oa.status,
  oa.sign_on_mode,
  COALESCE(m.integration_kind, '') AS integration_kind
FROM okta_apps oa
LEFT JOIN integration_okta_app_map m
  ON m.okta_source_kind = oa.source_kind
 AND m.okta_source_name = oa.source_name
 AND m.okta_app_external_id = oa.external_id
WHERE
  oa.expired_at IS NULL
  AND oa.last_observed_run_id IS NOT NULL
  AND (
    sqlc.arg(query)::text = ''
    OR oa.label ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR oa.name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR oa.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
  AND (
    sqlc.arg(status_filter)::text = ''
    OR UPPER(TRIM(oa.status)) = UPPER(TRIM(sqlc.arg(status_filter)::text))
  )
  AND (
    sqlc.arg(integration_filter)::text = ''
    OR (sqlc.arg(integration_filter)::text = 'connected' AND m.integration_kind IS NOT NULL)
    OR (sqlc.arg(integration_filter)::text = 'not_connected' AND m.integration_kind IS NULL)
  )
ORDER BY (m.integration_kind IS NULL), oa.label, oa.name, oa.source_name, oa.external_id
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: ListDistinctOktaAppStatuses :many
SELECT DISTINCT UPPER(TRIM(oa.status))::text AS status
FROM okta_apps oa
WHERE oa.expired_at IS NULL
  AND oa.last_observed_run_id IS NOT NULL
  AND TRIM(oa.status) != ''
ORDER BY status;

-- name: GetOktaAppBySourceAndExternalIDWithIntegration :one
SELECT
  oa.id,
  oa.source_name,
  oa.external_id,
  oa.label,
  oa.name,
  oa.status,
  oa.sign_on_mode,
  oa.raw_json,
  oa.created_at,
  oa.updated_at,
  COALESCE(m.integration_kind, '') AS integration_kind
FROM okta_apps oa
LEFT JOIN integration_okta_app_map m
  ON m.okta_source_kind = oa.source_kind
 AND m.okta_source_name = oa.source_name
 AND m.okta_app_external_id = oa.external_id
WHERE oa.source_kind = 'okta'
  AND oa.source_name = sqlc.arg(source_name)::text
  AND oa.external_id = sqlc.arg(external_id)::text
  AND oa.expired_at IS NULL
  AND oa.last_observed_run_id IS NOT NULL;

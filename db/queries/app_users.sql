-- name: UpsertAppUser :one
INSERT INTO app_users (source_kind, source_name, external_id, email, display_name, raw_json, last_login_at, last_login_ip, last_login_region, seen_in_run_id, seen_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now(), now())
ON CONFLICT (source_kind, source_name, external_id) DO UPDATE SET
  email = EXCLUDED.email,
  display_name = EXCLUDED.display_name,
  raw_json = EXCLUDED.raw_json,
  last_login_at = COALESCE(EXCLUDED.last_login_at, app_users.last_login_at),
  last_login_ip = CASE WHEN EXCLUDED.last_login_ip <> '' THEN EXCLUDED.last_login_ip ELSE app_users.last_login_ip END,
  last_login_region = CASE WHEN EXCLUDED.last_login_region <> '' THEN EXCLUDED.last_login_region ELSE app_users.last_login_region END,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  updated_at = now()
RETURNING *;

-- name: UpsertAppUsersBulkBySource :execrows
WITH input AS (
  SELECT
    i,
    (sqlc.arg(external_ids)::text[])[i] AS external_id,
    (sqlc.arg(emails)::text[])[i] AS email,
    (sqlc.arg(display_names)::text[])[i] AS display_name,
    (sqlc.arg(raw_jsons)::jsonb[])[i] AS raw_json,
    (sqlc.arg(last_login_ats)::timestamptz[])[i] AS last_login_at,
    (sqlc.arg(last_login_ips)::text[])[i] AS last_login_ip,
    (sqlc.arg(last_login_regions)::text[])[i] AS last_login_region
  FROM generate_subscripts(sqlc.arg(external_ids)::text[], 1) AS s(i)
),
dedup AS (
  SELECT DISTINCT ON (external_id)
    external_id,
    email,
    display_name,
    raw_json,
    last_login_at,
    last_login_ip,
    last_login_region
  FROM input
  ORDER BY external_id, i DESC
)
INSERT INTO app_users (
  source_kind,
  source_name,
  external_id,
  email,
  display_name,
  raw_json,
  last_login_at,
  last_login_ip,
  last_login_region,
  seen_in_run_id,
  seen_at,
  updated_at
)
SELECT
  sqlc.arg(source_kind)::text,
  sqlc.arg(source_name)::text,
  input.external_id,
  input.email,
  input.display_name,
  input.raw_json,
  input.last_login_at,
  input.last_login_ip,
  input.last_login_region,
  sqlc.arg(seen_in_run_id)::bigint,
  now(),
  now()
FROM dedup input
ON CONFLICT (source_kind, source_name, external_id) DO UPDATE SET
  email = EXCLUDED.email,
  display_name = EXCLUDED.display_name,
  raw_json = EXCLUDED.raw_json,
  last_login_at = COALESCE(EXCLUDED.last_login_at, app_users.last_login_at),
  last_login_ip = CASE WHEN EXCLUDED.last_login_ip <> '' THEN EXCLUDED.last_login_ip ELSE app_users.last_login_ip END,
  last_login_region = CASE WHEN EXCLUDED.last_login_region <> '' THEN EXCLUDED.last_login_region ELSE app_users.last_login_region END,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  updated_at = now()
;

-- name: ListAppUsersBySource :many
SELECT *
FROM app_users
WHERE source_kind = $1
  AND source_name = $2
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
ORDER BY id DESC;

-- name: ListAppUsersWithLinkBySource :many
SELECT
  au.*,
  COALESCE(il.idp_user_id, 0) AS idp_user_id
FROM app_users au
LEFT JOIN identity_links il ON il.app_user_id = au.id
WHERE au.source_kind = $1 AND au.source_name = $2
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
ORDER BY au.id DESC;

-- name: CountAppUsersWithLinkBySourceAndQuery :one
SELECT count(*)
FROM app_users au
WHERE
  au.source_kind = sqlc.arg(source_kind)
  AND au.source_name = sqlc.arg(source_name)
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
  AND (
    sqlc.arg(query)::text = ''
    OR au.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.email ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
  );

-- name: ListAppUsersWithLinkPageBySourceAndQuery :many
SELECT
  au.*,
  COALESCE(il.idp_user_id, 0) AS idp_user_id
FROM app_users au
LEFT JOIN identity_links il ON il.app_user_id = au.id
WHERE
  au.source_kind = sqlc.arg(source_kind)
  AND au.source_name = sqlc.arg(source_name)
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
  AND (
    sqlc.arg(query)::text = ''
    OR au.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.email ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
ORDER BY au.id DESC
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: CountAppUsersBySourceAndQueryAndState :one
SELECT count(*)
FROM app_users au
WHERE
  au.source_kind = sqlc.arg(source_kind)
  AND au.source_name = sqlc.arg(source_name)
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
  AND (
    sqlc.arg(query)::text = ''
    OR au.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.email ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
  AND (
    sqlc.arg(state)::text = ''
    OR (sqlc.arg(state)::text = 'active' AND lower(COALESCE(NULLIF(trim(au.raw_json->>'status'), ''), '')) = 'active')
    OR (sqlc.arg(state)::text = 'inactive' AND lower(COALESCE(NULLIF(trim(au.raw_json->>'status'), ''), '')) <> 'active')
  );

-- name: ListAppUsersPageBySourceAndQueryAndState :many
SELECT au.*
FROM app_users au
WHERE
  au.source_kind = sqlc.arg(source_kind)
  AND au.source_name = sqlc.arg(source_name)
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
  AND (
    sqlc.arg(query)::text = ''
    OR au.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.email ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
  AND (
    sqlc.arg(state)::text = ''
    OR (sqlc.arg(state)::text = 'active' AND lower(COALESCE(NULLIF(trim(au.raw_json->>'status'), ''), '')) = 'active')
    OR (sqlc.arg(state)::text = 'inactive' AND lower(COALESCE(NULLIF(trim(au.raw_json->>'status'), ''), '')) <> 'active')
  )
ORDER BY au.id DESC
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: ListUnmatchedAppUsersBySource :many
SELECT au.*
FROM app_users au
LEFT JOIN identity_links il ON il.app_user_id = au.id
WHERE au.source_kind = $1
  AND au.source_name = $2
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
  AND il.id IS NULL
ORDER BY au.display_name, au.email, au.external_id;

-- name: CountUnmatchedAppUsersBySourceAndQuery :one
SELECT count(*)
FROM app_users au
LEFT JOIN identity_links il ON il.app_user_id = au.id
WHERE
  au.source_kind = sqlc.arg(source_kind)
  AND au.source_name = sqlc.arg(source_name)
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
  AND il.id IS NULL
  AND (
    sqlc.arg(query)::text = ''
    OR au.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.email ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
  );

-- name: ListUnmatchedAppUsersPageBySourceAndQuery :many
SELECT au.*
FROM app_users au
LEFT JOIN identity_links il ON il.app_user_id = au.id
WHERE
  au.source_kind = sqlc.arg(source_kind)
  AND au.source_name = sqlc.arg(source_name)
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
  AND il.id IS NULL
  AND (
    sqlc.arg(query)::text = ''
    OR au.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.email ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
ORDER BY au.display_name, au.email, au.external_id
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: ListAppUsersWithoutLinkWithEmailBySource :many
SELECT au.*
FROM app_users au
LEFT JOIN identity_links il ON il.app_user_id = au.id
WHERE au.source_kind = $1
  AND au.source_name = $2
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
  AND il.id IS NULL
  AND au.email <> ''
ORDER BY au.id;

-- name: GetAppUser :one
SELECT *
FROM app_users
WHERE id = $1 AND expired_at IS NULL AND last_observed_run_id IS NOT NULL;

-- name: CountAppUsersBySource :one
SELECT count(*)
FROM app_users
WHERE source_kind = $1
  AND source_name = $2
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL;

-- name: CountMatchedAppUsersBySource :one
SELECT count(*)
FROM app_users au
JOIN identity_links il ON il.app_user_id = au.id
WHERE au.source_kind = $1
  AND au.source_name = $2
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL;

-- name: CountUnmatchedAppUsersBySource :one
SELECT count(*)
FROM app_users au
LEFT JOIN identity_links il ON il.app_user_id = au.id
WHERE au.source_kind = $1
  AND au.source_name = $2
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
  AND il.id IS NULL;

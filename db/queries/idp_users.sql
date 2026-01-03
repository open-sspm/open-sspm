-- name: UpsertIdPUser :one
INSERT INTO idp_users (external_id, email, display_name, status, raw_json, last_login_at, last_login_ip, last_login_region, seen_in_run_id, seen_at, updated_at)
VALUES (
  sqlc.arg(external_id)::text,
  lower(trim(sqlc.arg(email)::text)),
  sqlc.arg(display_name)::text,
  sqlc.arg(status)::text,
  sqlc.arg(raw_json)::jsonb,
  sqlc.arg(last_login_at)::timestamptz,
  sqlc.arg(last_login_ip)::text,
  sqlc.arg(last_login_region)::text,
  sqlc.arg(seen_in_run_id)::bigint,
  now(),
  now()
)
ON CONFLICT (external_id) DO UPDATE SET
  email = EXCLUDED.email,
  display_name = EXCLUDED.display_name,
  status = EXCLUDED.status,
  raw_json = EXCLUDED.raw_json,
  last_login_at = COALESCE(EXCLUDED.last_login_at, idp_users.last_login_at),
  last_login_ip = COALESCE(NULLIF(EXCLUDED.last_login_ip, ''), idp_users.last_login_ip),
  last_login_region = COALESCE(NULLIF(EXCLUDED.last_login_region, ''), idp_users.last_login_region),
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  updated_at = now()
RETURNING *;

-- name: UpsertIdPUsersBulk :execrows
WITH input AS (
  SELECT
    i,
    (sqlc.arg(external_ids)::text[])[i] AS external_id,
    lower(trim((sqlc.arg(emails)::text[])[i])) AS email,
    (sqlc.arg(display_names)::text[])[i] AS display_name,
    (sqlc.arg(statuses)::text[])[i] AS status,
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
    status,
    raw_json,
    last_login_at,
    last_login_ip,
    last_login_region
  FROM input
  ORDER BY external_id, i DESC
)
INSERT INTO idp_users (
  external_id,
  email,
  display_name,
  status,
  raw_json,
  last_login_at,
  last_login_ip,
  last_login_region,
  seen_in_run_id,
  seen_at,
  updated_at
)
SELECT
  input.external_id,
  input.email,
  input.display_name,
  input.status,
  input.raw_json,
  input.last_login_at,
  input.last_login_ip,
  input.last_login_region,
  sqlc.arg(seen_in_run_id)::bigint,
  now(),
  now()
FROM dedup input
ON CONFLICT (external_id) DO UPDATE SET
  email = EXCLUDED.email,
  display_name = EXCLUDED.display_name,
  status = EXCLUDED.status,
  raw_json = EXCLUDED.raw_json,
  last_login_at = COALESCE(EXCLUDED.last_login_at, idp_users.last_login_at),
  last_login_ip = COALESCE(NULLIF(EXCLUDED.last_login_ip, ''), idp_users.last_login_ip),
  last_login_region = COALESCE(NULLIF(EXCLUDED.last_login_region, ''), idp_users.last_login_region),
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  updated_at = now()
;

-- name: ListIdPUsers :many
SELECT *
FROM idp_users
WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL
ORDER BY id ASC;

-- name: ListIdPUsersByStatus :many
SELECT *
FROM idp_users
WHERE
  expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
  AND (
  (COALESCE($1::text, '') = '')
  OR ($1::text = 'active' AND status = 'ACTIVE')
  OR ($1::text = 'inactive' AND status <> 'ACTIVE')
  )
ORDER BY id ASC;

-- name: GetIdPUser :one
SELECT *
FROM idp_users
WHERE id = $1 AND expired_at IS NULL AND last_observed_run_id IS NOT NULL;

-- name: FindIdPUserByEmail :one
SELECT *
FROM idp_users
WHERE email = $1 AND expired_at IS NULL AND last_observed_run_id IS NOT NULL
LIMIT 1;

-- name: CountIdPUsers :one
SELECT count(*)
FROM idp_users
WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

-- name: CountIdPUsersByQueryAndState :one
SELECT count(*)
FROM idp_users
WHERE
  expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
  AND (
    sqlc.arg(query)::text = ''
    OR email ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
  AND (
    sqlc.arg(state)::text = ''
    OR (sqlc.arg(state)::text = 'active' AND status = 'ACTIVE')
    OR (sqlc.arg(state)::text = 'inactive' AND status <> 'ACTIVE')
  );

-- name: ListIdPUsersPageByQueryAndState :many
SELECT *
FROM idp_users
WHERE
  expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
  AND (
    sqlc.arg(query)::text = ''
    OR email ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
  AND (
    sqlc.arg(state)::text = ''
    OR (sqlc.arg(state)::text = 'active' AND status = 'ACTIVE')
    OR (sqlc.arg(state)::text = 'inactive' AND status <> 'ACTIVE')
  )
ORDER BY id ASC
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: ListIdPUsersForCommand :many
SELECT
  id,
  email,
  display_name,
  status
FROM idp_users
WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL
ORDER BY
  (status = 'ACTIVE') DESC,
  lower(COALESCE(NULLIF(trim(display_name), ''), email)) ASC,
  lower(email) ASC,
  id ASC;

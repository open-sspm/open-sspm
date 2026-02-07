-- Legacy IdP query names now backed by Okta accounts in accounts.

-- name: UpsertIdPUsersBulk :execrows
WITH sync_source AS (
  SELECT COALESCE(NULLIF(trim(sr.source_name), ''), 'okta') AS source_name
  FROM sync_runs sr
  WHERE sr.id = sqlc.arg(seen_in_run_id)::bigint
  LIMIT 1
),
input AS (
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
INSERT INTO accounts (
  source_kind,
  source_name,
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
  'okta',
  COALESCE((SELECT source_name FROM sync_source), 'okta'),
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
ON CONFLICT (source_kind, source_name, external_id) DO UPDATE SET
  email = EXCLUDED.email,
  display_name = EXCLUDED.display_name,
  status = EXCLUDED.status,
  raw_json = EXCLUDED.raw_json,
  last_login_at = COALESCE(EXCLUDED.last_login_at, accounts.last_login_at),
  last_login_ip = COALESCE(NULLIF(EXCLUDED.last_login_ip, ''), accounts.last_login_ip),
  last_login_region = COALESCE(NULLIF(EXCLUDED.last_login_region, ''), accounts.last_login_region),
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  updated_at = now();

-- name: ListIdPUsers :many
SELECT *
FROM accounts
WHERE source_kind = 'okta'
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
ORDER BY id ASC;

-- name: GetIdPUser :one
SELECT *
FROM accounts
WHERE id = $1
  AND source_kind = 'okta'
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL;

-- name: CountIdPUsers :one
SELECT count(*)
FROM accounts
WHERE source_kind = 'okta'
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL;

-- name: CountIdPUsersByQueryAndState :one
SELECT count(*)
FROM accounts
WHERE
  source_kind = 'okta'
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
  AND (
    sqlc.arg(query)::text = ''
    OR email ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
  AND (
    sqlc.arg(state)::text = ''
    OR (sqlc.arg(state)::text = 'active' AND lower(status) = 'active')
    OR (sqlc.arg(state)::text = 'inactive' AND lower(status) <> 'active')
  );

-- name: ListIdPUsersPageByQueryAndState :many
SELECT *
FROM accounts
WHERE
  source_kind = 'okta'
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
  AND (
    sqlc.arg(query)::text = ''
    OR email ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
  AND (
    sqlc.arg(state)::text = ''
    OR (sqlc.arg(state)::text = 'active' AND lower(status) = 'active')
    OR (sqlc.arg(state)::text = 'inactive' AND lower(status) <> 'active')
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
FROM accounts
WHERE source_kind = 'okta'
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
ORDER BY
  (lower(status) = 'active') DESC,
  lower(COALESCE(NULLIF(trim(display_name), ''), email)) ASC,
  lower(email) ASC,
  id ASC
LIMIT 200;

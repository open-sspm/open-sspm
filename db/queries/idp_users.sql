-- Legacy IdP query names now backed by Okta accounts in accounts.

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

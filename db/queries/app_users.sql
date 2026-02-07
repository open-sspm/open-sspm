-- name: UpsertAppUsersBulkBySource :execrows
WITH input AS (
  SELECT
    i,
    (sqlc.arg(external_ids)::text[])[i] AS external_id,
    lower(trim((sqlc.arg(emails)::text[])[i])) AS email,
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
  sqlc.arg(source_kind)::text,
  sqlc.arg(source_name)::text,
  input.external_id,
  input.email,
  input.display_name,
  COALESCE(NULLIF(trim(input.raw_json ->> 'status'), ''), ''),
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
  last_login_ip = CASE WHEN EXCLUDED.last_login_ip <> '' THEN EXCLUDED.last_login_ip ELSE accounts.last_login_ip END,
  last_login_region = CASE WHEN EXCLUDED.last_login_region <> '' THEN EXCLUDED.last_login_region ELSE accounts.last_login_region END,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  updated_at = now()
;

-- name: UpsertOktaAccountsBulk :execrows
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
  sqlc.arg(source_name)::text,
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

-- name: CountAppUsersWithLinkBySourceAndQuery :one
SELECT count(*)
FROM accounts au
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
  COALESCE(ia.identity_id, 0) AS idp_user_id
FROM accounts au
LEFT JOIN identity_accounts ia ON ia.account_id = au.id
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
FROM accounts au
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
    OR (
      sqlc.arg(state)::text = 'active'
      AND lower(COALESCE(NULLIF(trim(au.status), ''), NULLIF(trim(au.raw_json->>'status'), ''), '')) = 'active'
    )
    OR (
      sqlc.arg(state)::text = 'inactive'
      AND lower(COALESCE(NULLIF(trim(au.status), ''), NULLIF(trim(au.raw_json->>'status'), ''), '')) <> 'active'
    )
  );

-- name: ListAppUsersPageBySourceAndQueryAndState :many
SELECT au.*
FROM accounts au
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
    OR (
      sqlc.arg(state)::text = 'active'
      AND lower(COALESCE(NULLIF(trim(au.status), ''), NULLIF(trim(au.raw_json->>'status'), ''), '')) = 'active'
    )
    OR (
      sqlc.arg(state)::text = 'inactive'
      AND lower(COALESCE(NULLIF(trim(au.status), ''), NULLIF(trim(au.raw_json->>'status'), ''), '')) <> 'active'
    )
  )
ORDER BY au.id DESC
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: CountUnmatchedAppUsersBySourceAndQuery :one
WITH authoritative_identities AS (
  SELECT DISTINCT ia.identity_id
  FROM identity_accounts ia
  JOIN accounts anchor ON anchor.id = ia.account_id
  JOIN identity_source_settings iss
    ON iss.source_kind = anchor.source_kind
   AND iss.source_name = anchor.source_name
   AND iss.is_authoritative
  WHERE anchor.expired_at IS NULL
    AND anchor.last_observed_run_id IS NOT NULL
)
SELECT count(*)
FROM accounts au
LEFT JOIN identity_accounts ia ON ia.account_id = au.id
LEFT JOIN authoritative_identities ai ON ai.identity_id = ia.identity_id
WHERE
  au.source_kind = sqlc.arg(source_kind)
  AND au.source_name = sqlc.arg(source_name)
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
  AND (
    ia.identity_id IS NULL
    OR ai.identity_id IS NULL
  )
  AND (
    sqlc.arg(query)::text = ''
    OR au.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.email ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
  );

-- name: ListUnmatchedAppUsersPageBySourceAndQuery :many
WITH authoritative_identities AS (
  SELECT DISTINCT ia.identity_id
  FROM identity_accounts ia
  JOIN accounts anchor ON anchor.id = ia.account_id
  JOIN identity_source_settings iss
    ON iss.source_kind = anchor.source_kind
   AND iss.source_name = anchor.source_name
   AND iss.is_authoritative
  WHERE anchor.expired_at IS NULL
    AND anchor.last_observed_run_id IS NOT NULL
)
SELECT au.*
FROM accounts au
LEFT JOIN identity_accounts ia ON ia.account_id = au.id
LEFT JOIN authoritative_identities ai ON ai.identity_id = ia.identity_id
WHERE
  au.source_kind = sqlc.arg(source_kind)
  AND au.source_name = sqlc.arg(source_name)
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
  AND (
    ia.identity_id IS NULL
    OR ai.identity_id IS NULL
  )
  AND (
    sqlc.arg(query)::text = ''
    OR au.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.email ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
ORDER BY au.display_name, au.email, au.external_id
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: GetAppUser :one
SELECT *
FROM accounts
WHERE id = $1
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL;

-- name: CountAppUsersBySource :one
SELECT count(*)
FROM accounts
WHERE source_kind = $1
  AND source_name = $2
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL;

-- name: CountMatchedAppUsersBySource :one
WITH authoritative_identities AS (
  SELECT DISTINCT ia.identity_id
  FROM identity_accounts ia
  JOIN accounts anchor ON anchor.id = ia.account_id
  JOIN identity_source_settings iss
    ON iss.source_kind = anchor.source_kind
   AND iss.source_name = anchor.source_name
   AND iss.is_authoritative
  WHERE anchor.expired_at IS NULL
    AND anchor.last_observed_run_id IS NOT NULL
)
SELECT count(*)
FROM accounts au
JOIN identity_accounts ia ON ia.account_id = au.id
JOIN authoritative_identities ai ON ai.identity_id = ia.identity_id
WHERE au.source_kind = $1
  AND au.source_name = $2
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL;

-- name: CountUnmatchedAppUsersBySource :one
WITH authoritative_identities AS (
  SELECT DISTINCT ia.identity_id
  FROM identity_accounts ia
  JOIN accounts anchor ON anchor.id = ia.account_id
  JOIN identity_source_settings iss
    ON iss.source_kind = anchor.source_kind
   AND iss.source_name = anchor.source_name
   AND iss.is_authoritative
  WHERE anchor.expired_at IS NULL
    AND anchor.last_observed_run_id IS NOT NULL
)
SELECT count(*)
FROM accounts au
LEFT JOIN identity_accounts ia ON ia.account_id = au.id
LEFT JOIN authoritative_identities ai ON ai.identity_id = ia.identity_id
WHERE au.source_kind = $1
  AND au.source_name = $2
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
  AND (
    ia.identity_id IS NULL
    OR ai.identity_id IS NULL
  );

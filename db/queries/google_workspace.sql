-- name: CountGoogleWorkspaceUsersBySourceAndQuery :one
SELECT count(*)
FROM accounts au
WHERE
  au.source_kind = sqlc.arg(source_kind)::text
  AND au.source_name = sqlc.arg(source_name)::text
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
  AND lower(COALESCE(NULLIF(trim(au.raw_json ->> 'entity_category'), ''), '')) = 'user'
  AND (
    sqlc.arg(query)::text = ''
    OR au.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.email ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
  );

-- name: ListGoogleWorkspaceUsersPageBySourceAndQuery :many
SELECT
  au.*,
  COALESCE(ia.identity_id, 0) AS idp_user_id
FROM accounts au
LEFT JOIN identity_accounts ia ON ia.account_id = au.id
WHERE
  au.source_kind = sqlc.arg(source_kind)::text
  AND au.source_name = sqlc.arg(source_name)::text
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
  AND lower(COALESCE(NULLIF(trim(au.raw_json ->> 'entity_category'), ''), '')) = 'user'
  AND (
    sqlc.arg(query)::text = ''
    OR au.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.email ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
ORDER BY au.id DESC
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: CountUnmatchedGoogleWorkspaceUsersBySourceAndQuery :one
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
WHERE
  au.source_kind = sqlc.arg(source_kind)::text
  AND au.source_name = sqlc.arg(source_name)::text
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
  AND lower(COALESCE(NULLIF(trim(au.raw_json ->> 'entity_category'), ''), '')) = 'user'
  AND (
    sqlc.arg(query)::text = ''
    OR au.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.email ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
  AND NOT EXISTS (
    SELECT 1
    FROM identity_accounts ia
    WHERE ia.account_id = au.id
      AND ia.identity_id IN (SELECT identity_id FROM authoritative_identities)
  );

-- name: ListUnmatchedGoogleWorkspaceUsersPageBySourceAndQuery :many
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
WHERE
  au.source_kind = sqlc.arg(source_kind)::text
  AND au.source_name = sqlc.arg(source_name)::text
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
  AND lower(COALESCE(NULLIF(trim(au.raw_json ->> 'entity_category'), ''), '')) = 'user'
  AND (
    sqlc.arg(query)::text = ''
    OR au.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.email ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
  AND NOT EXISTS (
    SELECT 1
    FROM identity_accounts ia
    WHERE ia.account_id = au.id
      AND ia.identity_id IN (SELECT identity_id FROM authoritative_identities)
  )
ORDER BY au.id DESC
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: CountGoogleWorkspaceGroupsBySourceAndQuery :one
SELECT count(*)
FROM accounts au
WHERE
  au.source_kind = sqlc.arg(source_kind)::text
  AND au.source_name = sqlc.arg(source_name)::text
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
  AND lower(COALESCE(NULLIF(trim(au.raw_json ->> 'entity_category'), ''), '')) = 'group'
  AND (
    sqlc.arg(query)::text = ''
    OR au.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.email ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
  );

-- name: ListGoogleWorkspaceGroupsPageBySourceAndQuery :many
SELECT au.*
FROM accounts au
WHERE
  au.source_kind = sqlc.arg(source_kind)::text
  AND au.source_name = sqlc.arg(source_name)::text
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
  AND lower(COALESCE(NULLIF(trim(au.raw_json ->> 'entity_category'), ''), '')) = 'group'
  AND (
    sqlc.arg(query)::text = ''
    OR au.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.email ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
ORDER BY lower(COALESCE(NULLIF(trim(au.display_name), ''), au.external_id)) ASC, au.id ASC
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: ListGoogleWorkspaceGroupMemberCountsByGroupExternalIDs :many
WITH requested AS (
  SELECT DISTINCT trim(group_external_id)::text AS group_external_id
  FROM unnest(sqlc.arg(group_external_ids)::text[]) AS g(group_external_id)
  WHERE trim(group_external_id) <> ''
)
SELECT
  requested.group_external_id,
  count(DISTINCT e.app_user_id)::bigint AS member_count,
  count(DISTINCT e.app_user_id) FILTER (WHERE lower(trim(e.permission)) = 'owner')::bigint AS owner_count,
  count(DISTINCT e.app_user_id) FILTER (WHERE lower(trim(e.permission)) = 'manager')::bigint AS manager_count
FROM requested
LEFT JOIN entitlements e
  ON e.kind = 'google_group_member'
 AND e.resource = ('google_group:' || requested.group_external_id)
 AND e.expired_at IS NULL
 AND e.last_observed_run_id IS NOT NULL
GROUP BY requested.group_external_id
ORDER BY requested.group_external_id;

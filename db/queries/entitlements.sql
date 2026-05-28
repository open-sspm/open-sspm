-- name: UpsertEntitlementsBulkBySource :execrows
WITH input AS (
  SELECT
    i,
    (sqlc.arg(account_external_ids)::text[])[i] AS account_external_id,
    (sqlc.arg(kinds)::text[])[i] AS kind,
    (sqlc.arg(resources)::text[])[i] AS resource,
    (sqlc.arg(permissions)::text[])[i] AS permission,
    (sqlc.arg(raw_jsons)::jsonb[])[i] AS raw_json
  FROM generate_subscripts(sqlc.arg(account_external_ids)::text[], 1) AS s(i)
),
dedup AS (
  SELECT DISTINCT ON (account_external_id, kind, resource, permission)
    account_external_id,
    kind,
    resource,
    permission,
    raw_json
  FROM input
  ORDER BY account_external_id, kind, resource, permission, i DESC
)
INSERT INTO entitlements (
  app_user_id,
  kind,
  resource,
  permission,
  raw_json,
  seen_in_run_id,
  seen_at,
  updated_at
)
SELECT
  au.id,
  input.kind,
  input.resource,
  input.permission,
  input.raw_json,
  sqlc.arg(seen_in_run_id)::bigint,
  now(),
  now()
FROM dedup input
JOIN accounts au
  ON au.source_kind = sqlc.arg(source_kind)::text
  AND au.source_name = sqlc.arg(source_name)::text
  AND au.external_id = input.account_external_id
  AND (au.expired_at IS NULL OR au.seen_in_run_id = sqlc.arg(seen_in_run_id)::bigint)
  AND (
    au.last_observed_run_id IS NOT NULL
    OR au.seen_in_run_id = sqlc.arg(seen_in_run_id)::bigint
  )
ON CONFLICT (app_user_id, kind, resource, permission) DO UPDATE SET
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  updated_at = now();

-- name: ListEntitlementsForAccountIDs :many
SELECT
  id,
  app_user_id AS account_id,
  kind,
  resource,
  permission,
  raw_json,
  created_at,
  seen_in_run_id,
  seen_at,
  last_observed_run_id,
  last_observed_at,
  expired_at,
  expired_run_id,
  updated_at
FROM entitlements
WHERE app_user_id = ANY(sqlc.arg(account_ids)::bigint[])
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
ORDER BY app_user_id, id;

-- name: ListEntitlementAccessBySourceAndResourceRef :many
SELECT
  e.id AS entitlement_id,
  e.kind AS entitlement_kind,
  e.resource AS entitlement_resource,
  e.permission AS entitlement_permission,
  e.raw_json AS entitlement_raw_json,
  e.created_at AS entitlement_created_at,
  e.updated_at AS entitlement_updated_at,
  au.id AS account_id,
  au.source_kind AS account_source_kind,
  au.source_name AS account_source_name,
  au.external_id AS account_external_id,
  au.email AS account_email,
  au.display_name AS account_display_name,
  au.raw_json AS account_raw_json,
  ia.link_reason AS link_reason,
  i.id AS identity_id,
  i.primary_email AS identity_email,
  i.display_name AS identity_display_name,
  i.kind AS identity_status
FROM entitlements e
JOIN accounts au ON au.id = e.app_user_id
LEFT JOIN identity_accounts ia ON ia.account_id = au.id
LEFT JOIN identities i ON i.id = ia.identity_id
WHERE au.source_kind = sqlc.arg(source_kind)::text
  AND au.source_name = sqlc.arg(source_name)::text
  AND e.resource = sqlc.arg(resource_ref)::text
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
  AND e.expired_at IS NULL
  AND e.last_observed_run_id IS NOT NULL
ORDER BY i.primary_email, au.external_id, e.permission, e.id;

-- name: CountOktaAppAssignedAccountsFromEntitlementsByQuery :one
SELECT count(DISTINCT au.id)
FROM entitlements e
JOIN accounts au ON au.id = e.app_user_id
WHERE au.source_kind = 'okta'
  AND au.source_name = sqlc.arg(source_name)::text
  AND (
    au.entity_category = 'user'
    OR (
      au.entity_category = 'unknown'
      AND lower(trim(au.external_id)) NOT LIKE 'group:%'
    )
  )
  AND e.kind = 'application_assignment'
  AND e.resource = sqlc.arg(app_external_id)::text
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
  AND e.expired_at IS NULL
  AND e.last_observed_run_id IS NOT NULL
  AND (
    sqlc.arg(state)::text = ''
    OR (sqlc.arg(state)::text = 'active' AND au.status = 'ACTIVE')
    OR (sqlc.arg(state)::text = 'inactive' AND au.status <> 'ACTIVE')
  )
  AND (
    sqlc.arg(query)::text = ''
    OR au.email ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR au.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
  );

-- name: ListOktaAppAssignedAccountsFromEntitlementsPageByQuery :many
WITH ranked AS (
  SELECT
    au.id AS okta_account_id,
    au.external_id AS okta_account_external_id,
    au.email AS okta_account_email,
    au.display_name AS okta_account_display_name,
    au.status AS okta_account_status,
    e.permission AS scope,
    COALESCE((e.raw_json #> '{attributes,profile}')::text, (e.raw_json -> 'profile')::text, '{}'::text) AS profile_json,
    row_number() OVER (
      PARTITION BY au.id
      ORDER BY
        CASE UPPER(TRIM(e.permission))
          WHEN 'USER' THEN 0
          WHEN 'GROUP' THEN 1
          ELSE 2
        END,
        e.id
    ) AS assignment_rank
  FROM entitlements e
  JOIN accounts au ON au.id = e.app_user_id
  WHERE au.source_kind = 'okta'
    AND au.source_name = sqlc.arg(source_name)::text
    AND (
      au.entity_category = 'user'
      OR (
        au.entity_category = 'unknown'
        AND lower(trim(au.external_id)) NOT LIKE 'group:%'
      )
    )
    AND e.kind = 'application_assignment'
    AND e.resource = sqlc.arg(app_external_id)::text
    AND au.expired_at IS NULL
    AND au.last_observed_run_id IS NOT NULL
    AND e.expired_at IS NULL
    AND e.last_observed_run_id IS NOT NULL
    AND (
      sqlc.arg(state)::text = ''
      OR (sqlc.arg(state)::text = 'active' AND au.status = 'ACTIVE')
      OR (sqlc.arg(state)::text = 'inactive' AND au.status <> 'ACTIVE')
    )
    AND (
      sqlc.arg(query)::text = ''
      OR au.email ILIKE ('%' || sqlc.arg(query)::text || '%')
      OR au.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
      OR au.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    )
)
SELECT
  okta_account_id,
  okta_account_external_id,
  okta_account_email,
  okta_account_display_name,
  okta_account_status,
  scope,
  profile_json
FROM ranked
WHERE assignment_rank = 1
ORDER BY (okta_account_display_name = ''), okta_account_display_name, okta_account_email, okta_account_external_id
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: ListOktaAppGrantingGroupsFromEntitlementsForAccounts :many
SELECT DISTINCT
  gm.app_user_id AS okta_account_id,
  COALESCE(NULLIF(trim(group_account.display_name), ''), NULLIF(trim(gm.raw_json #>> '{attributes,target,display_name}'), ''), NULLIF(trim(gm.raw_json #>> '{profile,name}'), ''), regexp_replace(gm.resource, '^group:', ''))::text AS okta_group_name,
  regexp_replace(gm.resource, '^group:', '') AS okta_group_external_id
FROM entitlements gm
JOIN accounts user_account ON user_account.id = gm.app_user_id
JOIN accounts group_account
  ON group_account.source_kind = user_account.source_kind
 AND group_account.source_name = user_account.source_name
 AND group_account.external_id = gm.resource
JOIN entitlements app_group
  ON app_group.app_user_id = group_account.id
WHERE user_account.source_kind = 'okta'
  AND user_account.source_name = sqlc.arg(source_name)::text
  AND gm.app_user_id = ANY(sqlc.arg(okta_account_ids)::bigint[])
  AND gm.kind = 'group_membership'
  AND app_group.kind = 'application_assignment'
  AND app_group.resource = sqlc.arg(app_external_id)::text
  AND user_account.expired_at IS NULL
  AND user_account.last_observed_run_id IS NOT NULL
  AND group_account.expired_at IS NULL
  AND group_account.last_observed_run_id IS NOT NULL
  AND gm.expired_at IS NULL
  AND gm.last_observed_run_id IS NOT NULL
  AND app_group.expired_at IS NULL
  AND app_group.last_observed_run_id IS NOT NULL
ORDER BY gm.app_user_id, okta_group_name, okta_group_external_id;

-- name: ListOktaAppAssignmentsFromEntitlementsForAccount :many
WITH ranked AS (
  SELECT
    e.id AS entitlement_id,
    e.resource AS okta_app_external_id,
    e.permission AS scope,
    COALESCE((e.raw_json #> '{attributes,profile}')::text, (e.raw_json -> 'profile')::text, '{}'::text) AS profile_json,
    oa.source_name AS okta_app_source_name,
    oa.label AS app_label,
    oa.name AS app_name,
    COALESCE(m.integration_kind, '') AS integration_kind,
    row_number() OVER (
      PARTITION BY e.resource
      ORDER BY
        CASE UPPER(TRIM(e.permission))
          WHEN 'USER' THEN 0
          WHEN 'GROUP' THEN 1
          ELSE 2
        END,
        e.id
    ) AS assignment_rank
  FROM entitlements e
  JOIN accounts au ON au.id = e.app_user_id
  JOIN okta_apps oa
    ON oa.source_kind = au.source_kind
   AND oa.source_name = au.source_name
   AND oa.external_id = e.resource
  LEFT JOIN integration_okta_app_map m
    ON m.okta_source_kind = oa.source_kind
   AND m.okta_source_name = oa.source_name
   AND m.okta_app_external_id = oa.external_id
  WHERE e.app_user_id = $1
    AND au.source_kind = 'okta'
    AND e.kind = 'application_assignment'
    AND au.expired_at IS NULL
    AND au.last_observed_run_id IS NOT NULL
    AND e.expired_at IS NULL
    AND e.last_observed_run_id IS NOT NULL
    AND oa.expired_at IS NULL
    AND oa.last_observed_run_id IS NOT NULL
)
SELECT
  entitlement_id,
  okta_app_external_id,
  scope,
  profile_json,
  okta_app_source_name,
  app_label,
  app_name,
  integration_kind
FROM ranked
WHERE assignment_rank = 1
ORDER BY app_label, app_name, okta_app_external_id;

-- name: ListOktaAppGrantingGroupsFromEntitlementsForAccountApps :many
SELECT DISTINCT
  app_group.resource AS okta_app_external_id,
  COALESCE(NULLIF(trim(group_account.display_name), ''), NULLIF(trim(gm.raw_json #>> '{attributes,target,display_name}'), ''), NULLIF(trim(gm.raw_json #>> '{profile,name}'), ''), regexp_replace(gm.resource, '^group:', ''))::text AS okta_group_name,
  regexp_replace(gm.resource, '^group:', '') AS okta_group_external_id
FROM entitlements gm
JOIN accounts user_account ON user_account.id = gm.app_user_id
JOIN accounts group_account
  ON group_account.source_kind = user_account.source_kind
 AND group_account.source_name = user_account.source_name
 AND group_account.external_id = gm.resource
JOIN entitlements app_group
  ON app_group.app_user_id = group_account.id
WHERE gm.app_user_id = sqlc.arg(okta_account_id)::bigint
  AND app_group.resource = ANY(sqlc.arg(okta_app_external_ids)::text[])
  AND user_account.source_kind = 'okta'
  AND gm.kind = 'group_membership'
  AND app_group.kind = 'application_assignment'
  AND user_account.expired_at IS NULL
  AND user_account.last_observed_run_id IS NOT NULL
  AND group_account.expired_at IS NULL
  AND group_account.last_observed_run_id IS NOT NULL
  AND gm.expired_at IS NULL
  AND gm.last_observed_run_id IS NOT NULL
  AND app_group.expired_at IS NULL
  AND app_group.last_observed_run_id IS NOT NULL
ORDER BY okta_app_external_id, okta_group_name, okta_group_external_id;

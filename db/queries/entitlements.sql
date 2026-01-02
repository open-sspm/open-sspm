-- name: InsertEntitlement :one
INSERT INTO entitlements (app_user_id, kind, resource, permission, raw_json, seen_in_run_id, seen_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, now(), now())
ON CONFLICT (app_user_id, kind, resource, permission) DO UPDATE SET
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  updated_at = now()
RETURNING *;

-- name: UpsertEntitlementsBulkBySource :execrows
WITH input AS (
  SELECT
    i,
    (sqlc.arg(app_user_external_ids)::text[])[i] AS app_user_external_id,
    (sqlc.arg(kinds)::text[])[i] AS kind,
    (sqlc.arg(resources)::text[])[i] AS resource,
    (sqlc.arg(permissions)::text[])[i] AS permission,
    (sqlc.arg(raw_jsons)::jsonb[])[i] AS raw_json
  FROM generate_subscripts(sqlc.arg(app_user_external_ids)::text[], 1) AS s(i)
),
dedup AS (
  SELECT DISTINCT ON (app_user_external_id, kind, resource, permission)
    app_user_external_id,
    kind,
    resource,
    permission,
    raw_json
  FROM input
  ORDER BY app_user_external_id, kind, resource, permission, i DESC
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
JOIN app_users au
  ON au.source_kind = sqlc.arg(source_kind)::text
  AND au.source_name = sqlc.arg(source_name)::text
  AND au.external_id = input.app_user_external_id
ON CONFLICT (app_user_id, kind, resource, permission) DO UPDATE SET
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  updated_at = now();

-- name: ListEntitlementsForAppUser :many
SELECT *
FROM entitlements
WHERE app_user_id = $1
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
ORDER BY id;

-- name: ListEntitlementsForAppUserIDs :many
SELECT *
FROM entitlements
WHERE app_user_id = ANY(sqlc.arg(app_user_ids)::bigint[])
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
ORDER BY app_user_id, id;

-- name: ListEntitlementResourcesByAppUserIDsAndKind :many
SELECT
  app_user_id,
  resource
FROM entitlements
WHERE app_user_id = ANY(sqlc.arg(app_user_ids)::bigint[])
  AND kind = sqlc.arg(ent_kind)
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
ORDER BY app_user_id, resource;

-- name: ListEntitlementAccessBySourceAndResourceRef :many
SELECT
  e.id AS entitlement_id,
  e.kind AS entitlement_kind,
  e.resource AS entitlement_resource,
  e.permission AS entitlement_permission,
  e.raw_json AS entitlement_raw_json,
  e.created_at AS entitlement_created_at,
  e.updated_at AS entitlement_updated_at,
  au.id AS app_user_id,
  au.source_kind AS app_user_source_kind,
  au.source_name AS app_user_source_name,
  au.external_id AS app_user_external_id,
  au.email AS app_user_email,
  au.display_name AS app_user_display_name,
  au.raw_json AS app_user_raw_json,
  il.link_reason AS link_reason,
  iu.id AS idp_user_id,
  iu.email AS idp_user_email,
  iu.display_name AS idp_user_display_name,
  iu.status AS idp_user_status
FROM entitlements e
JOIN app_users au ON au.id = e.app_user_id
LEFT JOIN identity_links il ON il.app_user_id = au.id
LEFT JOIN idp_users iu ON iu.id = il.idp_user_id
WHERE au.source_kind = sqlc.arg(source_kind)::text
  AND au.source_name = sqlc.arg(source_name)::text
  AND e.resource = sqlc.arg(resource_ref)::text
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
  AND e.expired_at IS NULL
  AND e.last_observed_run_id IS NOT NULL
ORDER BY iu.email, au.external_id, e.permission, e.id;

-- name: DeleteEntitlementsForAppUser :exec
DELETE FROM entitlements WHERE app_user_id = $1;

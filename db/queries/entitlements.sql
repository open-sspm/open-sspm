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

-- name: DeleteEntitlementsForAppUser :exec
DELETE FROM entitlements WHERE app_user_id = $1;

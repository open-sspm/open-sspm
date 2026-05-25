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

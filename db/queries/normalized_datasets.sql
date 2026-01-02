-- name: ListNormalizedEntitlementAssignments :many
SELECT
  e.id AS entitlement_id,
  iu.id AS idp_user_id,
  iu.email AS idp_user_email,
  iu.display_name AS idp_user_display_name,
  iu.status AS idp_user_status,
  au.source_kind AS source_kind,
  au.source_name AS source_name,
  au.external_id AS app_user_external_id,
  e.kind AS entitlement_kind,
  e.resource AS entitlement_resource,
  e.permission AS entitlement_permission
FROM entitlements e
JOIN app_users au ON au.id = e.app_user_id
JOIN identity_links il ON il.app_user_id = au.id
JOIN idp_users iu ON iu.id = il.idp_user_id
WHERE
  e.expired_at IS NULL
  AND e.last_observed_run_id IS NOT NULL
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
  AND iu.expired_at IS NULL
  AND iu.last_observed_run_id IS NOT NULL
ORDER BY iu.id, au.id, e.id;


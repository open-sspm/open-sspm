-- name: ListNormalizedIdentitiesV1 :many
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
SELECT
  i.id AS idp_user_id,
  COALESCE(auth_account.external_id, '') AS idp_user_external_id,
  i.primary_email AS idp_user_email,
  i.display_name AS idp_user_display_name,
  COALESCE(NULLIF(auth_account.status, ''), i.kind) AS idp_user_status
FROM identities i
JOIN authoritative_identities ai ON ai.identity_id = i.id
LEFT JOIN LATERAL (
  SELECT a.external_id, a.status
  FROM identity_accounts ia
  JOIN accounts a ON a.id = ia.account_id
  JOIN identity_source_settings iss
    ON iss.source_kind = a.source_kind
   AND iss.source_name = a.source_name
   AND iss.is_authoritative
  WHERE ia.identity_id = i.id
    AND a.expired_at IS NULL
    AND a.last_observed_run_id IS NOT NULL
  ORDER BY a.id ASC
  LIMIT 1
) auth_account ON TRUE
ORDER BY i.id;

-- name: ListNormalizedIdentitiesV2 :many
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
SELECT
  i.id AS identity_id,
  i.kind AS identity_kind,
  i.primary_email AS identity_email,
  i.display_name AS identity_display_name,
  (ai.identity_id IS NOT NULL)::boolean AS identity_managed,
  COALESCE(auth_account.source_kind, '') AS authoritative_source_kind,
  COALESCE(auth_account.source_name, '') AS authoritative_source_name,
  COALESCE(auth_account.external_id, '') AS authoritative_external_id
FROM identities i
LEFT JOIN authoritative_identities ai ON ai.identity_id = i.id
LEFT JOIN LATERAL (
  SELECT a.source_kind, a.source_name, a.external_id
  FROM identity_accounts ia
  JOIN accounts a ON a.id = ia.account_id
  JOIN identity_source_settings iss
    ON iss.source_kind = a.source_kind
   AND iss.source_name = a.source_name
   AND iss.is_authoritative
  WHERE ia.identity_id = i.id
    AND a.expired_at IS NULL
    AND a.last_observed_run_id IS NOT NULL
  ORDER BY a.id ASC
  LIMIT 1
) auth_account ON TRUE
ORDER BY i.id;

-- name: ListNormalizedEntitlementAssignmentsV1 :many
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
SELECT
  e.id AS entitlement_id,
  i.id AS idp_user_id,
  i.primary_email AS idp_user_email,
  i.display_name AS idp_user_display_name,
  i.kind AS idp_user_status,
  au.source_kind AS source_kind,
  au.source_name AS source_name,
  au.external_id AS app_user_external_id,
  e.kind AS entitlement_kind,
  e.resource AS entitlement_resource,
  e.permission AS entitlement_permission
FROM entitlements e
JOIN accounts au ON au.id = e.app_user_id
JOIN identity_accounts ia ON ia.account_id = au.id
JOIN identities i ON i.id = ia.identity_id
JOIN authoritative_identities auth_i ON auth_i.identity_id = i.id
WHERE
  e.expired_at IS NULL
  AND e.last_observed_run_id IS NOT NULL
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
ORDER BY i.id, au.id, e.id;

-- name: ListNormalizedEntitlementAssignmentsV2 :many
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
SELECT
  e.id AS entitlement_id,
  i.id AS identity_id,
  i.kind AS identity_kind,
  i.primary_email AS identity_email,
  i.display_name AS identity_display_name,
  (auth_i.identity_id IS NOT NULL)::boolean AS identity_managed,
  au.source_kind AS source_kind,
  au.source_name AS source_name,
  au.external_id AS app_user_external_id,
  e.kind AS entitlement_kind,
  e.resource AS entitlement_resource,
  e.permission AS entitlement_permission
FROM entitlements e
JOIN accounts au ON au.id = e.app_user_id
JOIN identity_accounts ia ON ia.account_id = au.id
JOIN identities i ON i.id = ia.identity_id
LEFT JOIN authoritative_identities auth_i ON auth_i.identity_id = i.id
WHERE
  e.expired_at IS NULL
  AND e.last_observed_run_id IS NOT NULL
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
ORDER BY i.id, au.id, e.id;

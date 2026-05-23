-- name: ListNormalizedIdentities :many
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
  CASE
    WHEN i.kind IN ('service', 'bot') THEN 'non_human'
    WHEN ai.identity_id IS NOT NULL THEN 'managed'
    ELSE 'unmanaged'
  END AS identity_posture,
  CASE
    WHEN i.kind IN ('service', 'bot') THEN 'not_applicable'
    WHEN ai.identity_id IS NOT NULL THEN 'anchored'
    ELSE 'missing_anchor'
  END AS identity_anchor_state,
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

-- name: ListNormalizedEntitlementAssignments :many
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
  CASE
    WHEN i.kind IN ('service', 'bot') THEN 'non_human'
    WHEN auth_i.identity_id IS NOT NULL THEN 'managed'
    ELSE 'unmanaged'
  END AS identity_posture,
  CASE
    WHEN i.kind IN ('service', 'bot') THEN 'not_applicable'
    WHEN auth_i.identity_id IS NOT NULL THEN 'anchored'
    ELSE 'missing_anchor'
  END AS identity_anchor_state,
  COALESCE(auth_account.source_kind, '') AS authoritative_source_kind,
  COALESCE(auth_account.source_name, '') AS authoritative_source_name,
  COALESCE(auth_account.external_id, '') AS authoritative_external_id,
  au.source_kind AS account_source_kind,
  au.source_name AS account_source_name,
  au.external_id AS account_external_id,
  e.kind AS entitlement_kind,
  e.resource AS entitlement_resource,
  e.permission AS entitlement_permission
FROM entitlements e
JOIN accounts au ON au.id = e.app_user_id
JOIN identity_accounts ia ON ia.account_id = au.id
JOIN identities i ON i.id = ia.identity_id
LEFT JOIN authoritative_identities auth_i ON auth_i.identity_id = i.id
LEFT JOIN LATERAL (
  SELECT a.source_kind, a.source_name, a.external_id
  FROM identity_accounts ia2
  JOIN accounts a ON a.id = ia2.account_id
  JOIN identity_source_settings iss
    ON iss.source_kind = a.source_kind
   AND iss.source_name = a.source_name
   AND iss.is_authoritative
  WHERE ia2.identity_id = i.id
    AND a.expired_at IS NULL
    AND a.last_observed_run_id IS NOT NULL
  ORDER BY a.id ASC
  LIMIT 1
) auth_account ON TRUE
WHERE
  e.expired_at IS NULL
  AND e.last_observed_run_id IS NOT NULL
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
ORDER BY i.id, au.id, e.id;

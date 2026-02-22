-- Compatibility query names on top of identity_accounts.

-- name: CreateIdentityLink :one
INSERT INTO identity_accounts (identity_id, account_id, link_reason, confidence, updated_at)
VALUES ($1, $2, $3, 1.0, now())
ON CONFLICT (account_id) DO UPDATE SET
  identity_id = EXCLUDED.identity_id,
  link_reason = EXCLUDED.link_reason,
  confidence = EXCLUDED.confidence,
  updated_at = EXCLUDED.updated_at
RETURNING
  id,
  identity_id AS idp_user_id,
  account_id AS app_user_id,
  link_reason,
  created_at;

-- name: BulkAutoLinkByEmail :execrows
INSERT INTO identity_accounts (identity_id, account_id, link_reason, confidence, updated_at)
SELECT DISTINCT ON (au.id)
  i.id,
  au.id,
  'auto_email',
  1.0,
  now()
FROM accounts au
JOIN identities i ON lower(trim(i.primary_email)) = lower(trim(au.email))
LEFT JOIN identity_accounts ia ON ia.account_id = au.id
WHERE au.source_kind = sqlc.arg(source_kind)
  AND au.source_name = sqlc.arg(source_name)
  AND au.email <> ''
  AND i.primary_email <> ''
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
  AND ia.id IS NULL
ORDER BY au.id, i.id ASC
ON CONFLICT (account_id) DO NOTHING;

-- name: GetIdentityLinkByAppUser :one
SELECT
  ia.id,
  ia.identity_id AS idp_user_id,
  ia.account_id AS app_user_id,
  ia.link_reason,
  ia.created_at
FROM identity_accounts ia
WHERE ia.account_id = $1;

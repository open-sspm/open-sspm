-- name: UpsertIdentityAccountLink :one
INSERT INTO identity_accounts (identity_id, account_id, link_reason, confidence, updated_at)
VALUES (
  sqlc.arg(identity_id)::bigint,
  sqlc.arg(account_id)::bigint,
  sqlc.arg(link_reason)::text,
  sqlc.arg(confidence)::real,
  now()
)
ON CONFLICT (account_id) DO UPDATE SET
  identity_id = EXCLUDED.identity_id,
  link_reason = EXCLUDED.link_reason,
  confidence = EXCLUDED.confidence,
  updated_at = EXCLUDED.updated_at
RETURNING *;

-- name: GetIdentityAccountLinkByAccountID :one
SELECT *
FROM identity_accounts
WHERE account_id = $1;

-- name: GetIdentityBySourceAndExternalID :one
SELECT i.*
FROM identities i
JOIN identity_accounts ia ON ia.identity_id = i.id
JOIN accounts a ON a.id = ia.account_id
WHERE lower(trim(a.source_kind)) = lower(trim(sqlc.arg(source_kind)::text))
  AND lower(trim(a.source_name)) = lower(trim(sqlc.arg(source_name)::text))
  AND lower(trim(a.external_id)) = lower(trim(sqlc.arg(external_id)::text))
  AND a.expired_at IS NULL
  AND a.last_observed_run_id IS NOT NULL
ORDER BY i.id ASC
LIMIT 1;

-- name: ListLinkedAccountsForIdentity :many
SELECT a.*
FROM accounts a
JOIN identity_accounts ia ON ia.account_id = a.id
WHERE ia.identity_id = $1
  AND a.expired_at IS NULL
  AND a.last_observed_run_id IS NOT NULL
ORDER BY a.source_kind, a.source_name, a.external_id;

-- name: ListUnlinkedAccountsPage :many
SELECT a.*
FROM accounts a
LEFT JOIN identity_accounts ia ON ia.account_id = a.id
WHERE ia.id IS NULL
  AND a.expired_at IS NULL
  AND a.last_observed_run_id IS NOT NULL
ORDER BY a.id ASC
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: CountUnlinkedAccounts :one
SELECT count(*)
FROM accounts a
LEFT JOIN identity_accounts ia ON ia.account_id = a.id
WHERE ia.id IS NULL
  AND a.expired_at IS NULL
  AND a.last_observed_run_id IS NOT NULL;

-- name: ListIdentityAccountAttributes :many
SELECT
  ia.identity_id,
  a.id AS account_id,
  a.source_kind,
  a.source_name,
  a.email,
  a.display_name
FROM identity_accounts ia
JOIN accounts a ON a.id = ia.account_id
WHERE a.expired_at IS NULL
  AND a.last_observed_run_id IS NOT NULL
ORDER BY ia.identity_id, a.id;

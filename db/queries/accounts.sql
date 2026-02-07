-- name: GetAccountByID :one
SELECT *
FROM accounts
WHERE id = $1;

-- name: ListAccountsForIdentity :many
SELECT a.*
FROM accounts a
JOIN identity_accounts ia ON ia.account_id = a.id
WHERE ia.identity_id = $1
ORDER BY a.source_kind, a.source_name, a.external_id;

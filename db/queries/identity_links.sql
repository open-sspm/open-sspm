-- name: CreateIdentityLink :one
INSERT INTO identity_links (idp_user_id, app_user_id, link_reason)
VALUES ($1, $2, $3)
ON CONFLICT (app_user_id) DO UPDATE SET
  idp_user_id = EXCLUDED.idp_user_id,
  link_reason = EXCLUDED.link_reason
RETURNING *;

-- name: BulkAutoLinkByEmail :execrows
INSERT INTO identity_links (idp_user_id, app_user_id, link_reason)
SELECT DISTINCT ON (au.id)
  iu.id,
  au.id,
  'auto_email'
FROM app_users au
JOIN idp_users iu ON lower(iu.email) = lower(au.email)
LEFT JOIN identity_links il ON il.app_user_id = au.id
WHERE au.source_kind = sqlc.arg(source_kind)
  AND au.source_name = sqlc.arg(source_name)
  AND au.email <> ''
  AND iu.email <> ''
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
  AND iu.expired_at IS NULL
  AND iu.last_observed_run_id IS NOT NULL
  AND il.id IS NULL
ORDER BY au.id, (iu.status = 'ACTIVE') DESC, iu.id ASC
ON CONFLICT (app_user_id) DO NOTHING;

-- name: ListLinkedAppUsersForIdPUser :many
SELECT au.*
FROM app_users au
JOIN identity_links il ON il.app_user_id = au.id
WHERE il.idp_user_id = $1
  AND au.expired_at IS NULL
  AND au.last_observed_run_id IS NOT NULL
ORDER BY au.source_kind, au.source_name, au.external_id;

-- name: GetIdentityLinkByAppUser :one
SELECT * FROM identity_links WHERE app_user_id = $1;

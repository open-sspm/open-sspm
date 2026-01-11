-- name: CountAuthUsers :one
SELECT count(*)
FROM auth_users;

-- name: CountAuthAdmins :one
SELECT count(*)
FROM auth_users
WHERE role = 'admin' AND is_active = true;

-- name: ListActiveAuthAdminsForUpdate :many
SELECT id
FROM auth_users
WHERE role = 'admin' AND is_active = true
FOR UPDATE;

-- name: GetAuthUser :one
SELECT *
FROM auth_users
WHERE id = $1;

-- name: GetAuthUserForUpdate :one
SELECT *
FROM auth_users
WHERE id = $1
FOR UPDATE;

-- name: GetAuthUserByEmail :one
SELECT *
FROM auth_users
WHERE email = lower(trim($1));

-- name: ListAuthUsers :many
SELECT *
FROM auth_users
ORDER BY email ASC;

-- name: CreateAuthUser :one
INSERT INTO auth_users (
  email,
  password_hash,
  role,
  is_active,
  created_at,
  updated_at
)
VALUES (
  lower(trim(sqlc.arg(email)::text)),
  sqlc.arg(password_hash)::text,
  sqlc.arg(role)::text,
  sqlc.arg(is_active)::boolean,
  now(),
  now()
)
RETURNING *;

-- name: UpdateAuthUserPasswordHash :exec
UPDATE auth_users
SET
  password_hash = sqlc.arg(password_hash)::text,
  updated_at = now()
WHERE id = sqlc.arg(id)::bigint;

-- name: UpdateAuthUserRole :exec
UPDATE auth_users
SET
  role = sqlc.arg(role)::text,
  updated_at = now()
WHERE id = sqlc.arg(id)::bigint;

-- name: DeleteAuthUser :exec
DELETE FROM auth_users
WHERE id = $1;

-- name: UpdateAuthUserLoginMeta :exec
UPDATE auth_users
SET
  last_login_at = sqlc.arg(last_login_at)::timestamptz,
  last_login_ip = sqlc.arg(last_login_ip)::text,
  updated_at = now()
WHERE id = sqlc.arg(id)::bigint;

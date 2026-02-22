-- name: ListAuthoritativeSources :many
SELECT *
FROM identity_source_settings
WHERE is_authoritative = TRUE
ORDER BY source_kind, source_name;

-- name: ListIdentitySourceSettings :many
SELECT *
FROM identity_source_settings
ORDER BY source_kind, source_name;

-- name: UpsertIdentitySourceSetting :one
INSERT INTO identity_source_settings (source_kind, source_name, is_authoritative, updated_at)
VALUES (
  sqlc.arg(source_kind)::text,
  sqlc.arg(source_name)::text,
  sqlc.arg(is_authoritative)::boolean,
  now()
)
ON CONFLICT (source_kind, source_name) DO UPDATE SET
  is_authoritative = EXCLUDED.is_authoritative,
  updated_at = EXCLUDED.updated_at
RETURNING *;

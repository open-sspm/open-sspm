-- name: ListConnectorSecrets :many
SELECT kind, secret_name, ciphertext, nonce, version, created_at, updated_at
FROM connector_secrets
ORDER BY kind, secret_name;

-- name: ListConnectorSecretsByKind :many
SELECT kind, secret_name, ciphertext, nonce, version, created_at, updated_at
FROM connector_secrets
WHERE kind = $1
ORDER BY secret_name;

-- name: UpsertConnectorSecret :one
INSERT INTO connector_secrets (
  kind,
  secret_name,
  ciphertext,
  nonce,
  version
)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (kind, secret_name) DO UPDATE SET
  ciphertext = EXCLUDED.ciphertext,
  nonce = EXCLUDED.nonce,
  version = EXCLUDED.version,
  updated_at = now()
RETURNING kind, secret_name, ciphertext, nonce, version, created_at, updated_at;

-- name: DeleteConnectorSecretsByKind :execrows
DELETE FROM connector_secrets
WHERE kind = $1;

-- name: ListConnectorConfigs :many
SELECT kind, enabled, config, created_at, updated_at
FROM connector_configs
ORDER BY kind;

-- name: GetConnectorConfig :one
SELECT kind, enabled, config, created_at, updated_at
FROM connector_configs
WHERE kind = $1;

-- name: UpdateConnectorConfigEnabled :one
UPDATE connector_configs
SET enabled = $2, updated_at = now()
WHERE kind = $1
RETURNING kind, enabled, config, created_at, updated_at;

-- name: UpdateConnectorConfig :one
UPDATE connector_configs
SET config = $2, updated_at = now()
WHERE kind = $1
RETURNING kind, enabled, config, created_at, updated_at;

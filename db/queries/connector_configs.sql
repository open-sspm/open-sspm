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

-- name: UpsertConnectorConfig :one
INSERT INTO connector_configs (kind, enabled, config)
VALUES ($1, $2, $3)
ON CONFLICT (kind) DO UPDATE
SET enabled = EXCLUDED.enabled,
    config = EXCLUDED.config,
    updated_at = now()
RETURNING kind, enabled, config, created_at, updated_at;

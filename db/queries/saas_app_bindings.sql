-- name: UpsertSaaSAppBinding :exec
INSERT INTO saas_app_bindings (
  saas_app_id,
  connector_kind,
  connector_source_name,
  binding_source,
  confidence,
  is_primary,
  created_by_auth_user_id,
  updated_at
)
VALUES (
  sqlc.arg(saas_app_id)::bigint,
  sqlc.arg(connector_kind)::text,
  sqlc.arg(connector_source_name)::text,
  sqlc.arg(binding_source)::text,
  sqlc.arg(confidence)::real,
  sqlc.arg(is_primary)::boolean,
  sqlc.narg(created_by_auth_user_id)::bigint,
  now()
)
ON CONFLICT (saas_app_id, connector_kind, connector_source_name) DO UPDATE SET
  binding_source = EXCLUDED.binding_source,
  confidence = EXCLUDED.confidence,
  is_primary = EXCLUDED.is_primary,
  created_by_auth_user_id = COALESCE(EXCLUDED.created_by_auth_user_id, saas_app_bindings.created_by_auth_user_id),
  updated_at = now()
WHERE NOT (
  saas_app_bindings.binding_source = 'manual'
  AND EXCLUDED.binding_source = 'auto'
);

-- name: ClearPrimarySaaSAppBindingsBySaaSAppID :exec
UPDATE saas_app_bindings
SET is_primary = false,
    updated_at = now()
WHERE saas_app_id = sqlc.arg(saas_app_id)::bigint
  AND is_primary;

-- name: DeleteManualSaaSAppBindingsBySaaSAppID :execrows
DELETE FROM saas_app_bindings
WHERE saas_app_id = sqlc.arg(saas_app_id)::bigint
  AND binding_source = 'manual';

-- name: RecomputePrimarySaaSAppBindingBySaaSAppID :execrows
WITH ranked AS (
  SELECT
    id,
    row_number() OVER (
      ORDER BY
        CASE WHEN binding_source = 'manual' THEN 0 ELSE 1 END,
        confidence DESC,
        id ASC
    ) AS rn
  FROM saas_app_bindings
  WHERE saas_app_id = sqlc.arg(saas_app_id)::bigint
)
UPDATE saas_app_bindings b
SET
  is_primary = (r.rn = 1),
  updated_at = now()
FROM ranked r
WHERE b.id = r.id
  AND b.is_primary IS DISTINCT FROM (r.rn = 1);

-- name: RecomputePrimarySaaSAppBindingsForAll :execrows
WITH ranked AS (
  SELECT
    id,
    saas_app_id,
    row_number() OVER (
      PARTITION BY saas_app_id
      ORDER BY
        CASE WHEN binding_source = 'manual' THEN 0 ELSE 1 END,
        confidence DESC,
        id ASC
    ) AS rn
  FROM saas_app_bindings
)
UPDATE saas_app_bindings b
SET
  is_primary = (r.rn = 1),
  updated_at = now()
FROM ranked r
WHERE b.id = r.id
  AND b.is_primary IS DISTINCT FROM (r.rn = 1);

-- name: ListSaaSAppBindingsBySaaSAppID :many
SELECT *
FROM saas_app_bindings
WHERE saas_app_id = $1
ORDER BY
  is_primary DESC,
  CASE WHEN binding_source = 'manual' THEN 0 ELSE 1 END,
  confidence DESC,
  id ASC;

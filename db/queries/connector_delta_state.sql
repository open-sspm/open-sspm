-- name: ListConnectorDeltaStatesBySource :many
SELECT *
FROM connector_delta_state
WHERE source_kind = sqlc.arg(source_kind)::text
  AND source_name = sqlc.arg(source_name)::text
ORDER BY resource ASC;

-- name: UpsertConnectorDeltaState :exec
INSERT INTO connector_delta_state (
  source_kind,
  source_name,
  resource,
  delta_link,
  last_success_run_id,
  last_finished_at,
  updated_at
)
VALUES (
  sqlc.arg(source_kind)::text,
  sqlc.arg(source_name)::text,
  sqlc.arg(resource)::text,
  sqlc.arg(delta_link)::text,
  sqlc.narg(last_success_run_id)::bigint,
  now(),
  now()
)
ON CONFLICT (source_kind, source_name, resource) DO UPDATE SET
  delta_link = EXCLUDED.delta_link,
  last_success_run_id = EXCLUDED.last_success_run_id,
  last_finished_at = EXCLUDED.last_finished_at,
  updated_at = now();

-- name: DeleteConnectorDeltaStatesBySource :exec
DELETE FROM connector_delta_state
WHERE source_kind = sqlc.arg(source_kind)::text
  AND source_name = sqlc.arg(source_name)::text;

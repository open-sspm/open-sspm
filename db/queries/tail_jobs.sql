-- name: GetActiveTailSyncJobByScope :one
SELECT *
FROM sync_jobs
WHERE lane = 'tail'
  AND COALESCE(connector_kind, '') = COALESCE(sqlc.narg(connector_kind), '')
  AND COALESCE(source_name, '') = COALESCE(sqlc.narg(source_name), '')
  AND COALESCE(resource, '') = COALESCE(sqlc.narg(resource), '')
  AND status IN ('pending', 'claimed', 'running')
LIMIT 1;

-- name: CreateTailSyncJob :one
INSERT INTO sync_jobs (
  id,
  lane,
  connector_kind,
  source_name,
  resource,
  trigger_kind,
  status,
  attempt_count,
  available_at,
  payload,
  priority,
  created_reason,
  rerun_requested
) VALUES (
  sqlc.arg(id),
  'tail',
  sqlc.arg(connector_kind)::text,
  sqlc.arg(source_name)::text,
  sqlc.arg(resource)::text,
  'scheduled',
  'pending',
  0,
  sqlc.arg(available_at)::timestamptz,
  sqlc.arg(payload)::jsonb,
  sqlc.arg(priority)::int,
  sqlc.arg(created_reason)::text,
  false
)
RETURNING *;


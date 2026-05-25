-- name: GetConnectorCursorState :one
SELECT *
FROM connector_cursor_state
WHERE source_kind = sqlc.arg(source_kind)::text
  AND source_name = sqlc.arg(source_name)::text
  AND resource = sqlc.arg(resource)::text;

-- name: GetConnectorCursorStateForUpdate :one
SELECT *
FROM connector_cursor_state
WHERE source_kind = sqlc.arg(source_kind)::text
  AND source_name = sqlc.arg(source_name)::text
  AND resource = sqlc.arg(resource)::text
FOR UPDATE;

-- name: UpsertConnectorCursorState :exec
INSERT INTO connector_cursor_state (
  source_kind,
  source_id,
  source_name,
  resource,
  cursor_kind,
  cursor_json,
  watermark,
  cursor_expires_at,
  last_success_at,
  last_attempt_at,
  last_error_at,
  last_error,
  last_run_id,
  last_provider_event_id,
  needs_full_resync,
  version,
  updated_at
)
VALUES (
  sqlc.arg(source_kind)::text,
  sqlc.narg(source_id)::bigint,
  sqlc.arg(source_name)::text,
  sqlc.arg(resource)::text,
  sqlc.arg(cursor_kind)::text,
  sqlc.arg(cursor_json)::jsonb,
  sqlc.narg(watermark)::timestamptz,
  sqlc.narg(cursor_expires_at)::timestamptz,
  sqlc.narg(last_success_at)::timestamptz,
  sqlc.narg(last_attempt_at)::timestamptz,
  sqlc.narg(last_error_at)::timestamptz,
  sqlc.arg(last_error)::text,
  sqlc.narg(last_run_id)::bigint,
  sqlc.arg(last_provider_event_id)::text,
  sqlc.arg(needs_full_resync)::boolean,
  0,
  now()
)
ON CONFLICT (source_kind, source_name, resource) DO UPDATE SET
  source_id = COALESCE(EXCLUDED.source_id, connector_cursor_state.source_id),
  cursor_kind = EXCLUDED.cursor_kind,
  cursor_json = EXCLUDED.cursor_json,
  watermark = EXCLUDED.watermark,
  cursor_expires_at = EXCLUDED.cursor_expires_at,
  last_success_at = COALESCE(EXCLUDED.last_success_at, connector_cursor_state.last_success_at),
  last_attempt_at = COALESCE(EXCLUDED.last_attempt_at, connector_cursor_state.last_attempt_at),
  last_error_at = EXCLUDED.last_error_at,
  last_error = EXCLUDED.last_error,
  last_run_id = COALESCE(EXCLUDED.last_run_id, connector_cursor_state.last_run_id),
  last_provider_event_id = EXCLUDED.last_provider_event_id,
  needs_full_resync = EXCLUDED.needs_full_resync,
  version = connector_cursor_state.version + 1,
  updated_at = now();

-- name: MarkConnectorCursorNeedsFullResync :exec
UPDATE connector_cursor_state
SET
  needs_full_resync = true,
  last_error_at = now(),
  last_error = sqlc.arg(last_error)::text,
  updated_at = now()
WHERE source_kind = sqlc.arg(source_kind)::text
  AND source_name = sqlc.arg(source_name)::text
  AND resource = sqlc.arg(resource)::text;

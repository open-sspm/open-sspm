-- Lease-based sync locks (no session-pinned connections).

-- name: TryAcquireSyncLockLease :one
INSERT INTO sync_locks (
  scope_kind,
  scope_name,
  holder_instance_id,
  holder_token,
  acquired_at,
  lease_expires_at,
  heartbeat_at
)
VALUES (
  sqlc.arg(scope_kind),
  sqlc.arg(scope_name),
  sqlc.arg(holder_instance_id),
  sqlc.arg(holder_token),
  clock_timestamp(),
  clock_timestamp() + (sqlc.arg(lease_seconds)::bigint * interval '1 second'),
  clock_timestamp()
)
ON CONFLICT (scope_kind, scope_name) DO UPDATE
SET
  holder_instance_id = EXCLUDED.holder_instance_id,
  holder_token = EXCLUDED.holder_token,
  acquired_at = clock_timestamp(),
  lease_expires_at = EXCLUDED.lease_expires_at,
  heartbeat_at = EXCLUDED.heartbeat_at
WHERE
  sync_locks.lease_expires_at < clock_timestamp()
  OR sync_locks.holder_token = EXCLUDED.holder_token
RETURNING
  scope_kind,
  scope_name,
  holder_instance_id,
  holder_token,
  acquired_at,
  lease_expires_at,
  heartbeat_at;

-- name: RenewSyncLockLease :one
UPDATE sync_locks
SET
  lease_expires_at = clock_timestamp() + (sqlc.arg(lease_seconds)::bigint * interval '1 second'),
  heartbeat_at = clock_timestamp()
WHERE
  scope_kind = sqlc.arg(scope_kind)
  AND scope_name = sqlc.arg(scope_name)
  AND holder_token = sqlc.arg(holder_token)
RETURNING lease_expires_at;

-- name: ReleaseSyncLockLease :exec
DELETE FROM sync_locks
WHERE
  scope_kind = sqlc.arg(scope_kind)
  AND scope_name = sqlc.arg(scope_name)
  AND holder_token = sqlc.arg(holder_token);

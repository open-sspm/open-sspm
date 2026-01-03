-- name: CreateSyncRun :one
INSERT INTO sync_runs (source_kind, source_name, status, started_at)
VALUES ($1, $2, 'running', now())
RETURNING id;

-- name: GetLatestFinishedSyncRunBySource :one
SELECT id, status, finished_at, error_kind
FROM sync_runs
WHERE source_kind = $1
  AND source_name = $2
  AND finished_at IS NOT NULL
ORDER BY finished_at DESC
LIMIT 1;

-- name: ListRecentFinishedSyncRunsBySource :many
SELECT id, status, finished_at, error_kind
FROM sync_runs
WHERE source_kind = $1
  AND source_name = $2
  AND finished_at IS NOT NULL
ORDER BY finished_at DESC
LIMIT $3;

-- name: FailSyncRun :exec
UPDATE sync_runs
SET status = $2, finished_at = now(), message = $3, error_kind = $4
WHERE id = $1;

-- name: MarkSyncRunSuccess :exec
UPDATE sync_runs
SET status = 'success', finished_at = now(), message = '', stats = $2, error_kind = ''
WHERE id = $1;

-- name: AcquireAdvisoryLock :exec
SELECT pg_advisory_lock($1::bigint);

-- name: TryAcquireAdvisoryLock :one
SELECT pg_try_advisory_lock($1::bigint);

-- name: ReleaseAdvisoryLock :exec
SELECT pg_advisory_unlock($1::bigint);

-- name: NotifyResyncRequested :exec
SELECT pg_notify('open_sspm_resync_requested', '');

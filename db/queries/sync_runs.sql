-- name: CreateSyncRun :one
INSERT INTO sync_runs (source_kind, source_name, status, started_at)
VALUES ($1, $2, 'running', now())
RETURNING id;

-- name: ListRecentFinishedSyncRunsBySource :many
SELECT id, status, finished_at, error_kind
FROM sync_runs
WHERE source_kind = $1
  AND source_name = $2
  AND finished_at IS NOT NULL
ORDER BY finished_at DESC
LIMIT $3;

-- name: ListRecentFinishedSyncRunsForSources :many
WITH requested AS (
  SELECT k.kind AS source_kind, n.name AS source_name
  FROM unnest(sqlc.arg(source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
),
ranked AS (
  SELECT
    r.source_kind,
    r.source_name,
    r.id,
    r.status,
    r.finished_at,
    r.error_kind,
    row_number() OVER (PARTITION BY r.source_kind, r.source_name ORDER BY r.finished_at DESC) AS rn
  FROM sync_runs r
  JOIN requested q
    ON r.source_kind = q.source_kind
   AND r.source_name = q.source_name
  WHERE r.finished_at IS NOT NULL
)
SELECT source_kind, source_name, id, status, finished_at, error_kind
FROM ranked
WHERE rn <= sqlc.arg(limit_rows)::int
ORDER BY source_kind, source_name, finished_at DESC;

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

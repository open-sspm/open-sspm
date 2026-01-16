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

-- name: GetSyncRunRollupsForSources :many
WITH requested AS (
  SELECT k.kind AS source_kind, n.name AS source_name
  FROM unnest(sqlc.arg(source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
),
last_run AS (
  SELECT DISTINCT ON (r.source_kind, r.source_name)
    r.source_kind,
    r.source_name,
    r.id AS last_run_id,
    r.status AS last_run_status,
    r.started_at AS last_run_started_at,
    r.finished_at AS last_run_finished_at,
    r.error_kind AS last_run_error_kind
  FROM sync_runs r
  JOIN requested q
    ON r.source_kind = q.source_kind
   AND r.source_name = q.source_name
  WHERE r.finished_at IS NOT NULL
  ORDER BY r.source_kind, r.source_name, r.finished_at DESC
),
last_success AS (
  SELECT
    r.source_kind,
    r.source_name,
    max(r.finished_at) AS last_success_at
  FROM sync_runs r
  JOIN requested q
    ON r.source_kind = q.source_kind
   AND r.source_name = q.source_name
  WHERE r.finished_at IS NOT NULL
    AND r.status = 'success'
  GROUP BY r.source_kind, r.source_name
),
stats_7d AS (
  SELECT
    r.source_kind,
    r.source_name,
    count(*) FILTER (WHERE r.finished_at >= now() - interval '7 days') AS finished_count_7d,
    count(*) FILTER (WHERE r.finished_at >= now() - interval '7 days' AND r.status = 'success') AS success_count_7d,
    avg(EXTRACT(EPOCH FROM (r.finished_at - r.started_at)) * 1000.0)
      FILTER (WHERE r.finished_at >= now() - interval '7 days' AND r.status = 'success') AS avg_success_duration_ms_7d
  FROM sync_runs r
  JOIN requested q
    ON r.source_kind = q.source_kind
   AND r.source_name = q.source_name
  WHERE r.finished_at IS NOT NULL
    AND r.finished_at >= now() - interval '7 days'
  GROUP BY r.source_kind, r.source_name
)
SELECT
  q.source_kind::text AS source_kind,
  q.source_name::text AS source_name,
  lr.last_run_id,
  lr.last_run_status,
  lr.last_run_started_at,
  lr.last_run_finished_at,
  lr.last_run_error_kind,
  ls.last_success_at::timestamptz AS last_success_at,
  COALESCE(s.finished_count_7d, 0) AS finished_count_7d,
  COALESCE(s.success_count_7d, 0) AS success_count_7d,
  s.avg_success_duration_ms_7d
FROM requested q
LEFT JOIN last_run lr
  ON lr.source_kind = q.source_kind
 AND lr.source_name = q.source_name
LEFT JOIN last_success ls
  ON ls.source_kind = q.source_kind
 AND ls.source_name = q.source_name
LEFT JOIN stats_7d s
  ON s.source_kind = q.source_kind
 AND s.source_name = q.source_name
ORDER BY q.source_kind, q.source_name;

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

-- name: CreateSyncJob :one
INSERT INTO sync_jobs (
  id,
  lane,
  connector_kind,
  source_name,
  trigger_kind,
  status,
  attempt_count,
  available_at,
  rerun_requested
) VALUES (
  sqlc.arg(id),
  sqlc.arg(lane),
  sqlc.narg(connector_kind),
  sqlc.narg(source_name),
  sqlc.arg(trigger_kind),
  sqlc.arg(status),
  sqlc.arg(attempt_count),
  sqlc.arg(available_at),
  sqlc.arg(rerun_requested)
)
RETURNING *;

-- name: GetActiveSyncJobByScope :one
SELECT *
FROM sync_jobs
WHERE
  lane = sqlc.arg(lane)
  AND COALESCE(connector_kind, '') = COALESCE(sqlc.narg(connector_kind), '')
  AND COALESCE(source_name, '') = COALESCE(sqlc.narg(source_name), '')
  AND status IN ('pending', 'claimed', 'running')
LIMIT 1;

-- name: PromoteSyncJobForManualRequest :one
UPDATE sync_jobs
SET
  trigger_kind = 'manual',
  available_at = CASE
    WHEN status IN ('pending', 'claimed') THEN clock_timestamp()
    ELSE available_at
  END,
  rerun_requested = CASE
    WHEN status = 'running' THEN true
    ELSE false
  END,
  updated_at = clock_timestamp()
WHERE
  id = sqlc.arg(id)
  AND status IN ('pending', 'claimed', 'running')
  AND trigger_kind = 'scheduled'
RETURNING *;

-- name: RequeueStaleSyncJobsByLane :execrows
UPDATE sync_jobs
SET
  status = 'pending',
  claimed_by = NULL,
  claimed_at = NULL,
  heartbeat_at = NULL,
  lease_expires_at = NULL,
  started_at = NULL,
  finished_at = NULL,
  updated_at = clock_timestamp()
WHERE
  lane = sqlc.arg(lane)
  AND status IN ('claimed', 'running')
  AND lease_expires_at IS NOT NULL
  AND lease_expires_at < clock_timestamp();

-- name: ClaimNextSyncJobByLane :one
WITH next_job AS (
  SELECT id
  FROM sync_jobs AS queue_job
  WHERE
    queue_job.lane = sqlc.arg(lane)
    AND queue_job.status = 'pending'
    AND queue_job.available_at <= clock_timestamp()
  ORDER BY
    CASE WHEN queue_job.trigger_kind = 'manual' THEN 0 ELSE 1 END,
    queue_job.priority DESC,
    queue_job.available_at ASC,
    queue_job.created_at ASC,
    queue_job.id ASC
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
UPDATE sync_jobs AS sj
SET
  status = 'claimed',
  claimed_by = sqlc.arg(claimed_by),
  claimed_at = clock_timestamp(),
  heartbeat_at = clock_timestamp(),
  lease_expires_at = clock_timestamp() + (sqlc.arg(lease_seconds)::bigint * interval '1 second'),
  attempt_count = sj.attempt_count + 1,
  updated_at = clock_timestamp()
FROM next_job
WHERE sj.id = next_job.id
RETURNING sj.*;

-- name: RenewSyncJobLease :one
UPDATE sync_jobs
SET
  heartbeat_at = clock_timestamp(),
  lease_expires_at = clock_timestamp() + (sqlc.arg(lease_seconds)::bigint * interval '1 second'),
  updated_at = clock_timestamp()
WHERE
  id = sqlc.arg(id)
  AND claimed_by = sqlc.arg(claimed_by)
  AND status IN ('claimed', 'running')
RETURNING lease_expires_at;

-- name: MarkSyncJobRunning :one
UPDATE sync_jobs
SET
  status = 'running',
  started_at = clock_timestamp(),
  updated_at = clock_timestamp()
WHERE
  id = sqlc.arg(id)
  AND claimed_by = sqlc.arg(claimed_by)
  AND status = 'claimed'
RETURNING *;

-- name: CompleteManualSyncJobSuccess :execrows
UPDATE sync_jobs
SET
  status = 'succeeded',
  rerun_requested = false,
  finished_at = clock_timestamp(),
  heartbeat_at = NULL,
  lease_expires_at = NULL,
  last_error = NULL,
  updated_at = clock_timestamp()
WHERE
  id = sqlc.arg(id)
  AND claimed_by = sqlc.arg(claimed_by)
  AND status IN ('claimed', 'running');

-- name: CompleteManualSyncJobFailure :execrows
UPDATE sync_jobs
SET
  status = 'failed',
  rerun_requested = false,
  finished_at = clock_timestamp(),
  heartbeat_at = NULL,
  lease_expires_at = NULL,
  last_error = sqlc.arg(last_error),
  updated_at = clock_timestamp()
WHERE
  id = sqlc.arg(id)
  AND claimed_by = sqlc.arg(claimed_by)
  AND status IN ('claimed', 'running');

-- name: CompleteScheduledSyncJobSuccess :execrows
UPDATE sync_jobs
SET
  status = CASE
    WHEN rerun_requested THEN 'pending'
    ELSE 'succeeded'
  END,
  trigger_kind = CASE
    WHEN rerun_requested THEN 'manual'
    ELSE 'scheduled'
  END,
  attempt_count = CASE
    WHEN rerun_requested THEN 0
    ELSE attempt_count
  END,
  rerun_requested = false,
  available_at = CASE
    WHEN rerun_requested THEN clock_timestamp()
    ELSE available_at
  END,
  claimed_by = NULL,
  claimed_at = NULL,
  heartbeat_at = NULL,
  lease_expires_at = NULL,
  started_at = CASE
    WHEN rerun_requested THEN NULL
    ELSE started_at
  END,
  finished_at = CASE
    WHEN rerun_requested THEN NULL
    ELSE clock_timestamp()
  END,
  last_error = NULL,
  updated_at = clock_timestamp()
WHERE
  id = sqlc.arg(id)
  AND claimed_by = sqlc.arg(claimed_by)
  AND status IN ('claimed', 'running');

-- name: CompleteScheduledSyncJobFailure :execrows
UPDATE sync_jobs
SET
  status = 'pending',
  trigger_kind = CASE
    WHEN rerun_requested THEN 'manual'
    ELSE 'scheduled'
  END,
  attempt_count = CASE
    WHEN rerun_requested THEN 0
    ELSE attempt_count
  END,
  rerun_requested = false,
  available_at = CASE
    WHEN rerun_requested THEN clock_timestamp()
    ELSE sqlc.arg(available_at)
  END,
  claimed_by = NULL,
  claimed_at = NULL,
  heartbeat_at = NULL,
  lease_expires_at = NULL,
  started_at = NULL,
  finished_at = NULL,
  last_error = sqlc.arg(last_error),
  updated_at = clock_timestamp()
WHERE
  id = sqlc.arg(id)
  AND claimed_by = sqlc.arg(claimed_by)
  AND status IN ('claimed', 'running');

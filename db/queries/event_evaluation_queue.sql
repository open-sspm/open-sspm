-- name: EnqueueEventEvaluation :exec
INSERT INTO event_evaluation_queue (
  event_received_at,
  event_id,
  status,
  available_at,
  updated_at
) VALUES (
  sqlc.arg(event_received_at)::timestamptz,
  sqlc.arg(event_id)::uuid,
  'queued',
  now(),
  now()
)
ON CONFLICT (event_received_at, event_id) DO NOTHING;

-- name: ClaimEventEvaluations :many
WITH next_jobs AS (
  SELECT id
  FROM event_evaluation_queue
  WHERE status = 'queued'
    AND available_at <= clock_timestamp()
  ORDER BY available_at ASC, id ASC
  LIMIT sqlc.arg(limit_rows)::int
  FOR UPDATE SKIP LOCKED
)
UPDATE event_evaluation_queue q
SET
  status = 'processing',
  attempts = q.attempts + 1,
  claimed_by = sqlc.arg(claimed_by)::text,
  claimed_at = clock_timestamp(),
  lease_until = clock_timestamp() + (sqlc.arg(lease_seconds)::bigint * interval '1 second'),
  updated_at = clock_timestamp()
FROM next_jobs
WHERE q.id = next_jobs.id
RETURNING q.*;

-- name: RenewEventEvaluationLease :execrows
UPDATE event_evaluation_queue
SET
  lease_until = clock_timestamp() + (sqlc.arg(lease_seconds)::bigint * interval '1 second'),
  updated_at = clock_timestamp()
WHERE id = ANY(sqlc.arg(ids)::bigint[])
  AND status = 'processing'
  AND claimed_by = sqlc.arg(claimed_by)::text;

-- name: RequeueStaleEventEvaluations :execrows
UPDATE event_evaluation_queue
SET
  status = 'queued',
  claimed_by = NULL,
  claimed_at = NULL,
  lease_until = NULL,
  updated_at = clock_timestamp()
WHERE status = 'processing'
  AND lease_until IS NOT NULL
  AND lease_until < clock_timestamp();

-- name: MarkEventEvaluationProcessed :execrows
UPDATE event_evaluation_queue
SET
  status = 'processed',
  claimed_by = NULL,
  claimed_at = NULL,
  lease_until = NULL,
  last_error = '',
  updated_at = clock_timestamp()
WHERE id = sqlc.arg(id)::bigint
  AND claimed_by = sqlc.arg(claimed_by)::text
  AND status = 'processing'
  AND lease_until > clock_timestamp();

-- name: MarkEventEvaluationRetry :execrows
UPDATE event_evaluation_queue
SET
  status = 'queued',
  available_at = sqlc.arg(available_at)::timestamptz,
  claimed_by = NULL,
  claimed_at = NULL,
  lease_until = NULL,
  last_error = sqlc.arg(last_error)::text,
  updated_at = clock_timestamp()
WHERE id = sqlc.arg(id)::bigint
  AND claimed_by = sqlc.arg(claimed_by)::text
  AND status = 'processing'
  AND lease_until > clock_timestamp();

-- name: MarkEventEvaluationDead :execrows
UPDATE event_evaluation_queue
SET
  status = 'dead',
  claimed_by = NULL,
  claimed_at = NULL,
  lease_until = NULL,
  last_error = sqlc.arg(last_error)::text,
  updated_at = clock_timestamp()
WHERE id = sqlc.arg(id)::bigint
  AND claimed_by = sqlc.arg(claimed_by)::text
  AND status = 'processing'
  AND lease_until > clock_timestamp();

-- name: GetEventForEvaluation :one
SELECT *
FROM events
WHERE received_at = sqlc.arg(received_at)::timestamptz
  AND id = sqlc.arg(id)::uuid;

-- name: DeleteFinishedEventEvaluationQueueBefore :execrows
DELETE FROM event_evaluation_queue
WHERE status IN ('processed', 'dead')
  AND updated_at < sqlc.arg(cutoff)::timestamptz;

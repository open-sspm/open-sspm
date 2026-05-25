-- name: EnqueueRiskpolicyEventEvaluation :exec
INSERT INTO riskpolicy_event_queue (
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

-- name: ClaimRiskpolicyEventEvaluations :many
WITH next_jobs AS (
  SELECT id
  FROM riskpolicy_event_queue
  WHERE status = 'queued'
    AND available_at <= clock_timestamp()
  ORDER BY available_at ASC, id ASC
  LIMIT sqlc.arg(limit_rows)::int
  FOR UPDATE SKIP LOCKED
)
UPDATE riskpolicy_event_queue q
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

-- name: RequeueStaleRiskpolicyEventEvaluations :execrows
UPDATE riskpolicy_event_queue
SET
  status = 'queued',
  claimed_by = NULL,
  claimed_at = NULL,
  lease_until = NULL,
  updated_at = clock_timestamp()
WHERE status = 'processing'
  AND lease_until IS NOT NULL
  AND lease_until < clock_timestamp();

-- name: MarkRiskpolicyEventEvaluationProcessed :execrows
UPDATE riskpolicy_event_queue
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

-- name: MarkRiskpolicyEventEvaluationRetry :execrows
UPDATE riskpolicy_event_queue
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

-- name: MarkRiskpolicyEventEvaluationDead :execrows
UPDATE riskpolicy_event_queue
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

-- name: GetEventForRiskpolicyEvaluation :one
SELECT *
FROM events
WHERE received_at = sqlc.arg(received_at)::timestamptz
  AND id = sqlc.arg(id)::uuid;

-- name: UpsertRiskpolicyEventShadowSignal :exec
INSERT INTO riskpolicy_event_shadow_signals (
  event_received_at,
  event_id,
  signal_id,
  policy_pack_id,
  policy_pack_version,
  severity,
  title,
  evidence,
  output,
  evaluated_at
) VALUES (
  sqlc.arg(event_received_at)::timestamptz,
  sqlc.arg(event_id)::uuid,
  sqlc.arg(signal_id)::text,
  sqlc.arg(policy_pack_id)::text,
  sqlc.arg(policy_pack_version)::text,
  sqlc.arg(severity)::text,
  sqlc.arg(title)::text,
  sqlc.arg(evidence)::text,
  sqlc.arg(output)::jsonb,
  now()
)
ON CONFLICT (event_received_at, event_id, signal_id) DO UPDATE SET
  policy_pack_id = EXCLUDED.policy_pack_id,
  policy_pack_version = EXCLUDED.policy_pack_version,
  severity = EXCLUDED.severity,
  title = EXCLUDED.title,
  evidence = EXCLUDED.evidence,
  output = EXCLUDED.output,
  evaluated_at = now();

-- name: DeleteFinishedRiskpolicyEventQueueBefore :execrows
DELETE FROM riskpolicy_event_queue
WHERE status IN ('processed', 'dead')
  AND updated_at < sqlc.arg(cutoff)::timestamptz;

-- name: DeleteRiskpolicyEventShadowSignalsBefore :execrows
DELETE FROM riskpolicy_event_shadow_signals
WHERE evaluated_at < sqlc.arg(cutoff)::timestamptz;

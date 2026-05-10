-- name: UpsertOktaPushInboxEventsBulk :many
WITH input AS (
  SELECT
    i,
    sqlc.arg(source_name)::text AS source_name,
    sqlc.arg(channel)::text AS channel,
    (sqlc.arg(delivery_external_ids)::text[])[i] AS delivery_external_id,
    (sqlc.arg(event_external_ids)::text[])[i] AS event_external_id,
    (sqlc.arg(event_types)::text[])[i] AS event_type,
    (sqlc.arg(event_indexes)::int[])[i] AS event_index,
    (sqlc.arg(published_ats)::timestamptz[])[i] AS published_at,
    (sqlc.arg(raw_jsons)::jsonb[])[i] AS raw_json
  FROM generate_subscripts(sqlc.arg(event_external_ids)::text[], 1) AS s(i)
),
dedup AS (
  SELECT DISTINCT ON (source_name, event_external_id)
    source_name,
    channel,
    COALESCE(delivery_external_id, '') AS delivery_external_id,
    event_external_id,
    COALESCE(event_type, '') AS event_type,
    COALESCE(event_index, 0) AS event_index,
    published_at,
    COALESCE(raw_json, '{}'::jsonb) AS raw_json,
    i
  FROM input
  WHERE trim(COALESCE(event_external_id, '')) <> ''
  ORDER BY source_name, event_external_id, i DESC
)
INSERT INTO okta_push_inbox (
  source_name,
  channel,
  delivery_external_id,
  event_external_id,
  event_type,
  event_index,
  published_at,
  raw_json,
  last_received_at,
  updated_at
)
SELECT
  source_name,
  channel,
  delivery_external_id,
  event_external_id,
  event_type,
  event_index,
  published_at,
  raw_json,
  now(),
  now()
FROM dedup
ON CONFLICT (source_name, event_external_id) DO UPDATE SET
  channel = EXCLUDED.channel,
  delivery_external_id = CASE
    WHEN trim(EXCLUDED.delivery_external_id) <> '' THEN EXCLUDED.delivery_external_id
    ELSE okta_push_inbox.delivery_external_id
  END,
  event_type = CASE
    WHEN trim(EXCLUDED.event_type) <> '' THEN EXCLUDED.event_type
    ELSE okta_push_inbox.event_type
  END,
  event_index = EXCLUDED.event_index,
  published_at = COALESCE(EXCLUDED.published_at, okta_push_inbox.published_at),
  raw_json = EXCLUDED.raw_json,
  last_received_at = now(),
  updated_at = CASE
    WHEN okta_push_inbox.status = 'processing' THEN okta_push_inbox.updated_at
    ELSE now()
  END
RETURNING id;

-- name: ClaimQueuedOktaPushInboxEvents :many
WITH candidates AS (
  SELECT id
  FROM okta_push_inbox
  WHERE status = 'queued'
    AND (next_attempt_at IS NULL OR next_attempt_at <= now())
  ORDER BY id
  LIMIT sqlc.arg(limit_rows)::int
  FOR UPDATE SKIP LOCKED
)
UPDATE okta_push_inbox i
SET status = 'processing',
    attempts = attempts + 1,
    updated_at = now()
FROM candidates c
WHERE i.id = c.id
RETURNING i.*;

-- name: ClaimQueuedOktaPushInboxEventsByIDs :many
WITH requested AS (
  SELECT DISTINCT unnest(sqlc.arg(ids)::bigint[]) AS id
),
candidates AS (
  SELECT i.id
  FROM okta_push_inbox i
  JOIN requested r ON r.id = i.id
  WHERE i.status = 'queued'
    AND (i.next_attempt_at IS NULL OR i.next_attempt_at <= now())
  ORDER BY i.id
  LIMIT sqlc.arg(limit_rows)::int
  FOR UPDATE SKIP LOCKED
)
UPDATE okta_push_inbox i
SET status = 'processing',
    attempts = attempts + 1,
    updated_at = now()
FROM candidates c
WHERE i.id = c.id
RETURNING i.*;

-- name: RequeueStaleOktaPushInboxProcessingRows :execrows
UPDATE okta_push_inbox
SET status = 'queued',
    next_attempt_at = now(),
    error_message = CASE
      WHEN trim(error_message) <> '' THEN error_message
      ELSE 'processing attempt timed out'
    END,
    updated_at = now()
WHERE status = 'processing'
  AND updated_at < now() - make_interval(secs => sqlc.arg(stale_after_seconds)::int);

-- name: MarkOktaPushInboxProcessed :execrows
UPDATE okta_push_inbox
SET status = 'processed',
    processed_run_id = sqlc.arg(processed_run_id)::bigint,
    processed_at = now(),
    next_attempt_at = NULL,
    error_message = '',
    updated_at = now()
WHERE id = ANY(sqlc.arg(ids)::bigint[]);

-- name: MarkOktaPushInboxIgnored :execrows
UPDATE okta_push_inbox
SET status = 'ignored',
    processed_run_id = sqlc.narg(processed_run_id)::bigint,
    processed_at = now(),
    next_attempt_at = NULL,
    error_message = sqlc.arg(error_message)::text,
    updated_at = now()
WHERE id = ANY(sqlc.arg(ids)::bigint[]);

-- name: MarkOktaPushInboxRetry :execrows
UPDATE okta_push_inbox
SET status = 'queued',
    next_attempt_at = sqlc.arg(next_attempt_at)::timestamptz,
    error_message = sqlc.arg(error_message)::text,
    updated_at = now()
WHERE id = ANY(sqlc.arg(ids)::bigint[]);

-- name: MarkOktaPushInboxDeadLetter :execrows
UPDATE okta_push_inbox
SET status = 'dead_letter',
    processed_at = now(),
    next_attempt_at = NULL,
    error_message = sqlc.arg(error_message)::text,
    updated_at = now()
WHERE id = ANY(sqlc.arg(ids)::bigint[]);

-- name: GetOktaPushIngestStatusBySource :one
SELECT
  source_name,
  count(*) FILTER (WHERE status = 'queued')::bigint AS queued_count,
  count(*) FILTER (WHERE status = 'processing')::bigint AS processing_count,
  count(*) FILTER (WHERE status = 'dead_letter')::bigint AS dead_letter_count,
  max(last_received_at)::timestamptz AS last_received_at,
  max(processed_at)::timestamptz AS last_processed_at,
  max(published_at)::timestamptz AS last_published_at,
  COALESCE(
    (
      SELECT error_message
      FROM okta_push_inbox latest_error
      WHERE latest_error.source_name = sqlc.arg(source_name)::text
        AND latest_error.status = 'dead_letter'
        AND trim(latest_error.error_message) <> ''
      ORDER BY latest_error.updated_at DESC, latest_error.id DESC
      LIMIT 1
    ),
    ''
  )::text AS last_dead_letter_error
FROM okta_push_inbox
WHERE source_name = sqlc.arg(source_name)::text
GROUP BY source_name;

-- name: CountOktaPushInboxByStatus :many
SELECT source_name, channel, status, count(*)::bigint AS row_count
FROM okta_push_inbox
GROUP BY source_name, channel, status
ORDER BY source_name, channel, status;

-- name: ListOktaPushInboxMetricsBySourceChannel :many
SELECT
  source_name,
  channel,
  count(*) FILTER (WHERE status = 'queued')::bigint AS queued_count,
  count(*) FILTER (WHERE status = 'processing')::bigint AS processing_count,
  count(*) FILTER (WHERE status = 'dead_letter')::bigint AS dead_letter_count,
  max(last_received_at)::timestamptz AS last_received_at,
  max(processed_at)::timestamptz AS last_processed_at,
  max(published_at)::timestamptz AS last_published_at
FROM okta_push_inbox
GROUP BY source_name, channel
ORDER BY source_name, channel;

-- name: DeleteOldOktaPushInboxRows :execrows
DELETE FROM okta_push_inbox
WHERE (
    status IN ('processed', 'ignored')
    AND processed_at < now() - make_interval(days => sqlc.arg(processed_retention_days)::int)
  )
  OR (
    status = 'dead_letter'
    AND updated_at < now() - make_interval(days => sqlc.arg(dead_letter_retention_days)::int)
  );

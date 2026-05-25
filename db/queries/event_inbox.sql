-- name: UpsertEventInboxDelivery :one
INSERT INTO event_inbox (
  source_kind,
  source_id,
  source_name,
  channel,
  external_event_id,
  dedupe_key,
  dedupe_hash,
  payload_hash,
  headers,
  query,
  raw_body,
  decoded_summary,
  trace_id
)
VALUES (
  sqlc.arg(source_kind)::text,
  sqlc.narg(source_id)::bigint,
  sqlc.arg(source_name)::text,
  sqlc.arg(channel)::text,
  sqlc.arg(external_event_id)::text,
  sqlc.arg(dedupe_key)::text,
  sqlc.arg(dedupe_hash)::bytea,
  sqlc.arg(payload_hash)::bytea,
  sqlc.arg(headers)::jsonb,
  sqlc.arg(query)::jsonb,
  sqlc.arg(raw_body)::bytea,
  sqlc.arg(decoded_summary)::jsonb,
  sqlc.arg(trace_id)::text
)
ON CONFLICT (source_kind, source_name, channel, dedupe_hash) DO UPDATE SET
  received_at = event_inbox.received_at
RETURNING id, status::text AS status;

-- name: ClaimQueuedEventInboxDeliveries :many
WITH next_rows AS (
  SELECT id
  FROM event_inbox
  WHERE status = 'queued'
    AND available_at <= now()
  ORDER BY available_at, id
  LIMIT sqlc.arg(limit_rows)::int
  FOR UPDATE SKIP LOCKED
)
UPDATE event_inbox i
SET status = 'processing',
    lease_owner = sqlc.arg(lease_owner)::text,
    lease_until = now() + (sqlc.arg(lease_seconds)::bigint * interval '1 second'),
    processing_started_at = now(),
    attempt_count = attempt_count + 1,
    last_error = '',
    updated_at = now()
FROM next_rows
WHERE i.id = next_rows.id
RETURNING
  i.id,
  i.source_kind,
  i.source_id,
  i.source_name,
  i.channel,
  i.external_event_id,
  i.dedupe_key,
  i.dedupe_hash,
  i.payload_hash,
  i.status::text AS status,
  i.received_at,
  i.available_at,
  i.processing_started_at,
  i.processed_at,
  i.updated_at,
  i.attempt_count,
  i.max_attempts,
  i.lease_owner,
  i.lease_until,
  i.headers,
  i.query,
  i.raw_body,
  i.decoded_summary,
  i.ignore_reason,
  i.last_error,
  i.trace_id;

-- name: RenewEventInboxLease :execrows
UPDATE event_inbox
SET lease_until = now() + (sqlc.arg(lease_seconds)::bigint * interval '1 second')
WHERE id = ANY(sqlc.arg(ids)::bigint[])
  AND status = 'processing'
  AND lease_owner = sqlc.arg(lease_owner)::text
  AND lease_until > now();

-- name: MarkEventInboxProcessed :execrows
UPDATE event_inbox
SET status = 'processed',
    processed_at = now(),
    updated_at = now(),
    lease_owner = NULL,
    lease_until = NULL,
    last_error = '',
    ignore_reason = '',
    decoded_summary = sqlc.arg(decoded_summary)::jsonb
WHERE id = ANY(sqlc.arg(ids)::bigint[])
  AND status = 'processing'
  AND lease_owner = sqlc.arg(lease_owner)::text
  AND lease_until > now();

-- name: MarkEventInboxIgnored :execrows
UPDATE event_inbox
SET status = 'ignored',
    processed_at = now(),
    updated_at = now(),
    lease_owner = NULL,
    lease_until = NULL,
    ignore_reason = sqlc.arg(ignore_reason)::text,
    last_error = '',
    decoded_summary = sqlc.arg(decoded_summary)::jsonb
WHERE id = ANY(sqlc.arg(ids)::bigint[])
  AND status = 'processing'
  AND lease_owner = sqlc.arg(lease_owner)::text
  AND lease_until > now();

-- name: MarkEventInboxRetry :execrows
UPDATE event_inbox
SET status = 'queued',
    available_at = sqlc.arg(available_at)::timestamptz,
    updated_at = now(),
    lease_owner = NULL,
    lease_until = NULL,
    last_error = sqlc.arg(last_error)::text
WHERE id = ANY(sqlc.arg(ids)::bigint[])
  AND status = 'processing'
  AND lease_owner = sqlc.arg(lease_owner)::text
  AND lease_until > now();

-- name: MarkEventInboxDead :execrows
UPDATE event_inbox
SET status = 'dead',
    processed_at = now(),
    updated_at = now(),
    lease_owner = NULL,
    lease_until = NULL,
    last_error = sqlc.arg(last_error)::text
WHERE id = ANY(sqlc.arg(ids)::bigint[])
  AND status = 'processing'
  AND lease_owner = sqlc.arg(lease_owner)::text
  AND lease_until > now();

-- name: RequeueExpiredEventInboxLeases :execrows
UPDATE event_inbox
SET status = 'queued',
    available_at = now(),
    updated_at = now(),
    lease_owner = NULL,
    lease_until = NULL,
    last_error = CASE
      WHEN trim(last_error) <> '' THEN last_error
      ELSE 'processing lease expired'
    END
WHERE status = 'processing'
  AND lease_until < now();

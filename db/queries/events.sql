-- name: InsertEventDedupeKey :one
INSERT INTO event_dedupe_keys (
  source_kind,
  source_id,
  source_name,
  dedupe_hash,
  dedupe_key,
  event_received_at,
  event_id
)
VALUES (
  sqlc.arg(source_kind)::text,
  sqlc.narg(source_id)::bigint,
  sqlc.arg(source_name)::text,
  sqlc.arg(dedupe_hash)::bytea,
  sqlc.arg(dedupe_key)::text,
  sqlc.arg(event_received_at)::timestamptz,
  sqlc.arg(event_id)::uuid
)
ON CONFLICT (source_kind, source_name, dedupe_hash) DO UPDATE SET
  last_seen_at = now(),
  duplicate_count = event_dedupe_keys.duplicate_count + 1
RETURNING event_id = sqlc.arg(event_id)::uuid AS inserted, event_id, event_received_at;

-- name: InsertEvent :exec
INSERT INTO events (
  id,
  received_at,
  occurred_at,
  observed_at,
  ingest_id,
  source_kind,
  source_id,
  source_name,
  channel,
  provider_event_id,
  dedupe_key,
  dedupe_hash,
  event_type,
  category,
  action,
  severity,
  actor_kind,
  actor_id,
  actor_email,
  actor_display_name,
  target_kind,
  target_id,
  target_name,
  target_ref,
  outcome,
  ip,
  user_agent,
  identity_id,
  saas_app_id,
  envelope,
  raw,
  trace_id
)
VALUES (
  sqlc.arg(id)::uuid,
  sqlc.arg(received_at)::timestamptz,
  sqlc.arg(occurred_at)::timestamptz,
  sqlc.narg(observed_at)::timestamptz,
  sqlc.narg(ingest_id)::bigint,
  sqlc.arg(source_kind)::text,
  sqlc.narg(source_id)::bigint,
  sqlc.arg(source_name)::text,
  sqlc.arg(channel)::text,
  sqlc.arg(provider_event_id)::text,
  sqlc.arg(dedupe_key)::text,
  sqlc.arg(dedupe_hash)::bytea,
  sqlc.arg(event_type)::text,
  sqlc.arg(category)::text,
  sqlc.arg(action)::text,
  sqlc.arg(severity)::smallint,
  sqlc.arg(actor_kind)::text,
  sqlc.arg(actor_id)::text,
  sqlc.arg(actor_email)::text,
  sqlc.arg(actor_display_name)::text,
  sqlc.arg(target_kind)::text,
  sqlc.arg(target_id)::text,
  sqlc.arg(target_name)::text,
  sqlc.arg(target_ref)::jsonb,
  sqlc.arg(outcome)::text,
  sqlc.narg(ip)::inet,
  sqlc.arg(user_agent)::text,
  sqlc.narg(identity_id)::bigint,
  sqlc.narg(saas_app_id)::bigint,
  sqlc.arg(envelope)::jsonb,
  sqlc.arg(raw)::jsonb,
  sqlc.arg(trace_id)::text
);

-- name: InsertEventTarget :exec
INSERT INTO event_targets (
  event_received_at,
  event_id,
  ordinal,
  role,
  target_kind,
  target_id,
  target_name,
  target_email,
  identity_id,
  saas_app_id,
  envelope
)
VALUES (
  sqlc.arg(event_received_at)::timestamptz,
  sqlc.arg(event_id)::uuid,
  sqlc.arg(ordinal)::int,
  sqlc.arg(role)::text,
  sqlc.arg(target_kind)::text,
  sqlc.arg(target_id)::text,
  sqlc.arg(target_name)::text,
  sqlc.arg(target_email)::text,
  sqlc.narg(identity_id)::bigint,
  sqlc.narg(saas_app_id)::bigint,
  sqlc.arg(envelope)::jsonb
);

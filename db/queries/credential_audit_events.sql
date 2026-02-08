-- name: UpsertCredentialAuditEventsBulkBySource :execrows
WITH input AS (
  SELECT
    i,
    (sqlc.arg(event_external_ids)::text[])[i] AS event_external_id,
    (sqlc.arg(event_types)::text[])[i] AS event_type,
    (sqlc.arg(event_times)::timestamptz[])[i] AS event_time,
    (sqlc.arg(actor_kinds)::text[])[i] AS actor_kind,
    (sqlc.arg(actor_external_ids)::text[])[i] AS actor_external_id,
    (sqlc.arg(actor_display_names)::text[])[i] AS actor_display_name,
    (sqlc.arg(target_kinds)::text[])[i] AS target_kind,
    (sqlc.arg(target_external_ids)::text[])[i] AS target_external_id,
    (sqlc.arg(target_display_names)::text[])[i] AS target_display_name,
    (sqlc.arg(credential_kinds)::text[])[i] AS credential_kind,
    (sqlc.arg(credential_external_ids)::text[])[i] AS credential_external_id,
    (sqlc.arg(raw_jsons)::jsonb[])[i] AS raw_json
  FROM generate_subscripts(sqlc.arg(event_external_ids)::text[], 1) AS s(i)
),
dedup AS (
  SELECT DISTINCT ON (event_external_id)
    event_external_id,
    event_type,
    event_time,
    actor_kind,
    actor_external_id,
    actor_display_name,
    target_kind,
    target_external_id,
    target_display_name,
    credential_kind,
    credential_external_id,
    raw_json
  FROM input
  ORDER BY event_external_id, i DESC
)
INSERT INTO credential_audit_events (
  source_kind,
  source_name,
  event_external_id,
  event_type,
  event_time,
  actor_kind,
  actor_external_id,
  actor_display_name,
  target_kind,
  target_external_id,
  target_display_name,
  credential_kind,
  credential_external_id,
  raw_json
)
SELECT
  sqlc.arg(source_kind)::text,
  sqlc.arg(source_name)::text,
  input.event_external_id,
  input.event_type,
  input.event_time,
  input.actor_kind,
  input.actor_external_id,
  input.actor_display_name,
  input.target_kind,
  input.target_external_id,
  input.target_display_name,
  input.credential_kind,
  input.credential_external_id,
  input.raw_json
FROM dedup input
ON CONFLICT (source_kind, source_name, event_external_id) DO UPDATE SET
  event_type = EXCLUDED.event_type,
  event_time = EXCLUDED.event_time,
  actor_kind = EXCLUDED.actor_kind,
  actor_external_id = EXCLUDED.actor_external_id,
  actor_display_name = EXCLUDED.actor_display_name,
  target_kind = EXCLUDED.target_kind,
  target_external_id = EXCLUDED.target_external_id,
  target_display_name = EXCLUDED.target_display_name,
  credential_kind = EXCLUDED.credential_kind,
  credential_external_id = EXCLUDED.credential_external_id,
  raw_json = EXCLUDED.raw_json;

-- name: CountCredentialAuditEventsBySourceAndQuery :one
SELECT count(*)
FROM credential_audit_events cae
WHERE cae.source_kind = sqlc.arg(source_kind)::text
  AND cae.source_name = sqlc.arg(source_name)::text
  AND (
    sqlc.arg(query)::text = ''
    OR cae.event_type ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR cae.actor_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR cae.actor_display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR cae.target_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR cae.target_display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR cae.credential_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
  );

-- name: ListCredentialAuditEventsPageBySourceAndQuery :many
SELECT cae.*
FROM credential_audit_events cae
WHERE cae.source_kind = sqlc.arg(source_kind)::text
  AND cae.source_name = sqlc.arg(source_name)::text
  AND (
    sqlc.arg(query)::text = ''
    OR cae.event_type ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR cae.actor_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR cae.actor_display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR cae.target_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR cae.target_display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR cae.credential_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
ORDER BY cae.event_time DESC, cae.id DESC
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: ListCredentialAuditEventsForCredential :many
SELECT cae.*
FROM credential_audit_events cae
WHERE cae.source_kind = sqlc.arg(source_kind)::text
  AND cae.source_name = sqlc.arg(source_name)::text
  AND cae.credential_kind = sqlc.arg(credential_kind)::text
  AND cae.credential_external_id = sqlc.arg(credential_external_id)::text
ORDER BY cae.event_time DESC, cae.id DESC
LIMIT sqlc.arg(limit_rows)::int;

-- name: ListCredentialAuditEventsForTarget :many
SELECT cae.*
FROM credential_audit_events cae
WHERE cae.source_kind = sqlc.arg(source_kind)::text
  AND cae.source_name = sqlc.arg(source_name)::text
  AND cae.target_kind = sqlc.arg(target_kind)::text
  AND cae.target_external_id = sqlc.arg(target_external_id)::text
ORDER BY cae.event_time DESC, cae.id DESC
LIMIT sqlc.arg(limit_rows)::int;

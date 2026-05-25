-- name: ListCanonicalDiscoveryEventProjections :many
SELECT
  e.received_at::timestamptz AS event_received_at,
  e.id AS event_id,
  e.source_kind::text AS source_kind,
  e.source_name::text AS source_name,
  substring(e.category from length('discovery.') + 1)::text AS signal_kind,
  e.provider_event_id::text AS event_external_id,
  COALESCE(NULLIF(trim(et.target_id), ''), e.target_id, '')::text AS source_app_id,
  COALESCE(NULLIF(trim(et.target_name), ''), e.target_name, '')::text AS source_app_name,
  COALESCE(NULLIF(trim(et.envelope->>'domain'), ''), '')::text AS source_app_domain,
  e.actor_id::text AS actor_external_id,
  lower(trim(e.actor_email))::text AS actor_email,
  e.actor_display_name::text AS actor_display_name,
  e.occurred_at::timestamptz AS observed_at
FROM events e
LEFT JOIN LATERAL (
  SELECT target_id, target_name, envelope
  FROM event_targets
  WHERE event_received_at = e.received_at
    AND event_id = e.id
  ORDER BY ordinal ASC
  LIMIT 1
) et ON TRUE
WHERE e.source_kind = sqlc.arg(source_kind)::text
  AND e.source_name = sqlc.arg(source_name)::text
  AND e.category LIKE 'discovery.%'
  AND trim(e.provider_event_id) <> ''
  AND (
    sqlc.narg(after_event_received_at)::timestamptz IS NULL
    OR e.received_at > sqlc.narg(after_event_received_at)::timestamptz
    OR (
      e.received_at = sqlc.narg(after_event_received_at)::timestamptz
      AND (
        sqlc.narg(after_event_id)::uuid IS NULL
        OR e.id > sqlc.narg(after_event_id)::uuid
      )
    )
  )
  AND (sqlc.narg(window_start)::timestamptz IS NULL OR e.occurred_at >= sqlc.narg(window_start)::timestamptz)
  AND (sqlc.narg(window_end)::timestamptz IS NULL OR e.occurred_at < sqlc.narg(window_end)::timestamptz)
ORDER BY e.received_at ASC, e.id ASC
LIMIT sqlc.arg(limit_rows)::int;

-- name: UpsertShadowDiscoveryEventProjection :exec
INSERT INTO event_projection_shadow_discovery_events (
  projection_name,
  source_kind,
  source_name,
  signal_kind,
  event_external_id,
  source_app_id,
  source_app_name,
  source_app_domain,
  actor_external_id,
  actor_email,
  actor_display_name,
  observed_at,
  event_received_at,
  event_id,
  projected_at
) VALUES (
  sqlc.arg(projection_name)::text,
  sqlc.arg(source_kind)::text,
  sqlc.arg(source_name)::text,
  sqlc.arg(signal_kind)::text,
  sqlc.arg(event_external_id)::text,
  sqlc.arg(source_app_id)::text,
  sqlc.arg(source_app_name)::text,
  sqlc.arg(source_app_domain)::text,
  sqlc.arg(actor_external_id)::text,
  sqlc.arg(actor_email)::text,
  sqlc.arg(actor_display_name)::text,
  sqlc.arg(observed_at)::timestamptz,
  sqlc.arg(event_received_at)::timestamptz,
  sqlc.arg(event_id)::uuid,
  now()
)
ON CONFLICT (projection_name, source_kind, source_name, signal_kind, event_external_id) DO UPDATE SET
  source_app_id = EXCLUDED.source_app_id,
  source_app_name = EXCLUDED.source_app_name,
  source_app_domain = EXCLUDED.source_app_domain,
  actor_external_id = EXCLUDED.actor_external_id,
  actor_email = EXCLUDED.actor_email,
  actor_display_name = EXCLUDED.actor_display_name,
  observed_at = EXCLUDED.observed_at,
  event_received_at = EXCLUDED.event_received_at,
  event_id = EXCLUDED.event_id,
  projected_at = now();

-- name: GetEventProjectionCheckpoint :one
SELECT *
FROM event_projection_checkpoints
WHERE projection_name = sqlc.arg(projection_name)::text
  AND source_kind = sqlc.arg(source_kind)::text
  AND source_name = sqlc.arg(source_name)::text;

-- name: UpsertEventProjectionCheckpoint :exec
INSERT INTO event_projection_checkpoints (
  projection_name,
  source_kind,
  source_name,
  last_event_received_at,
  last_event_id,
  last_projected_at,
  stats,
  updated_at
) VALUES (
  sqlc.arg(projection_name)::text,
  sqlc.arg(source_kind)::text,
  sqlc.arg(source_name)::text,
  sqlc.narg(last_event_received_at)::timestamptz,
  sqlc.narg(last_event_id)::uuid,
  now(),
  sqlc.arg(stats)::jsonb,
  now()
)
ON CONFLICT (projection_name, source_kind, source_name) DO UPDATE SET
  last_event_received_at = EXCLUDED.last_event_received_at,
  last_event_id = EXCLUDED.last_event_id,
  last_projected_at = EXCLUDED.last_projected_at,
  stats = EXCLUDED.stats,
  updated_at = now();

-- name: ListShadowDiscoveryEventKeys :many
SELECT
  source_kind::text,
  source_name::text,
  signal_kind::text,
  event_external_id::text
FROM event_projection_shadow_discovery_events
WHERE projection_name = sqlc.arg(projection_name)::text
  AND source_kind = sqlc.arg(source_kind)::text
  AND source_name = sqlc.arg(source_name)::text
  AND (sqlc.narg(window_start)::timestamptz IS NULL OR observed_at >= sqlc.narg(window_start)::timestamptz)
  AND (sqlc.narg(window_end)::timestamptz IS NULL OR observed_at < sqlc.narg(window_end)::timestamptz)
ORDER BY signal_kind ASC, event_external_id ASC;

-- name: ListBaselineDiscoveryEventKeys :many
SELECT
  source_kind::text,
  source_name::text,
  signal_kind::text,
  event_external_id::text
FROM saas_app_events
WHERE source_kind = sqlc.arg(source_kind)::text
  AND source_name = sqlc.arg(source_name)::text
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
  AND (sqlc.narg(window_start)::timestamptz IS NULL OR observed_at >= sqlc.narg(window_start)::timestamptz)
  AND (sqlc.narg(window_end)::timestamptz IS NULL OR observed_at < sqlc.narg(window_end)::timestamptz)
ORDER BY signal_kind ASC, event_external_id ASC;

-- name: CreateEventProjectionDiffRun :one
INSERT INTO event_projection_diff_runs (
  projection_name,
  source_kind,
  source_name,
  window_start,
  window_end,
  baseline_count,
  projected_count,
  matching_count,
  missing_in_projection_count,
  missing_in_baseline_count,
  sample
) VALUES (
  sqlc.arg(projection_name)::text,
  sqlc.arg(source_kind)::text,
  sqlc.arg(source_name)::text,
  sqlc.narg(window_start)::timestamptz,
  sqlc.narg(window_end)::timestamptz,
  sqlc.arg(baseline_count)::bigint,
  sqlc.arg(projected_count)::bigint,
  sqlc.arg(matching_count)::bigint,
  sqlc.arg(missing_in_projection_count)::bigint,
  sqlc.arg(missing_in_baseline_count)::bigint,
  sqlc.arg(sample)::jsonb
)
RETURNING *;

-- name: DeleteEventProjectionDiffRunsBefore :execrows
DELETE FROM event_projection_diff_runs
WHERE created_at < sqlc.arg(cutoff)::timestamptz;

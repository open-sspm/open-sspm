-- name: UpsertSaaSAppEventsBulkBySource :execrows
WITH input AS (
  SELECT
    i,
    sqlc.arg(source_kind)::text AS source_kind,
    sqlc.arg(source_name)::text AS source_name,
    (sqlc.arg(canonical_keys)::text[])[i] AS canonical_key,
    (sqlc.arg(signal_kinds)::text[])[i] AS signal_kind,
    (sqlc.arg(event_external_ids)::text[])[i] AS event_external_id,
    (sqlc.arg(source_app_ids)::text[])[i] AS source_app_id,
    (sqlc.arg(source_app_names)::text[])[i] AS source_app_name,
    (sqlc.arg(source_app_domains)::text[])[i] AS source_app_domain,
    (sqlc.arg(actor_external_ids)::text[])[i] AS actor_external_id,
    (sqlc.arg(actor_emails)::text[])[i] AS actor_email,
    (sqlc.arg(actor_display_names)::text[])[i] AS actor_display_name,
    (sqlc.arg(observed_ats)::timestamptz[])[i] AS observed_at,
    (sqlc.arg(scopes_jsons)::jsonb[])[i] AS scopes_json,
    (sqlc.arg(raw_jsons)::jsonb[])[i] AS raw_json
  FROM generate_subscripts(sqlc.arg(event_external_ids)::text[], 1) AS s(i)
),
dedup AS (
  SELECT DISTINCT ON (source_kind, source_name, signal_kind, event_external_id)
    source_kind,
    source_name,
    canonical_key,
    signal_kind,
    event_external_id,
    source_app_id,
    source_app_name,
    source_app_domain,
    actor_external_id,
    actor_email,
    actor_display_name,
    observed_at,
    scopes_json,
    raw_json
  FROM input
  WHERE trim(event_external_id) <> ''
  ORDER BY source_kind, source_name, signal_kind, event_external_id, i DESC
)
INSERT INTO saas_app_events (
  saas_app_id,
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
  scopes_json,
  raw_json,
  seen_in_run_id,
  seen_at,
  updated_at
)
SELECT
  sa.id,
  d.source_kind,
  d.source_name,
  d.signal_kind,
  d.event_external_id,
  d.source_app_id,
  d.source_app_name,
  d.source_app_domain,
  d.actor_external_id,
  lower(trim(d.actor_email)),
  d.actor_display_name,
  d.observed_at,
  COALESCE(d.scopes_json, '[]'::jsonb),
  COALESCE(d.raw_json, '{}'::jsonb),
  sqlc.arg(seen_in_run_id)::bigint,
  now(),
  now()
FROM dedup d
JOIN saas_apps sa ON sa.canonical_key = d.canonical_key
ON CONFLICT (source_kind, source_name, signal_kind, event_external_id) DO UPDATE SET
  saas_app_id = EXCLUDED.saas_app_id,
  source_app_id = EXCLUDED.source_app_id,
  source_app_name = EXCLUDED.source_app_name,
  source_app_domain = EXCLUDED.source_app_domain,
  actor_external_id = EXCLUDED.actor_external_id,
  actor_email = EXCLUDED.actor_email,
  actor_display_name = EXCLUDED.actor_display_name,
  observed_at = EXCLUDED.observed_at,
  scopes_json = EXCLUDED.scopes_json,
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  updated_at = now();

-- name: PromoteSaaSAppEventsSeenInRunBySource :execrows
UPDATE saas_app_events
SET
  last_observed_run_id = sqlc.arg(last_observed_run_id)::bigint,
  last_observed_at = now(),
  expired_at = NULL,
  expired_run_id = NULL
WHERE source_kind = sqlc.arg(source_kind)::text
  AND source_name = sqlc.arg(source_name)::text
  AND seen_in_run_id = sqlc.arg(last_observed_run_id)::bigint;

-- name: ExpireSaaSAppEventsNotSeenInRunBySource :execrows
UPDATE saas_app_events
SET
  expired_at = now(),
  expired_run_id = sqlc.arg(expired_run_id)::bigint
WHERE source_kind = sqlc.arg(source_kind)::text
  AND source_name = sqlc.arg(source_name)::text
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
  AND observed_at < now() - interval '30 days';

-- name: GetLatestSaaSDiscoveryObservedAtBySource :one
SELECT max(observed_at)::timestamptz AS last_observed_at
FROM saas_app_events
WHERE source_kind = $1
  AND source_name = $2
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL;

-- name: ListSaaSAppEventsBySaaSAppID :many
SELECT *
FROM saas_app_events
WHERE saas_app_id = sqlc.arg(saas_app_id)::bigint
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
ORDER BY observed_at DESC, id DESC
LIMIT sqlc.arg(limit_rows)::int;

-- name: ListTopActorsForSaaSAppByID :many
WITH normalized_events AS (
  SELECT
    NULLIF(trim(actor_display_name), '') AS actor_display_name,
    NULLIF(trim(actor_email), '') AS actor_email,
    NULLIF(trim(actor_external_id), '') AS actor_external_id,
    observed_at
  FROM saas_app_events
  WHERE saas_app_id = sqlc.arg(saas_app_id)::bigint
    AND expired_at IS NULL
    AND last_observed_run_id IS NOT NULL
    AND observed_at >= now() - interval '30 days'
),
grouped_actors AS (
  SELECT
    (array_agg(actor_display_name ORDER BY observed_at DESC) FILTER (WHERE actor_display_name IS NOT NULL))[1] AS actor_display_name,
    (array_agg(actor_email ORDER BY observed_at DESC) FILTER (WHERE actor_email IS NOT NULL))[1] AS actor_email,
    (array_agg(actor_external_id ORDER BY observed_at DESC) FILTER (WHERE actor_external_id IS NOT NULL))[1] AS actor_external_id,
    count(*) AS event_count,
    max(observed_at)::timestamptz AS last_observed_at
  FROM normalized_events
  GROUP BY COALESCE(actor_external_id, actor_email, actor_display_name, '')
)
SELECT
  COALESCE(actor_display_name, actor_email, actor_external_id, '')::text AS actor_label,
  COALESCE(actor_email, '')::text AS actor_email,
  COALESCE(actor_external_id, '')::text AS actor_external_id,
  event_count,
  last_observed_at
FROM grouped_actors
ORDER BY event_count DESC, last_observed_at DESC
LIMIT sqlc.arg(limit_rows)::int;

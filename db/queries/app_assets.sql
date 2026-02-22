-- name: UpsertAppAssetsBulkBySource :execrows
WITH input AS (
  SELECT
    i,
    (sqlc.arg(asset_kinds)::text[])[i] AS asset_kind,
    (sqlc.arg(external_ids)::text[])[i] AS external_id,
    (sqlc.arg(parent_external_ids)::text[])[i] AS parent_external_id,
    (sqlc.arg(display_names)::text[])[i] AS display_name,
    (sqlc.arg(statuses)::text[])[i] AS status,
    (sqlc.arg(created_at_sources)::timestamptz[])[i] AS created_at_source,
    (sqlc.arg(updated_at_sources)::timestamptz[])[i] AS updated_at_source,
    (sqlc.arg(raw_jsons)::jsonb[])[i] AS raw_json
  FROM generate_subscripts(sqlc.arg(external_ids)::text[], 1) AS s(i)
),
dedup AS (
  SELECT DISTINCT ON (asset_kind, external_id)
    asset_kind,
    external_id,
    parent_external_id,
    display_name,
    status,
    created_at_source,
    updated_at_source,
    raw_json
  FROM input
  ORDER BY asset_kind, external_id, i DESC
)
INSERT INTO app_assets (
  source_kind,
  source_name,
  asset_kind,
  external_id,
  parent_external_id,
  display_name,
  status,
  created_at_source,
  updated_at_source,
  raw_json,
  seen_in_run_id,
  seen_at,
  updated_at
)
SELECT
  sqlc.arg(source_kind)::text,
  sqlc.arg(source_name)::text,
  input.asset_kind,
  input.external_id,
  input.parent_external_id,
  input.display_name,
  input.status,
  input.created_at_source,
  input.updated_at_source,
  input.raw_json,
  sqlc.arg(seen_in_run_id)::bigint,
  now(),
  now()
FROM dedup input
ON CONFLICT (source_kind, source_name, asset_kind, external_id) DO UPDATE SET
  parent_external_id = EXCLUDED.parent_external_id,
  display_name = EXCLUDED.display_name,
  status = EXCLUDED.status,
  created_at_source = COALESCE(EXCLUDED.created_at_source, app_assets.created_at_source),
  updated_at_source = COALESCE(EXCLUDED.updated_at_source, app_assets.updated_at_source),
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  updated_at = now();

-- name: CountAppAssetsBySourceAndQueryAndKind :one
SELECT count(*)
FROM app_assets aa
WHERE
  aa.source_kind = sqlc.arg(source_kind)::text
  AND aa.source_name = sqlc.arg(source_name)::text
  AND aa.expired_at IS NULL
  AND aa.last_observed_run_id IS NOT NULL
  AND (
    sqlc.arg(asset_kind)::text = ''
    OR aa.asset_kind = sqlc.arg(asset_kind)::text
  )
  AND (
    sqlc.arg(query)::text = ''
    OR aa.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR aa.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR aa.parent_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
  );

-- name: ListAppAssetsPageBySourceAndQueryAndKind :many
SELECT aa.*
FROM app_assets aa
WHERE
  aa.source_kind = sqlc.arg(source_kind)::text
  AND aa.source_name = sqlc.arg(source_name)::text
  AND aa.expired_at IS NULL
  AND aa.last_observed_run_id IS NOT NULL
  AND (
    sqlc.arg(asset_kind)::text = ''
    OR aa.asset_kind = sqlc.arg(asset_kind)::text
  )
  AND (
    sqlc.arg(query)::text = ''
    OR aa.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR aa.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR aa.parent_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
ORDER BY
  lower(COALESCE(NULLIF(trim(aa.display_name), ''), aa.external_id)) ASC,
  aa.id ASC
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: GetAppAssetByID :one
SELECT *
FROM app_assets
WHERE id = $1
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL;

-- name: GetAppAssetBySourceAndKindAndExternalID :one
SELECT *
FROM app_assets
WHERE source_kind = sqlc.arg(source_kind)::text
  AND source_name = sqlc.arg(source_name)::text
  AND asset_kind = sqlc.arg(asset_kind)::text
  AND external_id = sqlc.arg(external_id)::text
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL;

-- name: PromoteAppAssetsSeenInRunBySource :execrows
UPDATE app_assets
SET
  last_observed_run_id = sqlc.arg(last_observed_run_id)::bigint,
  last_observed_at = now(),
  expired_at = NULL,
  expired_run_id = NULL
WHERE source_kind = sqlc.arg(source_kind)::text
  AND source_name = sqlc.arg(source_name)::text
  AND seen_in_run_id = sqlc.arg(last_observed_run_id)::bigint;

-- name: ExpireAppAssetsNotSeenInRunBySource :execrows
UPDATE app_assets
SET
  expired_at = now(),
  expired_run_id = sqlc.arg(expired_run_id)::bigint
WHERE source_kind = sqlc.arg(source_kind)::text
  AND source_name = sqlc.arg(source_name)::text
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
  AND (
    seen_in_run_id <> sqlc.arg(expired_run_id)::bigint
    OR seen_in_run_id IS NULL
  );

-- name: UpsertAppAssetOwnersBulkBySource :execrows
WITH input AS (
  SELECT
    i,
    (sqlc.arg(asset_kinds)::text[])[i] AS asset_kind,
    (sqlc.arg(asset_external_ids)::text[])[i] AS asset_external_id,
    (sqlc.arg(owner_kinds)::text[])[i] AS owner_kind,
    (sqlc.arg(owner_external_ids)::text[])[i] AS owner_external_id,
    (sqlc.arg(owner_display_names)::text[])[i] AS owner_display_name,
    (sqlc.arg(owner_emails)::text[])[i] AS owner_email,
    (sqlc.arg(raw_jsons)::jsonb[])[i] AS raw_json
  FROM generate_subscripts(sqlc.arg(asset_external_ids)::text[], 1) AS s(i)
),
dedup AS (
  SELECT DISTINCT ON (asset_kind, asset_external_id, owner_kind, owner_external_id)
    asset_kind,
    asset_external_id,
    owner_kind,
    owner_external_id,
    owner_display_name,
    owner_email,
    raw_json
  FROM input
  ORDER BY asset_kind, asset_external_id, owner_kind, owner_external_id, i DESC
)
INSERT INTO app_asset_owners (
  app_asset_id,
  owner_kind,
  owner_external_id,
  owner_display_name,
  owner_email,
  raw_json,
  seen_in_run_id,
  seen_at,
  updated_at
)
SELECT
  aa.id,
  input.owner_kind,
  input.owner_external_id,
  input.owner_display_name,
  input.owner_email,
  input.raw_json,
  sqlc.arg(seen_in_run_id)::bigint,
  now(),
  now()
FROM dedup input
JOIN app_assets aa
  ON aa.source_kind = sqlc.arg(source_kind)::text
  AND aa.source_name = sqlc.arg(source_name)::text
  AND aa.asset_kind = input.asset_kind
  AND aa.external_id = input.asset_external_id
ON CONFLICT (app_asset_id, owner_kind, owner_external_id) DO UPDATE SET
  owner_display_name = EXCLUDED.owner_display_name,
  owner_email = EXCLUDED.owner_email,
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  updated_at = now();

-- name: ListAppAssetOwnersByAssetID :many
SELECT *
FROM app_asset_owners
WHERE app_asset_id = $1
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
ORDER BY
  lower(COALESCE(NULLIF(trim(owner_display_name), ''), NULLIF(trim(owner_email), ''), owner_external_id)) ASC,
  id ASC;

-- name: ListAppAssetOwnersByAssetIDs :many
SELECT *
FROM app_asset_owners
WHERE app_asset_id = ANY(sqlc.arg(asset_ids)::bigint[])
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
ORDER BY app_asset_id ASC, id ASC;

-- name: PromoteAppAssetOwnersSeenInRunBySource :execrows
UPDATE app_asset_owners aao
SET
  last_observed_run_id = sqlc.arg(last_observed_run_id)::bigint,
  last_observed_at = now(),
  expired_at = NULL,
  expired_run_id = NULL
FROM app_assets aa
WHERE aa.id = aao.app_asset_id
  AND aa.source_kind = sqlc.arg(source_kind)::text
  AND aa.source_name = sqlc.arg(source_name)::text
  AND aao.seen_in_run_id = sqlc.arg(last_observed_run_id)::bigint;

-- name: ExpireAppAssetOwnersNotSeenInRunBySource :execrows
UPDATE app_asset_owners aao
SET
  expired_at = now(),
  expired_run_id = sqlc.arg(expired_run_id)::bigint
FROM app_assets aa
WHERE aa.id = aao.app_asset_id
  AND aa.source_kind = sqlc.arg(source_kind)::text
  AND aa.source_name = sqlc.arg(source_name)::text
  AND aao.expired_at IS NULL
  AND aao.last_observed_run_id IS NOT NULL
  AND (
    aao.seen_in_run_id <> sqlc.arg(expired_run_id)::bigint
    OR aao.seen_in_run_id IS NULL
  );

-- name: UpsertSaaSAppSourcesBulkBySource :execrows
WITH input AS (
  SELECT
    i,
    (sqlc.arg(canonical_keys)::text[])[i] AS canonical_key,
    (sqlc.arg(source_app_ids)::text[])[i] AS source_app_id,
    (sqlc.arg(source_app_names)::text[])[i] AS source_app_name,
    (sqlc.arg(source_app_domains)::text[])[i] AS source_app_domain,
    (sqlc.arg(seen_ats)::timestamptz[])[i] AS seen_at
  FROM generate_subscripts(sqlc.arg(source_app_ids)::text[], 1) AS s(i)
),
dedup AS (
  SELECT DISTINCT ON (source_app_id)
    canonical_key,
    source_app_id,
    source_app_name,
    source_app_domain,
    seen_at
  FROM input
  WHERE trim(source_app_id) <> ''
  ORDER BY source_app_id, i DESC
)
INSERT INTO saas_app_sources (
  saas_app_id,
  source_kind,
  source_name,
  source_app_id,
  source_app_name,
  source_app_domain,
  seen_in_run_id,
  seen_at,
  updated_at
)
SELECT
  sa.id,
  sqlc.arg(source_kind)::text,
  sqlc.arg(source_name)::text,
  d.source_app_id,
  d.source_app_name,
  d.source_app_domain,
  sqlc.arg(seen_in_run_id)::bigint,
  COALESCE(d.seen_at, now()),
  now()
FROM dedup d
JOIN saas_apps sa ON sa.canonical_key = d.canonical_key
ON CONFLICT (source_kind, source_name, source_app_id) DO UPDATE SET
  saas_app_id = EXCLUDED.saas_app_id,
  source_app_name = EXCLUDED.source_app_name,
  source_app_domain = EXCLUDED.source_app_domain,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  updated_at = now();

-- name: PromoteSaaSAppSourcesSeenInRunBySource :execrows
UPDATE saas_app_sources
SET
  last_observed_run_id = sqlc.arg(last_observed_run_id)::bigint,
  last_observed_at = now(),
  expired_at = NULL,
  expired_run_id = NULL
WHERE source_kind = sqlc.arg(source_kind)::text
  AND source_name = sqlc.arg(source_name)::text
  AND seen_in_run_id = sqlc.arg(last_observed_run_id)::bigint;

-- name: ExpireSaaSAppSourcesNotSeenInRunBySource :execrows
UPDATE saas_app_sources
SET
  expired_at = now(),
  expired_run_id = sqlc.arg(expired_run_id)::bigint
WHERE source_kind = sqlc.arg(source_kind)::text
  AND source_name = sqlc.arg(source_name)::text
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
  AND COALESCE(last_observed_at, seen_at, created_at) < now() - interval '30 days';

-- name: ListSaaSAppSourcesBySaaSAppID :many
SELECT *
FROM saas_app_sources
WHERE saas_app_id = $1
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
ORDER BY source_kind ASC, source_name ASC, lower(COALESCE(NULLIF(trim(source_app_name), ''), source_app_id)) ASC, id ASC;

-- name: ListSaaSAppIDsFromSourcesSeenInRunBySource :many
SELECT DISTINCT saas_app_id
FROM saas_app_sources
WHERE source_kind = sqlc.arg(source_kind)::text
  AND source_name = sqlc.arg(source_name)::text
  AND seen_in_run_id = sqlc.arg(seen_in_run_id)::bigint
ORDER BY saas_app_id ASC;

-- name: ListMappedOktaDiscoveryAppsBySource :many
SELECT DISTINCT
  sas.saas_app_id,
  m.integration_kind
FROM saas_app_sources sas
JOIN integration_okta_app_map m
  ON m.okta_app_external_id = sas.source_app_id
WHERE sas.source_kind = 'okta'
  AND sas.source_name = sqlc.arg(source_name)::text
  AND sas.expired_at IS NULL
  AND sas.last_observed_run_id IS NOT NULL
ORDER BY sas.saas_app_id ASC, m.integration_kind ASC;

-- name: ListEntraDiscoveryAppIDsWithManagedAssetsBySource :many
SELECT DISTINCT sas.saas_app_id
FROM saas_app_sources sas
JOIN app_assets aa
  ON aa.source_kind = 'entra'
 AND aa.source_name = sas.source_name
 AND aa.expired_at IS NULL
 AND aa.last_observed_run_id IS NOT NULL
 AND aa.asset_kind IN ('entra_application', 'entra_service_principal')
 AND (
   aa.external_id = sas.source_app_id
   OR aa.parent_external_id = sas.source_app_id
   OR lower(aa.display_name) = lower(sas.source_app_name)
 )
WHERE sas.source_kind = 'entra'
  AND sas.source_name = sqlc.arg(source_name)::text
  AND sas.expired_at IS NULL
  AND sas.last_observed_run_id IS NOT NULL
ORDER BY sas.saas_app_id ASC;

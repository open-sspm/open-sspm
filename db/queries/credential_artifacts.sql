-- name: UpsertCredentialArtifactsBulkBySource :execrows
WITH input AS (
  SELECT
    i,
    (sqlc.arg(asset_ref_kinds)::text[])[i] AS asset_ref_kind,
    (sqlc.arg(asset_ref_external_ids)::text[])[i] AS asset_ref_external_id,
    (sqlc.arg(credential_kinds)::text[])[i] AS credential_kind,
    (sqlc.arg(external_ids)::text[])[i] AS external_id,
    (sqlc.arg(display_names)::text[])[i] AS display_name,
    (sqlc.arg(fingerprints)::text[])[i] AS fingerprint,
    (sqlc.arg(scope_jsons)::jsonb[])[i] AS scope_json,
    (sqlc.arg(statuses)::text[])[i] AS status,
    (sqlc.arg(created_at_sources)::timestamptz[])[i] AS created_at_source,
    (sqlc.arg(expires_at_sources)::timestamptz[])[i] AS expires_at_source,
    (sqlc.arg(last_used_at_sources)::timestamptz[])[i] AS last_used_at_source,
    (sqlc.arg(created_by_kinds)::text[])[i] AS created_by_kind,
    (sqlc.arg(created_by_external_ids)::text[])[i] AS created_by_external_id,
    (sqlc.arg(created_by_display_names)::text[])[i] AS created_by_display_name,
    (sqlc.arg(approved_by_kinds)::text[])[i] AS approved_by_kind,
    (sqlc.arg(approved_by_external_ids)::text[])[i] AS approved_by_external_id,
    (sqlc.arg(approved_by_display_names)::text[])[i] AS approved_by_display_name,
    (sqlc.arg(raw_jsons)::jsonb[])[i] AS raw_json
  FROM generate_subscripts(sqlc.arg(external_ids)::text[], 1) AS s(i)
),
dedup AS (
  SELECT DISTINCT ON (asset_ref_kind, asset_ref_external_id, credential_kind, external_id)
    asset_ref_kind,
    asset_ref_external_id,
    credential_kind,
    external_id,
    display_name,
    fingerprint,
    scope_json,
    status,
    created_at_source,
    expires_at_source,
    last_used_at_source,
    created_by_kind,
    created_by_external_id,
    created_by_display_name,
    approved_by_kind,
    approved_by_external_id,
    approved_by_display_name,
    raw_json
  FROM input
  ORDER BY asset_ref_kind, asset_ref_external_id, credential_kind, external_id, i DESC
)
INSERT INTO credential_artifacts (
  source_kind,
  source_name,
  asset_ref_kind,
  asset_ref_external_id,
  credential_kind,
  external_id,
  display_name,
  fingerprint,
  scope_json,
  status,
  created_at_source,
  expires_at_source,
  last_used_at_source,
  created_by_kind,
  created_by_external_id,
  created_by_display_name,
  approved_by_kind,
  approved_by_external_id,
  approved_by_display_name,
  raw_json,
  seen_in_run_id,
  seen_at,
  updated_at
)
SELECT
  sqlc.arg(source_kind)::text,
  sqlc.arg(source_name)::text,
  input.asset_ref_kind,
  input.asset_ref_external_id,
  input.credential_kind,
  input.external_id,
  input.display_name,
  input.fingerprint,
  input.scope_json,
  input.status,
  input.created_at_source,
  input.expires_at_source,
  input.last_used_at_source,
  input.created_by_kind,
  input.created_by_external_id,
  input.created_by_display_name,
  input.approved_by_kind,
  input.approved_by_external_id,
  input.approved_by_display_name,
  input.raw_json,
  sqlc.arg(seen_in_run_id)::bigint,
  now(),
  now()
FROM dedup input
ON CONFLICT (source_kind, source_name, credential_kind, external_id, asset_ref_kind, asset_ref_external_id) DO UPDATE SET
  display_name = EXCLUDED.display_name,
  fingerprint = EXCLUDED.fingerprint,
  scope_json = EXCLUDED.scope_json,
  status = EXCLUDED.status,
  created_at_source = COALESCE(EXCLUDED.created_at_source, credential_artifacts.created_at_source),
  expires_at_source = COALESCE(EXCLUDED.expires_at_source, credential_artifacts.expires_at_source),
  last_used_at_source = COALESCE(EXCLUDED.last_used_at_source, credential_artifacts.last_used_at_source),
  created_by_kind = COALESCE(NULLIF(EXCLUDED.created_by_kind, ''), credential_artifacts.created_by_kind),
  created_by_external_id = COALESCE(NULLIF(EXCLUDED.created_by_external_id, ''), credential_artifacts.created_by_external_id),
  created_by_display_name = COALESCE(NULLIF(EXCLUDED.created_by_display_name, ''), credential_artifacts.created_by_display_name),
  approved_by_kind = COALESCE(NULLIF(EXCLUDED.approved_by_kind, ''), credential_artifacts.approved_by_kind),
  approved_by_external_id = COALESCE(NULLIF(EXCLUDED.approved_by_external_id, ''), credential_artifacts.approved_by_external_id),
  approved_by_display_name = COALESCE(NULLIF(EXCLUDED.approved_by_display_name, ''), credential_artifacts.approved_by_display_name),
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  updated_at = now();

-- name: CountCredentialArtifactsBySourceAndQueryAndFilters :one
SELECT count(*)
FROM credential_artifacts ca
WHERE
  ca.source_kind = sqlc.arg(source_kind)::text
  AND ca.source_name = sqlc.arg(source_name)::text
  AND ca.expired_at IS NULL
  AND ca.last_observed_run_id IS NOT NULL
  AND (
    sqlc.arg(credential_kind)::text = ''
    OR ca.credential_kind = sqlc.arg(credential_kind)::text
  )
  AND (
    sqlc.arg(status)::text = ''
    OR lower(ca.status) = lower(sqlc.arg(status)::text)
  )
  AND (
    sqlc.arg(risk_level)::text = ''
    OR lower(sqlc.arg(risk_level)::text) = (
      CASE
        WHEN ca.expires_at_source IS NOT NULL
          AND ca.expires_at_source < now()
          AND lower(COALESCE(NULLIF(trim(ca.status), ''), 'active')) IN ('active', 'approved', 'pending_approval')
          THEN 'critical'
        WHEN ca.expires_at_source IS NOT NULL
          AND ca.expires_at_source < now()
          THEN 'high'
        WHEN lower(ca.credential_kind) IN ('entra_client_secret', 'github_deploy_key', 'github_pat_request', 'github_pat_fine_grained')
          AND trim(ca.created_by_external_id) = ''
          AND trim(ca.approved_by_external_id) = ''
          THEN 'critical'
        WHEN ca.expires_at_source IS NOT NULL
          AND ca.expires_at_source >= now()
          AND ca.expires_at_source <= now() + make_interval(days => 7)
          THEN 'high'
        WHEN trim(ca.created_by_external_id) = ''
          THEN 'high'
        WHEN ca.last_used_at_source IS NOT NULL
          AND ca.last_used_at_source <= now() - make_interval(days => 90)
          THEN 'high'
        WHEN ca.expires_at_source IS NOT NULL
          AND ca.expires_at_source >= now()
          AND ca.expires_at_source <= now() + make_interval(days => 30)
          THEN 'medium'
        ELSE 'low'
      END
    )
  )
  AND (
    sqlc.arg(expiry_state)::text = ''
    OR (
      sqlc.arg(expiry_state)::text = 'expired'
      AND ca.expires_at_source IS NOT NULL
      AND ca.expires_at_source < now()
    )
    OR (
      sqlc.arg(expiry_state)::text = 'active'
      AND (ca.expires_at_source IS NULL OR ca.expires_at_source >= now())
    )
  )
  AND (
    sqlc.arg(expires_in_days)::int <= 0
    OR (
      ca.expires_at_source IS NOT NULL
      AND ca.expires_at_source >= now()
      AND ca.expires_at_source <= now() + make_interval(days => sqlc.arg(expires_in_days)::int)
    )
  )
  AND (
    sqlc.arg(query)::text = ''
    OR ca.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR ca.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR ca.asset_ref_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR ca.created_by_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR ca.approved_by_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
  );

-- name: ListCredentialArtifactsPageBySourceAndQueryAndFilters :many
SELECT ca.*
FROM credential_artifacts ca
WHERE
  ca.source_kind = sqlc.arg(source_kind)::text
  AND ca.source_name = sqlc.arg(source_name)::text
  AND ca.expired_at IS NULL
  AND ca.last_observed_run_id IS NOT NULL
  AND (
    sqlc.arg(credential_kind)::text = ''
    OR ca.credential_kind = sqlc.arg(credential_kind)::text
  )
  AND (
    sqlc.arg(status)::text = ''
    OR lower(ca.status) = lower(sqlc.arg(status)::text)
  )
  AND (
    sqlc.arg(risk_level)::text = ''
    OR lower(sqlc.arg(risk_level)::text) = (
      CASE
        WHEN ca.expires_at_source IS NOT NULL
          AND ca.expires_at_source < now()
          AND lower(COALESCE(NULLIF(trim(ca.status), ''), 'active')) IN ('active', 'approved', 'pending_approval')
          THEN 'critical'
        WHEN ca.expires_at_source IS NOT NULL
          AND ca.expires_at_source < now()
          THEN 'high'
        WHEN lower(ca.credential_kind) IN ('entra_client_secret', 'github_deploy_key', 'github_pat_request', 'github_pat_fine_grained')
          AND trim(ca.created_by_external_id) = ''
          AND trim(ca.approved_by_external_id) = ''
          THEN 'critical'
        WHEN ca.expires_at_source IS NOT NULL
          AND ca.expires_at_source >= now()
          AND ca.expires_at_source <= now() + make_interval(days => 7)
          THEN 'high'
        WHEN trim(ca.created_by_external_id) = ''
          THEN 'high'
        WHEN ca.last_used_at_source IS NOT NULL
          AND ca.last_used_at_source <= now() - make_interval(days => 90)
          THEN 'high'
        WHEN ca.expires_at_source IS NOT NULL
          AND ca.expires_at_source >= now()
          AND ca.expires_at_source <= now() + make_interval(days => 30)
          THEN 'medium'
        ELSE 'low'
      END
    )
  )
  AND (
    sqlc.arg(expiry_state)::text = ''
    OR (
      sqlc.arg(expiry_state)::text = 'expired'
      AND ca.expires_at_source IS NOT NULL
      AND ca.expires_at_source < now()
    )
    OR (
      sqlc.arg(expiry_state)::text = 'active'
      AND (ca.expires_at_source IS NULL OR ca.expires_at_source >= now())
    )
  )
  AND (
    sqlc.arg(expires_in_days)::int <= 0
    OR (
      ca.expires_at_source IS NOT NULL
      AND ca.expires_at_source >= now()
      AND ca.expires_at_source <= now() + make_interval(days => sqlc.arg(expires_in_days)::int)
    )
  )
  AND (
    sqlc.arg(query)::text = ''
    OR ca.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR ca.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR ca.asset_ref_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR ca.created_by_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR ca.approved_by_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
ORDER BY
  COALESCE(ca.expires_at_source, 'infinity'::timestamptz) ASC,
  lower(COALESCE(NULLIF(trim(ca.display_name), ''), ca.external_id)) ASC,
  ca.id ASC
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: ListCredentialArtifactsForAssetRef :many
SELECT ca.*
FROM credential_artifacts ca
WHERE ca.source_kind = sqlc.arg(source_kind)::text
  AND ca.source_name = sqlc.arg(source_name)::text
  AND ca.asset_ref_kind = sqlc.arg(asset_ref_kind)::text
  AND ca.asset_ref_external_id = sqlc.arg(asset_ref_external_id)::text
  AND ca.expired_at IS NULL
  AND ca.last_observed_run_id IS NOT NULL
ORDER BY
  COALESCE(ca.expires_at_source, 'infinity'::timestamptz) ASC,
  ca.id ASC;

-- name: GetCredentialArtifactByID :one
SELECT *
FROM credential_artifacts
WHERE id = $1
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL;

-- name: ListCredentialArtifactCountsByAssetRef :many
WITH requested AS (
  SELECT
    k.asset_ref_kind::text AS asset_ref_kind,
    e.asset_ref_external_id::text AS asset_ref_external_id
  FROM unnest(sqlc.arg(asset_ref_kinds)::text[]) WITH ORDINALITY AS k(asset_ref_kind, ord)
  JOIN unnest(sqlc.arg(asset_ref_external_ids)::text[]) WITH ORDINALITY AS e(asset_ref_external_id, ord)
    USING (ord)
)
SELECT
  r.asset_ref_kind::text AS asset_ref_kind,
  r.asset_ref_external_id::text AS asset_ref_external_id,
  count(ca.id)::bigint AS credential_count
FROM requested r
LEFT JOIN credential_artifacts ca
  ON ca.source_kind = sqlc.arg(source_kind)::text
  AND ca.source_name = sqlc.arg(source_name)::text
  AND ca.asset_ref_kind = r.asset_ref_kind
  AND ca.asset_ref_external_id = r.asset_ref_external_id
  AND ca.expired_at IS NULL
  AND ca.last_observed_run_id IS NOT NULL
GROUP BY r.asset_ref_kind, r.asset_ref_external_id
ORDER BY r.asset_ref_kind, r.asset_ref_external_id;

-- name: PromoteCredentialArtifactsSeenInRunBySource :execrows
UPDATE credential_artifacts
SET
  last_observed_run_id = sqlc.arg(last_observed_run_id)::bigint,
  last_observed_at = now(),
  expired_at = NULL,
  expired_run_id = NULL
WHERE source_kind = sqlc.arg(source_kind)::text
  AND source_name = sqlc.arg(source_name)::text
  AND seen_in_run_id = sqlc.arg(last_observed_run_id)::bigint;

-- name: ExpireCredentialArtifactsNotSeenInRunBySource :execrows
UPDATE credential_artifacts
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

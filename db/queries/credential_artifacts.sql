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
  lineage_key,
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
  md5(
    coalesce(sqlc.arg(source_kind)::text, '') || '|' ||
    coalesce(sqlc.arg(source_name)::text, '') || '|' ||
    coalesce(input.asset_ref_kind, '') || '|' ||
    coalesce(input.asset_ref_external_id, '') || '|' ||
    coalesce(input.credential_kind, '') || '|' ||
    CASE
      WHEN coalesce(input.display_name, '') ~ '\s*\[\d{4}\]\s*$' THEN
        'cohort:' || lower(trim(regexp_replace(coalesce(input.display_name, ''), '\s*\[\d{4}\]\s*$', '')))
      ELSE
        'id:' || coalesce(input.external_id, '') || '|' || lower(trim(coalesce(input.display_name, '')))
    END
  ),
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
  lineage_key = EXCLUDED.lineage_key,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  updated_at = now();

-- name: CountCredentialArtifactsBySourceAndQueryAndFilters :one
WITH rated_credentials AS (
  SELECT
    ca.*,
    COALESCE(risk.risk_level, 'low')::text AS risk_level,
    COALESCE(NULLIF(trim(aa.display_name), ''), '')::text AS asset_name
  FROM credential_artifacts ca
  LEFT JOIN credential_artifact_risk_read_models risk
    ON risk.credential_artifact_id = ca.id
  LEFT JOIN app_assets aa
    ON aa.source_kind = ca.source_kind
   AND aa.source_name = ca.source_name
   AND aa.expired_at IS NULL
   AND aa.external_id = CASE
        WHEN strpos(ca.asset_ref_external_id, ':') > 0 THEN substr(ca.asset_ref_external_id, strpos(ca.asset_ref_external_id, ':') + 1)
        ELSE ca.asset_ref_external_id
      END
   AND (
        strpos(ca.asset_ref_external_id, ':') = 0
        OR aa.asset_kind = split_part(ca.asset_ref_external_id, ':', 1)
      )
  WHERE
    ca.source_kind = sqlc.arg(source_kind)::text
    AND ca.source_name = sqlc.arg(source_name)::text
    AND ca.expired_at IS NULL
    AND ca.last_observed_run_id IS NOT NULL
    AND (
      cardinality(sqlc.arg(credential_kinds)::text[]) = 0
      OR ca.credential_kind = ANY(sqlc.arg(credential_kinds)::text[])
    )
    AND (
      sqlc.arg(status)::text = ''
      OR lower(ca.status) = lower(sqlc.arg(status)::text)
    )
    AND (
      sqlc.arg(owner)::text = ''
      OR ca.created_by_external_id ILIKE ('%' || sqlc.arg(owner)::text || '%')
      OR ca.created_by_display_name ILIKE ('%' || sqlc.arg(owner)::text || '%')
      OR ca.approved_by_external_id ILIKE ('%' || sqlc.arg(owner)::text || '%')
      OR ca.approved_by_display_name ILIKE ('%' || sqlc.arg(owner)::text || '%')
    )
    AND (
      sqlc.arg(asset)::text = ''
      OR COALESCE(NULLIF(trim(aa.display_name), ''), '') ILIKE ('%' || sqlc.arg(asset)::text || '%')
      OR ca.asset_ref_external_id ILIKE ('%' || sqlc.arg(asset)::text || '%')
      OR ca.asset_ref_kind ILIKE ('%' || sqlc.arg(asset)::text || '%')
    )
    AND (
      sqlc.arg(newer_days)::int <= 0
      OR (
        ca.created_at_source IS NOT NULL
        AND ca.created_at_source >= sqlc.arg(evaluated_at)::timestamptz - make_interval(days => sqlc.arg(newer_days)::int)
      )
    )
),
ranked AS (
  SELECT rc.*,
    ROW_NUMBER() OVER (
      PARTITION BY rc.lineage_key
      ORDER BY
        CASE rc.risk_level
          WHEN 'critical' THEN 1
          WHEN 'high' THEN 2
          WHEN 'medium' THEN 3
          WHEN 'low' THEN 4
          ELSE 5
        END ASC,
        COALESCE(rc.expires_at_source, 'infinity'::timestamptz) DESC,
        COALESCE(rc.created_at_source, '-infinity'::timestamptz) DESC,
        rc.id DESC
    ) AS lineage_rank
  FROM rated_credentials rc
),
latest AS (
  SELECT * FROM ranked WHERE lineage_rank = 1
)
SELECT count(*)::bigint
FROM latest rc
WHERE
  (
    cardinality(sqlc.arg(risk_levels)::text[]) = 0
    OR rc.risk_level = ANY(sqlc.arg(risk_levels)::text[])
  )
  AND (
    sqlc.arg(expiry_state)::text = ''
    OR (
      sqlc.arg(expiry_state)::text = 'expired'
      AND rc.expires_at_source IS NOT NULL
      AND rc.expires_at_source < sqlc.arg(evaluated_at)::timestamptz
    )
    OR (
      sqlc.arg(expiry_state)::text = 'active'
      AND (rc.expires_at_source IS NULL OR rc.expires_at_source >= sqlc.arg(evaluated_at)::timestamptz)
    )
  )
  AND (
    sqlc.arg(expires_in_days)::int <= 0
    OR (
      rc.expires_at_source IS NOT NULL
      AND rc.expires_at_source >= sqlc.arg(evaluated_at)::timestamptz
      AND rc.expires_at_source <= sqlc.arg(evaluated_at)::timestamptz + make_interval(days => sqlc.arg(expires_in_days)::int)
    )
  )
  AND (
    sqlc.arg(query)::text = ''
    OR rc.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.asset_ref_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.created_by_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.approved_by_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.asset_name ILIKE ('%' || sqlc.arg(query)::text || '%')
  );

-- name: ListCredentialArtifactsPageBySourceAndQueryAndFilters :many
WITH rated_credentials AS (
  SELECT
    ca.*,
    COALESCE(risk.risk_level, 'low')::text AS risk_level,
    COALESCE(NULLIF(trim(aa.display_name), ''), '')::text AS asset_name
  FROM credential_artifacts ca
  LEFT JOIN credential_artifact_risk_read_models risk
    ON risk.credential_artifact_id = ca.id
  LEFT JOIN app_assets aa
    ON aa.source_kind = ca.source_kind
   AND aa.source_name = ca.source_name
   AND aa.expired_at IS NULL
   AND aa.external_id = CASE
        WHEN strpos(ca.asset_ref_external_id, ':') > 0 THEN substr(ca.asset_ref_external_id, strpos(ca.asset_ref_external_id, ':') + 1)
        ELSE ca.asset_ref_external_id
      END
   AND (
        strpos(ca.asset_ref_external_id, ':') = 0
        OR aa.asset_kind = split_part(ca.asset_ref_external_id, ':', 1)
      )
  WHERE
    ca.source_kind = sqlc.arg(source_kind)::text
    AND ca.source_name = sqlc.arg(source_name)::text
    AND ca.expired_at IS NULL
    AND ca.last_observed_run_id IS NOT NULL
    AND (
      cardinality(sqlc.arg(credential_kinds)::text[]) = 0
      OR ca.credential_kind = ANY(sqlc.arg(credential_kinds)::text[])
    )
    AND (
      sqlc.arg(status)::text = ''
      OR lower(ca.status) = lower(sqlc.arg(status)::text)
    )
    AND (
      sqlc.arg(owner)::text = ''
      OR ca.created_by_external_id ILIKE ('%' || sqlc.arg(owner)::text || '%')
      OR ca.created_by_display_name ILIKE ('%' || sqlc.arg(owner)::text || '%')
      OR ca.approved_by_external_id ILIKE ('%' || sqlc.arg(owner)::text || '%')
      OR ca.approved_by_display_name ILIKE ('%' || sqlc.arg(owner)::text || '%')
    )
    AND (
      sqlc.arg(asset)::text = ''
      OR COALESCE(NULLIF(trim(aa.display_name), ''), '') ILIKE ('%' || sqlc.arg(asset)::text || '%')
      OR ca.asset_ref_external_id ILIKE ('%' || sqlc.arg(asset)::text || '%')
      OR ca.asset_ref_kind ILIKE ('%' || sqlc.arg(asset)::text || '%')
    )
    AND (
      sqlc.arg(newer_days)::int <= 0
      OR (
        ca.created_at_source IS NOT NULL
        AND ca.created_at_source >= sqlc.arg(evaluated_at)::timestamptz - make_interval(days => sqlc.arg(newer_days)::int)
      )
    )
),
ranked AS (
  SELECT rc.*,
    ROW_NUMBER() OVER (
      PARTITION BY rc.lineage_key
      ORDER BY
        CASE rc.risk_level
          WHEN 'critical' THEN 1
          WHEN 'high' THEN 2
          WHEN 'medium' THEN 3
          WHEN 'low' THEN 4
          ELSE 5
        END ASC,
        COALESCE(rc.expires_at_source, 'infinity'::timestamptz) DESC,
        COALESCE(rc.created_at_source, '-infinity'::timestamptz) DESC,
        rc.id DESC
    ) AS lineage_rank,
    COUNT(*) OVER (PARTITION BY rc.lineage_key) AS lineage_version_count
  FROM rated_credentials rc
),
latest AS (
  SELECT * FROM ranked WHERE lineage_rank = 1
)
SELECT
  rc.*,
  rc.lineage_version_count::bigint AS version_count
FROM latest rc
WHERE
  (
    cardinality(sqlc.arg(risk_levels)::text[]) = 0
    OR rc.risk_level = ANY(sqlc.arg(risk_levels)::text[])
  )
  AND (
    sqlc.arg(expiry_state)::text = ''
    OR (
      sqlc.arg(expiry_state)::text = 'expired'
      AND rc.expires_at_source IS NOT NULL
      AND rc.expires_at_source < sqlc.arg(evaluated_at)::timestamptz
    )
    OR (
      sqlc.arg(expiry_state)::text = 'active'
      AND (rc.expires_at_source IS NULL OR rc.expires_at_source >= sqlc.arg(evaluated_at)::timestamptz)
    )
  )
  AND (
    sqlc.arg(expires_in_days)::int <= 0
    OR (
      rc.expires_at_source IS NOT NULL
      AND rc.expires_at_source >= sqlc.arg(evaluated_at)::timestamptz
      AND rc.expires_at_source <= sqlc.arg(evaluated_at)::timestamptz + make_interval(days => sqlc.arg(expires_in_days)::int)
    )
  )
  AND (
    sqlc.arg(query)::text = ''
    OR rc.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.asset_ref_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.created_by_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.approved_by_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.asset_name ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
ORDER BY
  CASE WHEN sqlc.arg(sort_by)::text = 'risk' THEN
    CASE rc.risk_level
      WHEN 'critical' THEN 1
      WHEN 'high' THEN 2
      WHEN 'medium' THEN 3
      WHEN 'low' THEN 4
      ELSE 5
    END
  END ASC NULLS LAST,
  CASE WHEN sqlc.arg(sort_by)::text = 'asset' THEN lower(COALESCE(NULLIF(trim(rc.asset_name), ''), rc.asset_ref_external_id)) END ASC NULLS LAST,
  CASE WHEN sqlc.arg(sort_by)::text = 'credential' THEN lower(COALESCE(NULLIF(trim(rc.display_name), ''), rc.external_id)) END ASC NULLS LAST,
  COALESCE(rc.expires_at_source, 'infinity'::timestamptz) ASC,
  lower(COALESCE(NULLIF(trim(rc.display_name), ''), rc.external_id)) ASC,
  rc.id ASC
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: CountCredentialArtifactsBySourcesAndQueryAndFilters :one
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
),
rated_credentials AS (
  SELECT
    ca.*,
    COALESCE(risk.risk_level, 'low')::text AS risk_level,
    COALESCE(NULLIF(trim(aa.display_name), ''), '')::text AS asset_name
  FROM credential_artifacts ca
  LEFT JOIN credential_artifact_risk_read_models risk
    ON risk.credential_artifact_id = ca.id
  JOIN configured_sources cs
    ON cs.source_kind = ca.source_kind
   AND cs.source_name = ca.source_name
  LEFT JOIN app_assets aa
    ON aa.source_kind = ca.source_kind
   AND aa.source_name = ca.source_name
   AND aa.expired_at IS NULL
   AND aa.external_id = CASE
        WHEN strpos(ca.asset_ref_external_id, ':') > 0 THEN substr(ca.asset_ref_external_id, strpos(ca.asset_ref_external_id, ':') + 1)
        ELSE ca.asset_ref_external_id
      END
   AND (
        strpos(ca.asset_ref_external_id, ':') = 0
        OR aa.asset_kind = split_part(ca.asset_ref_external_id, ':', 1)
      )
  WHERE
    ca.expired_at IS NULL
    AND ca.last_observed_run_id IS NOT NULL
    AND (
      cardinality(sqlc.arg(credential_kinds)::text[]) = 0
      OR ca.credential_kind = ANY(sqlc.arg(credential_kinds)::text[])
    )
    AND (
      sqlc.arg(status)::text = ''
      OR lower(ca.status) = lower(sqlc.arg(status)::text)
    )
    AND (
      sqlc.arg(owner)::text = ''
      OR ca.created_by_external_id ILIKE ('%' || sqlc.arg(owner)::text || '%')
      OR ca.created_by_display_name ILIKE ('%' || sqlc.arg(owner)::text || '%')
      OR ca.approved_by_external_id ILIKE ('%' || sqlc.arg(owner)::text || '%')
      OR ca.approved_by_display_name ILIKE ('%' || sqlc.arg(owner)::text || '%')
    )
    AND (
      sqlc.arg(asset)::text = ''
      OR COALESCE(NULLIF(trim(aa.display_name), ''), '') ILIKE ('%' || sqlc.arg(asset)::text || '%')
      OR ca.asset_ref_external_id ILIKE ('%' || sqlc.arg(asset)::text || '%')
      OR ca.asset_ref_kind ILIKE ('%' || sqlc.arg(asset)::text || '%')
    )
    AND (
      sqlc.arg(newer_days)::int <= 0
      OR (
        ca.created_at_source IS NOT NULL
        AND ca.created_at_source >= sqlc.arg(evaluated_at)::timestamptz - make_interval(days => sqlc.arg(newer_days)::int)
      )
    )
),
ranked AS (
  SELECT rc.*,
    ROW_NUMBER() OVER (
      PARTITION BY rc.lineage_key
      ORDER BY
        CASE rc.risk_level
          WHEN 'critical' THEN 1
          WHEN 'high' THEN 2
          WHEN 'medium' THEN 3
          WHEN 'low' THEN 4
          ELSE 5
        END ASC,
        COALESCE(rc.expires_at_source, 'infinity'::timestamptz) DESC,
        COALESCE(rc.created_at_source, '-infinity'::timestamptz) DESC,
        rc.id DESC
    ) AS lineage_rank
  FROM rated_credentials rc
),
latest AS (
  SELECT * FROM ranked WHERE lineage_rank = 1
)
SELECT count(*)::bigint
FROM latest rc
WHERE
  (
    cardinality(sqlc.arg(risk_levels)::text[]) = 0
    OR rc.risk_level = ANY(sqlc.arg(risk_levels)::text[])
  )
  AND (
    sqlc.arg(expiry_state)::text = ''
    OR (
      sqlc.arg(expiry_state)::text = 'expired'
      AND rc.expires_at_source IS NOT NULL
      AND rc.expires_at_source < sqlc.arg(evaluated_at)::timestamptz
    )
    OR (
      sqlc.arg(expiry_state)::text = 'active'
      AND (rc.expires_at_source IS NULL OR rc.expires_at_source >= sqlc.arg(evaluated_at)::timestamptz)
    )
  )
  AND (
    sqlc.arg(expires_in_days)::int <= 0
    OR (
      rc.expires_at_source IS NOT NULL
      AND rc.expires_at_source >= sqlc.arg(evaluated_at)::timestamptz
      AND rc.expires_at_source <= sqlc.arg(evaluated_at)::timestamptz + make_interval(days => sqlc.arg(expires_in_days)::int)
    )
  )
  AND (
    sqlc.arg(query)::text = ''
    OR rc.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.asset_ref_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.created_by_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.approved_by_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.asset_name ILIKE ('%' || sqlc.arg(query)::text || '%')
  );

-- name: ListCredentialArtifactsPageBySourcesAndQueryAndFilters :many
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
),
rated_credentials AS (
  SELECT
    ca.*,
    COALESCE(risk.risk_level, 'low')::text AS risk_level,
    COALESCE(NULLIF(trim(aa.display_name), ''), '')::text AS asset_name
  FROM credential_artifacts ca
  LEFT JOIN credential_artifact_risk_read_models risk
    ON risk.credential_artifact_id = ca.id
  JOIN configured_sources cs
    ON cs.source_kind = ca.source_kind
   AND cs.source_name = ca.source_name
  LEFT JOIN app_assets aa
    ON aa.source_kind = ca.source_kind
   AND aa.source_name = ca.source_name
   AND aa.expired_at IS NULL
   AND aa.external_id = CASE
        WHEN strpos(ca.asset_ref_external_id, ':') > 0 THEN substr(ca.asset_ref_external_id, strpos(ca.asset_ref_external_id, ':') + 1)
        ELSE ca.asset_ref_external_id
      END
   AND (
        strpos(ca.asset_ref_external_id, ':') = 0
        OR aa.asset_kind = split_part(ca.asset_ref_external_id, ':', 1)
      )
  WHERE
    ca.expired_at IS NULL
    AND ca.last_observed_run_id IS NOT NULL
    AND (
      cardinality(sqlc.arg(credential_kinds)::text[]) = 0
      OR ca.credential_kind = ANY(sqlc.arg(credential_kinds)::text[])
    )
    AND (
      sqlc.arg(status)::text = ''
      OR lower(ca.status) = lower(sqlc.arg(status)::text)
    )
    AND (
      sqlc.arg(owner)::text = ''
      OR ca.created_by_external_id ILIKE ('%' || sqlc.arg(owner)::text || '%')
      OR ca.created_by_display_name ILIKE ('%' || sqlc.arg(owner)::text || '%')
      OR ca.approved_by_external_id ILIKE ('%' || sqlc.arg(owner)::text || '%')
      OR ca.approved_by_display_name ILIKE ('%' || sqlc.arg(owner)::text || '%')
    )
    AND (
      sqlc.arg(asset)::text = ''
      OR COALESCE(NULLIF(trim(aa.display_name), ''), '') ILIKE ('%' || sqlc.arg(asset)::text || '%')
      OR ca.asset_ref_external_id ILIKE ('%' || sqlc.arg(asset)::text || '%')
      OR ca.asset_ref_kind ILIKE ('%' || sqlc.arg(asset)::text || '%')
    )
    AND (
      sqlc.arg(newer_days)::int <= 0
      OR (
        ca.created_at_source IS NOT NULL
        AND ca.created_at_source >= sqlc.arg(evaluated_at)::timestamptz - make_interval(days => sqlc.arg(newer_days)::int)
      )
    )
),
ranked AS (
  SELECT rc.*,
    ROW_NUMBER() OVER (
      PARTITION BY rc.lineage_key
      ORDER BY
        CASE rc.risk_level
          WHEN 'critical' THEN 1
          WHEN 'high' THEN 2
          WHEN 'medium' THEN 3
          WHEN 'low' THEN 4
          ELSE 5
        END ASC,
        COALESCE(rc.expires_at_source, 'infinity'::timestamptz) DESC,
        COALESCE(rc.created_at_source, '-infinity'::timestamptz) DESC,
        rc.id DESC
    ) AS lineage_rank,
    COUNT(*) OVER (PARTITION BY rc.lineage_key) AS lineage_version_count
  FROM rated_credentials rc
),
latest AS (
  SELECT * FROM ranked WHERE lineage_rank = 1
)
SELECT
  rc.*,
  rc.lineage_version_count::bigint AS version_count
FROM latest rc
WHERE
  (
    cardinality(sqlc.arg(risk_levels)::text[]) = 0
    OR rc.risk_level = ANY(sqlc.arg(risk_levels)::text[])
  )
  AND (
    sqlc.arg(expiry_state)::text = ''
    OR (
      sqlc.arg(expiry_state)::text = 'expired'
      AND rc.expires_at_source IS NOT NULL
      AND rc.expires_at_source < sqlc.arg(evaluated_at)::timestamptz
    )
    OR (
      sqlc.arg(expiry_state)::text = 'active'
      AND (rc.expires_at_source IS NULL OR rc.expires_at_source >= sqlc.arg(evaluated_at)::timestamptz)
    )
  )
  AND (
    sqlc.arg(expires_in_days)::int <= 0
    OR (
      rc.expires_at_source IS NOT NULL
      AND rc.expires_at_source >= sqlc.arg(evaluated_at)::timestamptz
      AND rc.expires_at_source <= sqlc.arg(evaluated_at)::timestamptz + make_interval(days => sqlc.arg(expires_in_days)::int)
    )
  )
  AND (
    sqlc.arg(query)::text = ''
    OR rc.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.asset_ref_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.created_by_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.approved_by_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.asset_name ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
ORDER BY
  CASE WHEN sqlc.arg(sort_by)::text = 'risk' THEN
    CASE rc.risk_level
      WHEN 'critical' THEN 1
      WHEN 'high' THEN 2
      WHEN 'medium' THEN 3
      WHEN 'low' THEN 4
      ELSE 5
    END
  END ASC NULLS LAST,
  CASE WHEN sqlc.arg(sort_by)::text = 'asset' THEN lower(COALESCE(NULLIF(trim(rc.asset_name), ''), rc.asset_ref_external_id)) END ASC NULLS LAST,
  CASE WHEN sqlc.arg(sort_by)::text = 'credential' THEN lower(COALESCE(NULLIF(trim(rc.display_name), ''), rc.external_id)) END ASC NULLS LAST,
  COALESCE(rc.expires_at_source, 'infinity'::timestamptz) ASC,
  lower(COALESCE(NULLIF(trim(rc.display_name), ''), rc.external_id)) ASC,
  rc.source_kind ASC,
  rc.source_name ASC,
  rc.id ASC
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: ListCredentialArtifactsForExportBySourcesAndQueryAndFilters :many
-- Export-variant of the credentials list query. Returns every distinct active
-- credential matching the filters, with no lineage collapse, so two
-- same-named credentials with different external_id values both appear in the
-- exported CSV. lineage_version_count is still surfaced so each row can carry
-- a "this credential is part of an N-version lineage" hint for triage.
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
),
rated_credentials AS (
  SELECT
    ca.*,
    COALESCE(risk.risk_level, 'low')::text AS risk_level,
    COALESCE(NULLIF(trim(aa.display_name), ''), '')::text AS asset_name
  FROM credential_artifacts ca
  LEFT JOIN credential_artifact_risk_read_models risk
    ON risk.credential_artifact_id = ca.id
  JOIN configured_sources cs
    ON cs.source_kind = ca.source_kind
   AND cs.source_name = ca.source_name
  LEFT JOIN app_assets aa
    ON aa.source_kind = ca.source_kind
   AND aa.source_name = ca.source_name
   AND aa.expired_at IS NULL
   AND aa.external_id = CASE
        WHEN strpos(ca.asset_ref_external_id, ':') > 0 THEN substr(ca.asset_ref_external_id, strpos(ca.asset_ref_external_id, ':') + 1)
        ELSE ca.asset_ref_external_id
      END
   AND (
        strpos(ca.asset_ref_external_id, ':') = 0
        OR aa.asset_kind = split_part(ca.asset_ref_external_id, ':', 1)
      )
  WHERE
    ca.expired_at IS NULL
    AND ca.last_observed_run_id IS NOT NULL
    AND (
      cardinality(sqlc.arg(credential_kinds)::text[]) = 0
      OR ca.credential_kind = ANY(sqlc.arg(credential_kinds)::text[])
    )
    AND (
      sqlc.arg(status)::text = ''
      OR lower(ca.status) = lower(sqlc.arg(status)::text)
    )
    AND (
      sqlc.arg(owner)::text = ''
      OR ca.created_by_external_id ILIKE ('%' || sqlc.arg(owner)::text || '%')
      OR ca.created_by_display_name ILIKE ('%' || sqlc.arg(owner)::text || '%')
      OR ca.approved_by_external_id ILIKE ('%' || sqlc.arg(owner)::text || '%')
      OR ca.approved_by_display_name ILIKE ('%' || sqlc.arg(owner)::text || '%')
    )
    AND (
      sqlc.arg(asset)::text = ''
      OR COALESCE(NULLIF(trim(aa.display_name), ''), '') ILIKE ('%' || sqlc.arg(asset)::text || '%')
      OR ca.asset_ref_external_id ILIKE ('%' || sqlc.arg(asset)::text || '%')
      OR ca.asset_ref_kind ILIKE ('%' || sqlc.arg(asset)::text || '%')
    )
    AND (
      sqlc.arg(newer_days)::int <= 0
      OR (
        ca.created_at_source IS NOT NULL
        AND ca.created_at_source >= sqlc.arg(evaluated_at)::timestamptz - make_interval(days => sqlc.arg(newer_days)::int)
      )
    )
),
versioned AS (
  SELECT rc.*,
    COUNT(*) OVER (PARTITION BY rc.lineage_key) AS lineage_version_count
  FROM rated_credentials rc
)
SELECT
  rc.*,
  rc.lineage_version_count::bigint AS version_count
FROM versioned rc
WHERE
  (
    cardinality(sqlc.arg(risk_levels)::text[]) = 0
    OR rc.risk_level = ANY(sqlc.arg(risk_levels)::text[])
  )
  AND (
    sqlc.arg(expiry_state)::text = ''
    OR (
      sqlc.arg(expiry_state)::text = 'expired'
      AND rc.expires_at_source IS NOT NULL
      AND rc.expires_at_source < sqlc.arg(evaluated_at)::timestamptz
    )
    OR (
      sqlc.arg(expiry_state)::text = 'active'
      AND (rc.expires_at_source IS NULL OR rc.expires_at_source >= sqlc.arg(evaluated_at)::timestamptz)
    )
  )
  AND (
    sqlc.arg(expires_in_days)::int <= 0
    OR (
      rc.expires_at_source IS NOT NULL
      AND rc.expires_at_source >= sqlc.arg(evaluated_at)::timestamptz
      AND rc.expires_at_source <= sqlc.arg(evaluated_at)::timestamptz + make_interval(days => sqlc.arg(expires_in_days)::int)
    )
  )
  AND (
    sqlc.arg(query)::text = ''
    OR rc.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.asset_ref_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.created_by_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.approved_by_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.asset_name ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
ORDER BY
  CASE WHEN sqlc.arg(sort_by)::text = 'risk' THEN
    CASE rc.risk_level
      WHEN 'critical' THEN 1
      WHEN 'high' THEN 2
      WHEN 'medium' THEN 3
      WHEN 'low' THEN 4
      ELSE 5
    END
  END ASC NULLS LAST,
  CASE WHEN sqlc.arg(sort_by)::text = 'asset' THEN lower(COALESCE(NULLIF(trim(rc.asset_name), ''), rc.asset_ref_external_id)) END ASC NULLS LAST,
  CASE WHEN sqlc.arg(sort_by)::text = 'credential' THEN lower(COALESCE(NULLIF(trim(rc.display_name), ''), rc.external_id)) END ASC NULLS LAST,
  COALESCE(rc.expires_at_source, 'infinity'::timestamptz) ASC,
  lower(COALESCE(NULLIF(trim(rc.display_name), ''), rc.external_id)) ASC,
  rc.source_kind ASC,
  rc.source_name ASC,
  rc.id ASC
LIMIT sqlc.arg(page_limit)::int;

-- name: SummarizeCredentialsBySourceAndQuery :one
-- Returns counts of distinct active credentials for the credentials stat strip
-- and segment chips. Distinct rather than lineage-grouped so that two
-- same-named credentials with different external_id values both contribute to
-- the risk-level counts and are not hidden behind a single lineage row.
-- Respects source/q/credential_kind/owner/asset/newer scoping; segment-style
-- filters (status/risk/expiry) are intentionally excluded so cards can show
-- segment sizes while a segment is active.
WITH rated_credentials AS (
  SELECT
    ca.*,
    COALESCE(risk.risk_level, 'low')::text AS risk_level,
    COALESCE(NULLIF(trim(aa.display_name), ''), '')::text AS asset_name
  FROM credential_artifacts ca
  LEFT JOIN credential_artifact_risk_read_models risk
    ON risk.credential_artifact_id = ca.id
  LEFT JOIN app_assets aa
    ON aa.source_kind = ca.source_kind
   AND aa.source_name = ca.source_name
   AND aa.expired_at IS NULL
   AND aa.external_id = CASE
        WHEN strpos(ca.asset_ref_external_id, ':') > 0 THEN substr(ca.asset_ref_external_id, strpos(ca.asset_ref_external_id, ':') + 1)
        ELSE ca.asset_ref_external_id
      END
   AND (
        strpos(ca.asset_ref_external_id, ':') = 0
        OR aa.asset_kind = split_part(ca.asset_ref_external_id, ':', 1)
      )
  WHERE
    ca.source_kind = sqlc.arg(source_kind)::text
    AND ca.source_name = sqlc.arg(source_name)::text
    AND ca.expired_at IS NULL
    AND ca.last_observed_run_id IS NOT NULL
    AND (
      cardinality(sqlc.arg(credential_kinds)::text[]) = 0
      OR ca.credential_kind = ANY(sqlc.arg(credential_kinds)::text[])
    )
    AND (
      sqlc.arg(owner)::text = ''
      OR ca.created_by_external_id ILIKE ('%' || sqlc.arg(owner)::text || '%')
      OR ca.created_by_display_name ILIKE ('%' || sqlc.arg(owner)::text || '%')
      OR ca.approved_by_external_id ILIKE ('%' || sqlc.arg(owner)::text || '%')
      OR ca.approved_by_display_name ILIKE ('%' || sqlc.arg(owner)::text || '%')
    )
    AND (
      sqlc.arg(asset)::text = ''
      OR COALESCE(NULLIF(trim(aa.display_name), ''), '') ILIKE ('%' || sqlc.arg(asset)::text || '%')
      OR ca.asset_ref_external_id ILIKE ('%' || sqlc.arg(asset)::text || '%')
      OR ca.asset_ref_kind ILIKE ('%' || sqlc.arg(asset)::text || '%')
    )
    AND (
      sqlc.arg(newer_days)::int <= 0
      OR (
        ca.created_at_source IS NOT NULL
        AND ca.created_at_source >= sqlc.arg(evaluated_at)::timestamptz - make_interval(days => sqlc.arg(newer_days)::int)
      )
    )
),
matched AS (
  SELECT *
  FROM rated_credentials rc
  WHERE
    sqlc.arg(query)::text = ''
    OR rc.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.asset_ref_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.created_by_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.approved_by_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.asset_name ILIKE ('%' || sqlc.arg(query)::text || '%')
)
SELECT
  count(*)::bigint AS total,
  count(*) FILTER (
    WHERE m.expires_at_source IS NOT NULL
      AND m.expires_at_source < sqlc.arg(evaluated_at)::timestamptz
  )::bigint AS expired,
  count(*) FILTER (
    WHERE m.expires_at_source IS NOT NULL
      AND m.expires_at_source >= sqlc.arg(evaluated_at)::timestamptz
      AND m.expires_at_source <= sqlc.arg(evaluated_at)::timestamptz + make_interval(days => 30)
  )::bigint AS expiring_soon,
  count(*) FILTER (WHERE m.risk_level = 'critical')::bigint AS critical,
  count(*) FILTER (WHERE m.risk_level = 'high')::bigint AS high,
  -- The "warning" tier mixes medium-risk credentials with non-critical/high
  -- credentials inside the 30-day expiry window. If the expiry window
  -- elsewhere (CredentialExpiryTextClass, expiryWordAndTone in helpers.go) is
  -- ever retuned, update the interval below to keep the KPI consistent.
  count(*) FILTER (
    WHERE m.risk_level = 'medium'
       OR (
         m.expires_at_source IS NOT NULL
         AND m.expires_at_source >= sqlc.arg(evaluated_at)::timestamptz
         AND m.expires_at_source <= sqlc.arg(evaluated_at)::timestamptz + make_interval(days => 30)
         AND m.risk_level NOT IN ('critical', 'high')
       )
  )::bigint AS warning,
  count(*) FILTER (WHERE lower(m.status) IN ('pending_approval', 'pending'))::bigint AS pending_approval,
  count(*) FILTER (WHERE lower(m.status) = 'revoked')::bigint AS revoked,
  count(*) FILTER (
    WHERE m.expires_at_source IS NULL
       OR m.expires_at_source >= sqlc.arg(evaluated_at)::timestamptz
  )::bigint AS active,
  count(DISTINCT COALESCE(NULLIF(trim(m.asset_name), ''), m.asset_ref_kind || ':' || m.asset_ref_external_id))::bigint AS asset_count
FROM matched m;

-- name: SummarizeCredentialsBySourcesAndQuery :one
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
),
rated_credentials AS (
  SELECT
    ca.*,
    COALESCE(risk.risk_level, 'low')::text AS risk_level,
    COALESCE(NULLIF(trim(aa.display_name), ''), '')::text AS asset_name
  FROM credential_artifacts ca
  LEFT JOIN credential_artifact_risk_read_models risk
    ON risk.credential_artifact_id = ca.id
  JOIN configured_sources cs
    ON cs.source_kind = ca.source_kind
   AND cs.source_name = ca.source_name
  LEFT JOIN app_assets aa
    ON aa.source_kind = ca.source_kind
   AND aa.source_name = ca.source_name
   AND aa.expired_at IS NULL
   AND aa.external_id = CASE
        WHEN strpos(ca.asset_ref_external_id, ':') > 0 THEN substr(ca.asset_ref_external_id, strpos(ca.asset_ref_external_id, ':') + 1)
        ELSE ca.asset_ref_external_id
      END
   AND (
        strpos(ca.asset_ref_external_id, ':') = 0
        OR aa.asset_kind = split_part(ca.asset_ref_external_id, ':', 1)
      )
  WHERE
    ca.expired_at IS NULL
    AND ca.last_observed_run_id IS NOT NULL
    AND (
      cardinality(sqlc.arg(credential_kinds)::text[]) = 0
      OR ca.credential_kind = ANY(sqlc.arg(credential_kinds)::text[])
    )
    AND (
      sqlc.arg(owner)::text = ''
      OR ca.created_by_external_id ILIKE ('%' || sqlc.arg(owner)::text || '%')
      OR ca.created_by_display_name ILIKE ('%' || sqlc.arg(owner)::text || '%')
      OR ca.approved_by_external_id ILIKE ('%' || sqlc.arg(owner)::text || '%')
      OR ca.approved_by_display_name ILIKE ('%' || sqlc.arg(owner)::text || '%')
    )
    AND (
      sqlc.arg(asset)::text = ''
      OR COALESCE(NULLIF(trim(aa.display_name), ''), '') ILIKE ('%' || sqlc.arg(asset)::text || '%')
      OR ca.asset_ref_external_id ILIKE ('%' || sqlc.arg(asset)::text || '%')
      OR ca.asset_ref_kind ILIKE ('%' || sqlc.arg(asset)::text || '%')
    )
    AND (
      sqlc.arg(newer_days)::int <= 0
      OR (
        ca.created_at_source IS NOT NULL
        AND ca.created_at_source >= sqlc.arg(evaluated_at)::timestamptz - make_interval(days => sqlc.arg(newer_days)::int)
      )
    )
),
matched AS (
  SELECT *
  FROM rated_credentials rc
  WHERE
    sqlc.arg(query)::text = ''
    OR rc.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.asset_ref_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.created_by_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.approved_by_external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR rc.asset_name ILIKE ('%' || sqlc.arg(query)::text || '%')
)
SELECT
  count(*)::bigint AS total,
  count(*) FILTER (
    WHERE m.expires_at_source IS NOT NULL
      AND m.expires_at_source < sqlc.arg(evaluated_at)::timestamptz
  )::bigint AS expired,
  count(*) FILTER (
    WHERE m.expires_at_source IS NOT NULL
      AND m.expires_at_source >= sqlc.arg(evaluated_at)::timestamptz
      AND m.expires_at_source <= sqlc.arg(evaluated_at)::timestamptz + make_interval(days => 30)
  )::bigint AS expiring_soon,
  count(*) FILTER (WHERE m.risk_level = 'critical')::bigint AS critical,
  count(*) FILTER (WHERE m.risk_level = 'high')::bigint AS high,
  count(*) FILTER (
    WHERE m.risk_level = 'medium'
       OR (
         m.expires_at_source IS NOT NULL
         AND m.expires_at_source >= sqlc.arg(evaluated_at)::timestamptz
         AND m.expires_at_source <= sqlc.arg(evaluated_at)::timestamptz + make_interval(days => 30)
         AND m.risk_level NOT IN ('critical', 'high')
       )
  )::bigint AS warning,
  count(*) FILTER (WHERE lower(m.status) IN ('pending_approval', 'pending'))::bigint AS pending_approval,
  count(*) FILTER (WHERE lower(m.status) = 'revoked')::bigint AS revoked,
  count(*) FILTER (
    WHERE m.expires_at_source IS NULL
       OR m.expires_at_source >= sqlc.arg(evaluated_at)::timestamptz
  )::bigint AS active,
  count(DISTINCT COALESCE(NULLIF(trim(m.asset_name), ''), m.asset_ref_kind || ':' || m.asset_ref_external_id))::bigint AS asset_count
FROM matched m;

-- name: ListCredentialArtifactsForAssetRef :many
SELECT
  ca.*,
  COALESCE(risk.risk_level, 'low')::text AS risk_level
FROM credential_artifacts ca
LEFT JOIN credential_artifact_risk_read_models risk
  ON risk.credential_artifact_id = ca.id
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
SELECT
  ca.*,
  COALESCE(risk.risk_level, 'low')::text AS risk_level,
  COALESCE(risk.risk_signals_json, '[]'::jsonb)::jsonb AS risk_signals_json,
  COALESCE(risk.policy_packs_json, '[]'::jsonb)::jsonb AS policy_packs_json
FROM credential_artifacts ca
LEFT JOIN credential_artifact_risk_read_models risk
  ON risk.credential_artifact_id = ca.id
WHERE ca.id = sqlc.arg(id)::bigint
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

-- name: RefreshCredentialArtifactLifecycleStatusesBySource :execrows
WITH next_status AS (
  SELECT
    id,
    CASE
      WHEN expires_at_source IS NOT NULL AND expires_at_source < now() THEN 'expired'
      WHEN created_at_source IS NOT NULL AND created_at_source > now() THEN 'inactive'
      ELSE 'active'
    END AS status
  FROM credential_artifacts
  WHERE source_kind = sqlc.arg(source_kind)::text
    AND source_name = sqlc.arg(source_name)::text
    AND expired_at IS NULL
    AND last_observed_run_id IS NOT NULL
)
UPDATE credential_artifacts AS ca
SET
  status = next_status.status,
  updated_at = now()
FROM next_status
WHERE ca.id = next_status.id
  AND ca.status IS DISTINCT FROM next_status.status;

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

-- name: ExpireCredentialArtifactsForAssetRefsNotSeenInRunBySource :execrows
UPDATE credential_artifacts
SET
  expired_at = now(),
  expired_run_id = sqlc.arg(expired_run_id)::bigint
WHERE source_kind = sqlc.arg(source_kind)::text
  AND source_name = sqlc.arg(source_name)::text
  AND asset_ref_kind = 'app_asset'
  AND asset_ref_external_id = ANY(sqlc.arg(asset_ref_external_ids)::text[])
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
  AND (
    seen_in_run_id <> sqlc.arg(expired_run_id)::bigint
    OR seen_in_run_id IS NULL
  );

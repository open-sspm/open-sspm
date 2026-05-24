-- name: CreateIdentity :one
INSERT INTO identities (kind, display_name, primary_email)
VALUES (
  COALESCE(NULLIF(trim(sqlc.arg(kind)::text), ''), 'unknown'),
  COALESCE(sqlc.arg(display_name)::text, ''),
  lower(trim(COALESCE(sqlc.arg(primary_email)::text, '')))
)
RETURNING *;

-- name: FindUnambiguousIdentityByPrimaryEmail :one
-- Returns the single identity matching the email iff there is a strict winner
-- at the top tier. Only configured sources can grant authoritative tie-break
-- status; every existing identity with the email remains a duplicate candidate
-- so retired rows do not cause duplicate identity creation.
-- Returns no row when the email matches zero identities, or when two or more
-- identities tie at the top tier.
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
),
configured_accounts AS (
  SELECT DISTINCT ia.identity_id, a.source_kind, a.source_name
  FROM identity_accounts ia
  JOIN accounts a ON a.id = ia.account_id
  JOIN configured_sources cs
    ON cs.source_kind = a.source_kind
   AND cs.source_name = a.source_name
  WHERE a.expired_at IS NULL
    AND a.last_observed_run_id IS NOT NULL
),
authoritative_identities AS (
  SELECT DISTINCT ca.identity_id
  FROM configured_accounts ca
  JOIN identity_source_settings iss
    ON iss.source_kind = ca.source_kind
   AND iss.source_name = ca.source_name
   AND iss.is_authoritative
),
candidates AS (
  SELECT
    i.id,
    (ai.identity_id IS NOT NULL) AS is_authoritative
  FROM identities i
  LEFT JOIN authoritative_identities ai ON ai.identity_id = i.id
  WHERE lower(trim(i.primary_email)) = lower(trim(sqlc.arg(primary_email)::text))
),
top_tier AS (
  SELECT id
  FROM candidates
  WHERE is_authoritative = (SELECT bool_or(is_authoritative) FROM candidates)
)
SELECT i.*
FROM identities i
JOIN top_tier t ON t.id = i.id
WHERE (SELECT count(*) FROM top_tier) = 1;

-- name: ResolveIdentityByPrimaryEmail :one
-- Returns a deterministic existing identity and the link reason to use for an
-- email match in one statement, so the resolver observes a single snapshot.
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
),
configured_accounts AS (
  SELECT DISTINCT ia.identity_id, a.source_kind, a.source_name
  FROM identity_accounts ia
  JOIN accounts a ON a.id = ia.account_id
  JOIN configured_sources cs
    ON cs.source_kind = a.source_kind
   AND cs.source_name = a.source_name
  WHERE a.expired_at IS NULL
    AND a.last_observed_run_id IS NOT NULL
),
authoritative_identities AS (
  SELECT DISTINCT ca.identity_id
  FROM configured_accounts ca
  JOIN identity_source_settings iss
    ON iss.source_kind = ca.source_kind
   AND iss.source_name = ca.source_name
   AND iss.is_authoritative
),
candidates AS (
  SELECT
    i.id,
    (ai.identity_id IS NOT NULL) AS is_authoritative
  FROM identities i
  LEFT JOIN authoritative_identities ai ON ai.identity_id = i.id
  WHERE lower(trim(i.primary_email)) = lower(trim(sqlc.arg(primary_email)::text))
),
top_tier AS (
  SELECT
    id,
    count(*) OVER () AS top_tier_count
  FROM candidates
  WHERE is_authoritative = (SELECT bool_or(is_authoritative) FROM candidates)
)
SELECT
  id AS identity_id,
  CASE
    WHEN top_tier_count = 1 THEN 'auto_email'::text
    ELSE 'auto_provisional_ambiguous_email'::text
  END AS link_reason
FROM top_tier
ORDER BY id ASC
LIMIT 1;

-- name: CountIdentitiesByPrimaryEmail :one
SELECT count(*)
FROM identities
WHERE lower(trim(primary_email)) = lower(trim(sqlc.arg(primary_email)::text));

-- name: UpdateIdentityAttributes :exec
UPDATE identities
SET
  display_name = COALESCE(sqlc.arg(display_name)::text, identities.display_name),
  primary_email = lower(trim(COALESCE(sqlc.arg(primary_email)::text, identities.primary_email))),
  kind = COALESCE(NULLIF(trim(sqlc.arg(kind)::text), ''), identities.kind),
  updated_at = now()
WHERE id = sqlc.arg(id)::bigint;

-- name: CountIdentitiesInventoryByFilters :one
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
),
all_active_accounts AS (
  SELECT
    ia.identity_id,
    a.id AS account_id,
    a.source_kind,
    a.source_name,
    a.external_id,
    a.created_at,
    a.last_observed_at,
    lower(trim(COALESCE(NULLIF(a.status, ''), NULLIF(a.raw_json->>'status', ''), 'unknown'))) AS normalized_status
  FROM identity_accounts ia
  JOIN accounts a ON a.id = ia.account_id
  JOIN configured_sources cs
    ON cs.source_kind = a.source_kind
   AND cs.source_name = a.source_name
  WHERE a.expired_at IS NULL
    AND a.last_observed_run_id IS NOT NULL
),
filtered_source_accounts AS (
  SELECT *
  FROM all_active_accounts aa
  WHERE (
      sqlc.arg(source_kind)::text = ''
      OR aa.source_kind = sqlc.arg(source_kind)::text
    )
    AND (
      sqlc.arg(source_name)::text = ''
      OR aa.source_name = sqlc.arg(source_name)::text
    )
),
filtered_identities AS (
	SELECT
	  i.id,
	  COALESCE(NULLIF(trim(i.kind), ''), 'unknown') AS identity_type
  FROM identities i
  WHERE
    (
      sqlc.arg(query)::text = ''
      OR i.primary_email ILIKE ('%' || sqlc.arg(query)::text || '%')
      OR i.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
      OR EXISTS (
        SELECT 1
        FROM all_active_accounts aa
        WHERE aa.identity_id = i.id
          AND aa.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
      )
    )
    AND (
      sqlc.arg(identity_type)::text = ''
      OR i.kind = sqlc.arg(identity_type)::text
      OR (sqlc.arg(identity_type)::text = 'human' AND i.kind = 'unknown')
    )
),
candidate_identities AS (
  SELECT fi.*
  FROM filtered_identities fi
  WHERE EXISTS (
    SELECT 1
    FROM filtered_source_accounts fsa
    WHERE fsa.identity_id = fi.id
  )
),
account_rollups AS (
  SELECT
    aa.identity_id,
    MAX(aa.last_observed_at) AS last_seen_at,
    COUNT(*)::bigint AS account_count,
    BOOL_OR(aa.normalized_status IN ('active', 'enabled')) AS has_active,
    BOOL_OR(aa.normalized_status IN ('suspended', 'disabled', 'inactive', 'locked')) AS has_suspended,
    BOOL_AND(aa.normalized_status IN ('deleted', 'deprovisioned', 'terminated')) AS all_deleted,
    BOOL_OR(COALESCE(iss.is_authoritative, FALSE)) AS has_authoritative_anchor
  FROM all_active_accounts aa
  LEFT JOIN identity_source_settings iss
    ON iss.source_kind = aa.source_kind
   AND iss.source_name = aa.source_name
   AND iss.is_authoritative
  GROUP BY aa.identity_id
),
privileged_counts AS (
  SELECT
    aa.identity_id,
    COUNT(DISTINCT e.id)::bigint AS privileged_roles
  FROM all_active_accounts aa
  JOIN entitlements e ON e.app_user_id = aa.account_id
  WHERE e.expired_at IS NULL
    AND e.last_observed_run_id IS NOT NULL
    AND (
      (
        e.kind = 'github_team_repo_permission'
        AND lower(trim(e.permission)) IN ('admin', 'maintain')
      )
      OR (
        e.kind = 'datadog_role'
        AND (
          lower(trim(COALESCE(NULLIF(e.raw_json->>'role_name', ''), NULLIF(split_part(e.resource, ':', 2), '')))) LIKE '%admin%'
          OR lower(trim(COALESCE(NULLIF(e.raw_json->>'role_name', ''), NULLIF(split_part(e.resource, ':', 2), '')))) LIKE '%administrator%'
          OR lower(trim(COALESCE(NULLIF(e.raw_json->>'role_name', ''), NULLIF(split_part(e.resource, ':', 2), '')))) LIKE '%owner%'
        )
      )
      OR (
        e.kind = 'aws_permission_set'
        AND (
          lower(trim(e.permission)) LIKE '%admin%'
          OR lower(trim(e.permission)) LIKE '%administrator%'
          OR lower(trim(e.permission)) LIKE '%poweruser%'
          OR lower(trim(e.permission)) LIKE '%owner%'
          OR lower(trim(e.permission)) LIKE '%root%'
        )
      )
    )
  GROUP BY aa.identity_id
),
base_metrics AS (
	SELECT
	  ci.id,
	  ci.identity_type,
	  COALESCE(ar.has_authoritative_anchor, FALSE)::boolean AS has_authoritative_anchor,
    COALESCE(pc.privileged_roles, 0)::bigint AS privileged_roles,
    ar.last_seen_at::timestamptz AS last_seen_at,
    CASE
      WHEN COALESCE(ar.account_count, 0) = 0 THEN 'orphaned'
      WHEN COALESCE(ar.has_active, FALSE) THEN 'active'
      WHEN COALESCE(ar.all_deleted, FALSE) THEN 'deleted'
      WHEN COALESCE(ar.has_suspended, FALSE) THEN 'suspended'
      ELSE 'unknown'
    END AS status,
    CASE
      WHEN ar.last_seen_at IS NULL THEN 'never_seen'
      WHEN ar.last_seen_at >= now() - interval '30 days' THEN 'recent'
      WHEN ar.last_seen_at >= now() - interval '90 days' THEN 'aging'
      ELSE 'stale'
    END AS activity_state
  FROM candidate_identities ci
  LEFT JOIN account_rollups ar ON ar.identity_id = ci.id
  LEFT JOIN privileged_counts pc ON pc.identity_id = ci.id
),
base AS (
  SELECT
    bm.*,
    CASE
      WHEN bm.identity_type IN ('service', 'bot') THEN 'not_applicable'
      WHEN bm.has_authoritative_anchor THEN 'anchored'
      ELSE 'missing_anchor'
    END AS anchor_state,
    CASE
      WHEN bm.identity_type NOT IN ('service', 'bot')
           AND NOT bm.has_authoritative_anchor
           AND bm.privileged_roles > 0 THEN 'action_required'
      WHEN bm.privileged_roles > 0 AND bm.activity_state IN ('stale', 'never_seen') THEN 'action_required'
      WHEN bm.identity_type NOT IN ('service', 'bot')
           AND NOT bm.has_authoritative_anchor THEN 'review'
      WHEN bm.activity_state IN ('aging', 'stale', 'never_seen') THEN 'review'
      ELSE 'healthy'
    END AS row_state
  FROM base_metrics bm
)
SELECT COUNT(*)
FROM base b
WHERE
  (
    sqlc.arg(anchor_state)::text = ''
    OR b.anchor_state = sqlc.arg(anchor_state)::text
  )
  AND (
    sqlc.arg(privileged_only)::bool = FALSE
    OR b.privileged_roles > 0
  )
  AND (
    sqlc.arg(status)::text = ''
    OR b.status = sqlc.arg(status)::text
  )
  AND (
    sqlc.arg(activity_state)::text = ''
    OR b.activity_state = sqlc.arg(activity_state)::text
  )
  AND (
    sqlc.arg(row_state)::text = ''
    OR b.row_state = sqlc.arg(row_state)::text
  );

-- name: ListIdentitiesInventoryPageByFilters :many
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
),
all_active_accounts AS (
  SELECT
    ia.identity_id,
    a.id AS account_id,
    a.source_kind,
    a.source_name,
    a.external_id,
    a.created_at,
    a.last_observed_at,
    lower(trim(COALESCE(NULLIF(a.status, ''), NULLIF(a.raw_json->>'status', ''), 'unknown'))) AS normalized_status
  FROM identity_accounts ia
  JOIN accounts a ON a.id = ia.account_id
  JOIN configured_sources cs
    ON cs.source_kind = a.source_kind
   AND cs.source_name = a.source_name
  WHERE a.expired_at IS NULL
    AND a.last_observed_run_id IS NOT NULL
),
filtered_source_accounts AS (
  SELECT *
  FROM all_active_accounts aa
  WHERE (
      sqlc.arg(source_kind)::text = ''
      OR aa.source_kind = sqlc.arg(source_kind)::text
    )
    AND (
      sqlc.arg(source_name)::text = ''
      OR aa.source_name = sqlc.arg(source_name)::text
    )
),
filtered_identities AS (
  SELECT
    i.id,
    i.display_name,
    i.primary_email,
    i.kind AS identity_type,
    i.created_at AS identity_created_at
  FROM identities i
  WHERE
    (
      sqlc.arg(query)::text = ''
      OR i.primary_email ILIKE ('%' || sqlc.arg(query)::text || '%')
      OR i.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
      OR EXISTS (
        SELECT 1
        FROM all_active_accounts aa
        WHERE aa.identity_id = i.id
          AND aa.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
      )
    )
    AND (
      sqlc.arg(identity_type)::text = ''
      OR i.kind = sqlc.arg(identity_type)::text
      OR (sqlc.arg(identity_type)::text = 'human' AND i.kind = 'unknown')
    )
),
candidate_identities AS (
  SELECT fi.*
  FROM filtered_identities fi
  WHERE EXISTS (
    SELECT 1
    FROM filtered_source_accounts fsa
    WHERE fsa.identity_id = fi.id
  )
),
account_rollups AS (
  SELECT
    aa.identity_id,
    COUNT(DISTINCT (aa.source_kind, aa.source_name))::bigint AS integration_count,
    MAX(aa.last_observed_at) AS last_seen_at,
    MIN(aa.created_at) AS first_seen_at,
    COUNT(*)::bigint AS account_count,
    BOOL_OR(aa.normalized_status IN ('active', 'enabled')) AS has_active,
    BOOL_OR(aa.normalized_status IN ('suspended', 'disabled', 'inactive', 'locked')) AS has_suspended,
    BOOL_AND(aa.normalized_status IN ('deleted', 'deprovisioned', 'terminated')) AS all_deleted,
    BOOL_OR(COALESCE(iss.is_authoritative, FALSE)) AS has_authoritative_anchor
  FROM all_active_accounts aa
  LEFT JOIN identity_source_settings iss
    ON iss.source_kind = aa.source_kind
   AND iss.source_name = aa.source_name
   AND iss.is_authoritative
  GROUP BY aa.identity_id
),
privileged_counts AS (
  SELECT
    aa.identity_id,
    COUNT(DISTINCT e.id)::bigint AS privileged_roles
  FROM all_active_accounts aa
  JOIN entitlements e ON e.app_user_id = aa.account_id
  WHERE e.expired_at IS NULL
    AND e.last_observed_run_id IS NOT NULL
    AND (
      (
        e.kind = 'github_team_repo_permission'
        AND lower(trim(e.permission)) IN ('admin', 'maintain')
      )
      OR (
        e.kind = 'datadog_role'
        AND (
          lower(trim(COALESCE(NULLIF(e.raw_json->>'role_name', ''), NULLIF(split_part(e.resource, ':', 2), '')))) LIKE '%admin%'
          OR lower(trim(COALESCE(NULLIF(e.raw_json->>'role_name', ''), NULLIF(split_part(e.resource, ':', 2), '')))) LIKE '%administrator%'
          OR lower(trim(COALESCE(NULLIF(e.raw_json->>'role_name', ''), NULLIF(split_part(e.resource, ':', 2), '')))) LIKE '%owner%'
        )
      )
      OR (
        e.kind = 'aws_permission_set'
        AND (
          lower(trim(e.permission)) LIKE '%admin%'
          OR lower(trim(e.permission)) LIKE '%administrator%'
          OR lower(trim(e.permission)) LIKE '%poweruser%'
          OR lower(trim(e.permission)) LIKE '%owner%'
          OR lower(trim(e.permission)) LIKE '%root%'
        )
      )
    )
  GROUP BY aa.identity_id
),
primary_source AS (
  SELECT DISTINCT ON (fsa.identity_id)
    fsa.identity_id,
    fsa.source_kind,
    fsa.source_name
  FROM filtered_source_accounts fsa
  LEFT JOIN identity_source_settings iss
    ON iss.source_kind = fsa.source_kind
   AND iss.source_name = fsa.source_name
   AND iss.is_authoritative
  ORDER BY fsa.identity_id, (iss.is_authoritative IS NOT TRUE), fsa.account_id
),
base_metrics AS (
  SELECT
    ci.id,
    ci.display_name,
    ci.primary_email,
    ci.identity_type,
    COALESCE(ar.has_authoritative_anchor, FALSE)::boolean AS has_authoritative_anchor,
    COALESCE(ps.source_kind, '') AS source_kind,
    COALESCE(ps.source_name, '') AS source_name,
    COALESCE(ar.integration_count, 0)::bigint AS integration_count,
    COALESCE(pc.privileged_roles, 0)::bigint AS privileged_roles,
    ar.last_seen_at::timestamptz AS last_seen_at,
    COALESCE(ar.first_seen_at, ci.identity_created_at)::timestamptz AS first_seen_at,
    CASE
      WHEN COALESCE(ar.account_count, 0) = 0 THEN 'orphaned'
      WHEN COALESCE(ar.has_active, FALSE) THEN 'active'
      WHEN COALESCE(ar.all_deleted, FALSE) THEN 'deleted'
      WHEN COALESCE(ar.has_suspended, FALSE) THEN 'suspended'
      ELSE 'unknown'
    END AS status,
    CASE
      WHEN ar.last_seen_at IS NULL THEN 'never_seen'
      WHEN ar.last_seen_at >= now() - interval '30 days' THEN 'recent'
      WHEN ar.last_seen_at >= now() - interval '90 days' THEN 'aging'
      ELSE 'stale'
    END AS activity_state
  FROM candidate_identities ci
  LEFT JOIN account_rollups ar ON ar.identity_id = ci.id
  LEFT JOIN primary_source ps ON ps.identity_id = ci.id
  LEFT JOIN privileged_counts pc ON pc.identity_id = ci.id
),
base AS (
  SELECT
    bm.*,
    CASE
      WHEN bm.identity_type IN ('service', 'bot') THEN 'not_applicable'
      WHEN bm.has_authoritative_anchor THEN 'anchored'
      ELSE 'missing_anchor'
    END AS anchor_state,
    CASE
      WHEN bm.identity_type NOT IN ('service', 'bot')
           AND NOT bm.has_authoritative_anchor
           AND bm.privileged_roles > 0 THEN 'action_required'
      WHEN bm.privileged_roles > 0 AND bm.activity_state IN ('stale', 'never_seen') THEN 'action_required'
      WHEN bm.identity_type NOT IN ('service', 'bot')
           AND NOT bm.has_authoritative_anchor THEN 'review'
      WHEN bm.activity_state IN ('aging', 'stale', 'never_seen') THEN 'review'
      ELSE 'healthy'
    END AS row_state
  FROM base_metrics bm
)
SELECT
  b.id,
  b.display_name,
  b.primary_email,
  b.identity_type,
  b.anchor_state,
  b.source_kind,
  b.source_name,
  b.integration_count,
  b.privileged_roles,
  b.last_seen_at,
  b.first_seen_at,
  b.status,
  b.activity_state,
  b.row_state,
  COUNT(*) OVER()::bigint AS total_count
FROM base b
WHERE
  (
    sqlc.arg(anchor_state)::text = ''
    OR b.anchor_state = sqlc.arg(anchor_state)::text
  )
  AND (
    sqlc.arg(privileged_only)::bool = FALSE
    OR b.privileged_roles > 0
  )
  AND (
    sqlc.arg(status)::text = ''
    OR b.status = sqlc.arg(status)::text
  )
  AND (
    sqlc.arg(activity_state)::text = ''
    OR b.activity_state = sqlc.arg(activity_state)::text
  )
  AND (
    sqlc.arg(row_state)::text = ''
    OR b.row_state = sqlc.arg(row_state)::text
  )
ORDER BY
  CASE
    WHEN sqlc.arg(sort_by)::text = '' THEN
      CASE b.row_state
        WHEN 'action_required' THEN 0
        WHEN 'review' THEN 1
        ELSE 2
      END
  END ASC,
  CASE WHEN sqlc.arg(sort_by)::text = '' THEN b.privileged_roles END DESC,
  CASE WHEN sqlc.arg(sort_by)::text = '' THEN b.last_seen_at END ASC NULLS FIRST,
  CASE WHEN sqlc.arg(sort_by)::text = '' THEN b.id END DESC,

  CASE
    WHEN sqlc.arg(sort_by)::text = 'identity'
      AND sqlc.arg(sort_dir)::text = 'asc'
    THEN lower(COALESCE(NULLIF(trim(b.display_name), ''), NULLIF(trim(b.primary_email), ''), 'identity ' || b.id::text))
  END ASC,
  CASE
    WHEN sqlc.arg(sort_by)::text = 'identity'
      AND sqlc.arg(sort_dir)::text = 'desc'
    THEN lower(COALESCE(NULLIF(trim(b.display_name), ''), NULLIF(trim(b.primary_email), ''), 'identity ' || b.id::text))
  END DESC,

  CASE
    WHEN sqlc.arg(sort_by)::text = 'identity_type'
      AND sqlc.arg(sort_dir)::text = 'asc'
    THEN lower(b.identity_type)
  END ASC,
  CASE
    WHEN sqlc.arg(sort_by)::text = 'identity_type'
      AND sqlc.arg(sort_dir)::text = 'desc'
    THEN lower(b.identity_type)
  END DESC,

  CASE
    WHEN sqlc.arg(sort_by)::text = 'anchor'
      AND sqlc.arg(sort_dir)::text = 'asc'
    THEN CASE b.anchor_state WHEN 'anchored' THEN 0 WHEN 'missing_anchor' THEN 1 ELSE 2 END
  END ASC,
  CASE
    WHEN sqlc.arg(sort_by)::text = 'anchor'
      AND sqlc.arg(sort_dir)::text = 'desc'
    THEN CASE b.anchor_state WHEN 'missing_anchor' THEN 2 WHEN 'anchored' THEN 1 ELSE 0 END
  END DESC,

  CASE
    WHEN sqlc.arg(sort_by)::text = 'source_type'
      AND sqlc.arg(sort_dir)::text = 'asc'
    THEN NULLIF(lower(trim(b.source_kind)), '')
  END ASC NULLS LAST,
  CASE
    WHEN sqlc.arg(sort_by)::text = 'source_type'
      AND sqlc.arg(sort_dir)::text = 'desc'
    THEN NULLIF(lower(trim(b.source_kind)), '')
  END DESC NULLS LAST,

  CASE
    WHEN sqlc.arg(sort_by)::text = 'linked_sources'
      AND sqlc.arg(sort_dir)::text = 'asc'
    THEN b.integration_count
  END ASC,
  CASE
    WHEN sqlc.arg(sort_by)::text = 'linked_sources'
      AND sqlc.arg(sort_dir)::text = 'desc'
    THEN b.integration_count
  END DESC,

  CASE
    WHEN sqlc.arg(sort_by)::text = 'privileged_roles'
      AND sqlc.arg(sort_dir)::text = 'asc'
    THEN b.privileged_roles
  END ASC,
  CASE
    WHEN sqlc.arg(sort_by)::text = 'privileged_roles'
      AND sqlc.arg(sort_dir)::text = 'desc'
    THEN b.privileged_roles
  END DESC,

  CASE
    WHEN sqlc.arg(sort_by)::text = 'status'
      AND sqlc.arg(sort_dir)::text = 'asc'
    THEN
      CASE b.status
        WHEN 'active' THEN 0
        WHEN 'suspended' THEN 1
        WHEN 'deleted' THEN 2
        WHEN 'orphaned' THEN 3
        ELSE 4
      END
  END ASC,
  CASE
    WHEN sqlc.arg(sort_by)::text = 'status'
      AND sqlc.arg(sort_dir)::text = 'desc'
    THEN
      CASE b.status
        WHEN 'active' THEN 0
        WHEN 'suspended' THEN 1
        WHEN 'deleted' THEN 2
        WHEN 'orphaned' THEN 3
        ELSE 4
      END
  END DESC,

  CASE
    WHEN sqlc.arg(sort_by)::text = 'last_seen'
      AND sqlc.arg(sort_dir)::text = 'asc'
    THEN b.last_seen_at
  END ASC NULLS FIRST,
  CASE
    WHEN sqlc.arg(sort_by)::text = 'last_seen'
      AND sqlc.arg(sort_dir)::text = 'desc'
    THEN b.last_seen_at
  END DESC NULLS LAST,
  b.id DESC
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: SummarizeIdentitiesInventoryByFilters :one
-- Returns bucketed counts for the identity inventory, scoped to the
-- user-applied source, search, and identity_type filters but ignoring
-- segment-like filters (anchor, privileged, status, activity_state).
-- The result is used to drive the operator stat strip and segment chips on
-- the identities list: it tells the user the shape of the population they
-- are currently looking at, independent of any segment they have already
-- applied. Buckets are counts, not exclusive categories (an identity can be
-- both privileged and missing an authoritative anchor).
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
),
all_active_accounts AS (
  SELECT
    ia.identity_id,
    a.id AS account_id,
    a.source_kind,
    a.source_name,
    a.external_id,
    a.created_at,
    a.last_observed_at,
    lower(trim(COALESCE(NULLIF(a.status, ''), NULLIF(a.raw_json->>'status', ''), 'unknown'))) AS normalized_status
  FROM identity_accounts ia
  JOIN accounts a ON a.id = ia.account_id
  JOIN configured_sources cs
    ON cs.source_kind = a.source_kind
   AND cs.source_name = a.source_name
  WHERE a.expired_at IS NULL
    AND a.last_observed_run_id IS NOT NULL
),
filtered_source_accounts AS (
  SELECT *
  FROM all_active_accounts aa
  WHERE (
      sqlc.arg(source_kind)::text = ''
      OR aa.source_kind = sqlc.arg(source_kind)::text
    )
    AND (
      sqlc.arg(source_name)::text = ''
      OR aa.source_name = sqlc.arg(source_name)::text
    )
),
filtered_identities AS (
  SELECT
    i.id,
    i.kind AS identity_type
  FROM identities i
  WHERE
    (
      sqlc.arg(query)::text = ''
      OR i.primary_email ILIKE ('%' || sqlc.arg(query)::text || '%')
      OR i.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
      OR EXISTS (
        SELECT 1
        FROM all_active_accounts aa
        WHERE aa.identity_id = i.id
          AND aa.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
      )
    )
    AND (
      sqlc.arg(identity_type)::text = ''
      OR i.kind = sqlc.arg(identity_type)::text
      OR (sqlc.arg(identity_type)::text = 'human' AND i.kind = 'unknown')
    )
),
candidate_identities AS (
  SELECT fi.*
  FROM filtered_identities fi
  WHERE EXISTS (
    SELECT 1
    FROM filtered_source_accounts fsa
    WHERE fsa.identity_id = fi.id
  )
),
account_rollups AS (
  SELECT
    aa.identity_id,
    MAX(aa.last_observed_at) AS last_seen_at,
    COUNT(*)::bigint AS account_count,
    BOOL_OR(aa.normalized_status IN ('active', 'enabled')) AS has_active,
    BOOL_OR(aa.normalized_status IN ('suspended', 'disabled', 'inactive', 'locked')) AS has_suspended,
    BOOL_AND(aa.normalized_status IN ('deleted', 'deprovisioned', 'terminated')) AS all_deleted,
    BOOL_OR(COALESCE(iss.is_authoritative, FALSE)) AS has_authoritative_anchor
  FROM all_active_accounts aa
  LEFT JOIN identity_source_settings iss
    ON iss.source_kind = aa.source_kind
   AND iss.source_name = aa.source_name
   AND iss.is_authoritative
  GROUP BY aa.identity_id
),
privileged_counts AS (
  SELECT
    aa.identity_id,
    COUNT(DISTINCT e.id)::bigint AS privileged_roles
  FROM all_active_accounts aa
  JOIN entitlements e ON e.app_user_id = aa.account_id
  WHERE e.expired_at IS NULL
    AND e.last_observed_run_id IS NOT NULL
    AND (
      (
        e.kind = 'github_team_repo_permission'
        AND lower(trim(e.permission)) IN ('admin', 'maintain')
      )
      OR (
        e.kind = 'datadog_role'
        AND (
          lower(trim(COALESCE(NULLIF(e.raw_json->>'role_name', ''), NULLIF(split_part(e.resource, ':', 2), '')))) LIKE '%admin%'
          OR lower(trim(COALESCE(NULLIF(e.raw_json->>'role_name', ''), NULLIF(split_part(e.resource, ':', 2), '')))) LIKE '%administrator%'
          OR lower(trim(COALESCE(NULLIF(e.raw_json->>'role_name', ''), NULLIF(split_part(e.resource, ':', 2), '')))) LIKE '%owner%'
        )
      )
      OR (
        e.kind = 'aws_permission_set'
        AND (
          lower(trim(e.permission)) LIKE '%admin%'
          OR lower(trim(e.permission)) LIKE '%administrator%'
          OR lower(trim(e.permission)) LIKE '%poweruser%'
          OR lower(trim(e.permission)) LIKE '%owner%'
          OR lower(trim(e.permission)) LIKE '%root%'
        )
      )
    )
  GROUP BY aa.identity_id
),
base_metrics AS (
  SELECT
    ci.id,
    ci.identity_type,
    COALESCE(ar.has_authoritative_anchor, FALSE)::boolean AS has_authoritative_anchor,
    COALESCE(pc.privileged_roles, 0)::bigint AS privileged_roles,
    CASE
      WHEN COALESCE(ar.account_count, 0) = 0 THEN 'orphaned'
      WHEN COALESCE(ar.has_active, FALSE) THEN 'active'
      WHEN COALESCE(ar.all_deleted, FALSE) THEN 'deleted'
      WHEN COALESCE(ar.has_suspended, FALSE) THEN 'suspended'
      ELSE 'unknown'
    END AS status,
    CASE
      WHEN ar.last_seen_at IS NULL THEN 'never_seen'
      WHEN ar.last_seen_at >= now() - interval '30 days' THEN 'recent'
      WHEN ar.last_seen_at >= now() - interval '90 days' THEN 'aging'
      ELSE 'stale'
    END AS activity_state
  FROM candidate_identities ci
  LEFT JOIN account_rollups ar ON ar.identity_id = ci.id
  LEFT JOIN privileged_counts pc ON pc.identity_id = ci.id
),
base AS (
  SELECT
    bm.*,
    CASE
      WHEN bm.identity_type IN ('service', 'bot') THEN 'not_applicable'
      WHEN bm.has_authoritative_anchor THEN 'anchored'
      ELSE 'missing_anchor'
    END AS anchor_state,
    CASE
      WHEN bm.identity_type NOT IN ('service', 'bot')
           AND NOT bm.has_authoritative_anchor
           AND bm.privileged_roles > 0 THEN 'action_required'
      WHEN bm.privileged_roles > 0 AND bm.activity_state IN ('stale', 'never_seen') THEN 'action_required'
      WHEN bm.identity_type NOT IN ('service', 'bot')
           AND NOT bm.has_authoritative_anchor THEN 'review'
      WHEN bm.activity_state IN ('aging', 'stale', 'never_seen') THEN 'review'
      ELSE 'healthy'
    END AS row_state
  FROM base_metrics bm
)
SELECT
  COUNT(*)::bigint                                                          AS total_count,
  COUNT(*) FILTER (WHERE row_state = 'action_required')::bigint             AS action_required_count,
  COUNT(*) FILTER (WHERE row_state = 'review')::bigint                      AS review_count,
  COUNT(*) FILTER (WHERE privileged_roles > 0)::bigint                      AS privileged_count,
  COUNT(*) FILTER (
    WHERE privileged_roles > 0
      AND activity_state = 'stale'
  )::bigint                                                                 AS stale_privileged_count,
  COUNT(*) FILTER (WHERE anchor_state = 'missing_anchor')::bigint            AS missing_anchor_count,
  COUNT(*) FILTER (WHERE status = 'suspended')::bigint                      AS suspended_count,
  COUNT(*) FILTER (WHERE activity_state = 'stale')::bigint                   AS stale_count
FROM base;

-- name: GetIdentitySummaryByID :one
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
),
authoritative_identities AS (
  SELECT DISTINCT ia.identity_id
  FROM identity_accounts ia
  JOIN accounts anchor ON anchor.id = ia.account_id
  JOIN configured_sources cs
    ON cs.source_kind = anchor.source_kind
   AND cs.source_name = anchor.source_name
  JOIN identity_source_settings iss
    ON iss.source_kind = anchor.source_kind
   AND iss.source_name = anchor.source_name
   AND iss.is_authoritative
  WHERE anchor.expired_at IS NULL
    AND anchor.last_observed_run_id IS NOT NULL
)
SELECT
  i.*,
  CASE
    WHEN i.kind IN ('service', 'bot') THEN 'not_applicable'
    WHEN ai.identity_id IS NOT NULL THEN 'anchored'
    ELSE 'missing_anchor'
  END AS anchor_state,
  COUNT(ia.account_id) AS linked_accounts
FROM identities i
LEFT JOIN identity_accounts ia ON ia.identity_id = i.id
LEFT JOIN authoritative_identities ai ON ai.identity_id = i.id
WHERE i.id = $1
GROUP BY i.id, ai.identity_id;

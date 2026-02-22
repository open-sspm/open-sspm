-- name: CreateIdentity :one
INSERT INTO identities (kind, display_name, primary_email)
VALUES (
  COALESCE(NULLIF(trim(sqlc.arg(kind)::text), ''), 'unknown'),
  COALESCE(sqlc.arg(display_name)::text, ''),
  lower(trim(COALESCE(sqlc.arg(primary_email)::text, '')))
)
RETURNING *;

-- name: GetIdentityByID :one
SELECT *
FROM identities
WHERE id = $1;

-- name: GetIdentityByPrimaryEmail :many
SELECT *
FROM identities
WHERE lower(trim(primary_email)) = lower(trim(sqlc.arg(primary_email)::text))
ORDER BY id ASC;

-- name: GetPreferredIdentityByPrimaryEmail :one
WITH authoritative_identities AS (
  SELECT DISTINCT ia.identity_id
  FROM identity_accounts ia
  JOIN accounts anchor ON anchor.id = ia.account_id
  JOIN identity_source_settings iss
    ON iss.source_kind = anchor.source_kind
   AND iss.source_name = anchor.source_name
   AND iss.is_authoritative
  WHERE anchor.expired_at IS NULL
    AND anchor.last_observed_run_id IS NOT NULL
)
SELECT i.*
FROM identities i
LEFT JOIN authoritative_identities ai ON ai.identity_id = i.id
WHERE lower(trim(i.primary_email)) = lower(trim(sqlc.arg(primary_email)::text))
ORDER BY (ai.identity_id IS NOT NULL) DESC, i.id ASC
LIMIT 1;

-- name: UpdateIdentityAttributes :exec
UPDATE identities
SET
  display_name = COALESCE(sqlc.arg(display_name)::text, identities.display_name),
  primary_email = lower(trim(COALESCE(sqlc.arg(primary_email)::text, identities.primary_email))),
  kind = COALESCE(NULLIF(trim(sqlc.arg(kind)::text), ''), identities.kind),
  updated_at = now()
WHERE id = sqlc.arg(id)::bigint;

-- name: CountIdentitiesByQuery :one
SELECT count(*)
FROM identities i
WHERE
  sqlc.arg(query)::text = ''
  OR i.primary_email ILIKE ('%' || sqlc.arg(query)::text || '%')
  OR i.display_name ILIKE ('%' || sqlc.arg(query)::text || '%');

-- name: ListIdentitiesPageByQuery :many
WITH authoritative_identities AS (
  SELECT DISTINCT ia.identity_id
  FROM identity_accounts ia
  JOIN accounts anchor ON anchor.id = ia.account_id
  JOIN identity_source_settings iss
    ON iss.source_kind = anchor.source_kind
   AND iss.source_name = anchor.source_name
   AND iss.is_authoritative
  WHERE anchor.expired_at IS NULL
    AND anchor.last_observed_run_id IS NOT NULL
)
SELECT
  i.*,
  (ai.identity_id IS NOT NULL)::boolean AS managed,
  COUNT(ia.account_id) AS linked_accounts
FROM identities i
LEFT JOIN identity_accounts ia ON ia.identity_id = i.id
LEFT JOIN authoritative_identities ai ON ai.identity_id = i.id
WHERE
  sqlc.arg(query)::text = ''
  OR i.primary_email ILIKE ('%' || sqlc.arg(query)::text || '%')
  OR i.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
GROUP BY i.id, ai.identity_id
ORDER BY i.id DESC
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: ListIdentitiesDirectoryPageByQuery :many
WITH filtered_identities AS (
  SELECT i.id
  FROM identities i
  WHERE
    sqlc.arg(query)::text = ''
    OR i.primary_email ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR i.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
  ORDER BY i.id DESC
  LIMIT sqlc.arg(page_limit)::int
  OFFSET sqlc.arg(page_offset)::int
),
active_accounts AS (
  SELECT
    ia.identity_id,
    a.id AS account_id,
    a.source_kind,
    a.source_name,
    a.created_at,
    a.last_observed_at
  FROM filtered_identities fi
  JOIN identity_accounts ia ON ia.identity_id = fi.id
  JOIN accounts a ON a.id = ia.account_id
  WHERE a.expired_at IS NULL
    AND a.last_observed_run_id IS NOT NULL
),
authoritative_identities AS (
  SELECT DISTINCT aa.identity_id
  FROM active_accounts aa
  JOIN identity_source_settings iss
    ON iss.source_kind = aa.source_kind
   AND iss.source_name = aa.source_name
   AND iss.is_authoritative
),
integration_counts AS (
  SELECT
    aa.identity_id,
    COUNT(DISTINCT (aa.source_kind, aa.source_name)) AS integration_count
  FROM active_accounts aa
  WHERE trim(aa.source_kind) <> ''
    AND trim(aa.source_name) <> ''
  GROUP BY aa.identity_id
),
privileged_counts AS (
  SELECT
    aa.identity_id,
    COUNT(DISTINCT e.id) AS privileged_roles
  FROM active_accounts aa
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
activity_stats AS (
  SELECT
    aa.identity_id,
    MAX(aa.last_observed_at) AS last_seen_at,
    MIN(aa.created_at) AS first_created_at
  FROM active_accounts aa
  GROUP BY aa.identity_id
)
SELECT
  i.id,
  i.display_name,
  i.primary_email,
  (ai.identity_id IS NOT NULL)::boolean AS managed,
  COALESCE(ic.integration_count, 0)::bigint AS integration_count,
  COALESCE(pc.privileged_roles, 0)::bigint AS privileged_roles,
  ast.last_seen_at::timestamptz AS last_seen_at,
  COALESCE(ast.first_created_at, i.created_at)::timestamptz AS first_created_at
FROM filtered_identities fi
JOIN identities i ON i.id = fi.id
LEFT JOIN authoritative_identities ai ON ai.identity_id = fi.id
LEFT JOIN integration_counts ic ON ic.identity_id = fi.id
LEFT JOIN privileged_counts pc ON pc.identity_id = fi.id
LEFT JOIN activity_stats ast ON ast.identity_id = fi.id
ORDER BY i.id DESC;

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
    lower(trim(COALESCE(NULLIF(a.status, ''), NULLIF(a.raw_json->>'status', ''), 'unknown'))) AS normalized_status,
    ia.confidence,
    trim(ia.link_reason) AS link_reason
  FROM identity_accounts ia
  JOIN accounts a ON a.id = ia.account_id
  JOIN configured_sources cs
    ON cs.source_kind = a.source_kind
   AND cs.source_name = a.source_name
  WHERE a.expired_at IS NULL
    AND a.last_observed_run_id IS NOT NULL
),
filtered_identities AS (
  SELECT i.id
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
    )
),
source_accounts AS (
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
candidate_identities AS (
  SELECT fi.id
  FROM filtered_identities fi
  WHERE (
      sqlc.arg(source_kind)::text = ''
      AND sqlc.arg(source_name)::text = ''
    )
    OR EXISTS (
      SELECT 1
      FROM source_accounts sa
      WHERE sa.identity_id = fi.id
    )
),
managed_identities AS (
  SELECT DISTINCT aa.identity_id
  FROM all_active_accounts aa
  JOIN identity_source_settings iss
    ON iss.source_kind = aa.source_kind
   AND iss.source_name = aa.source_name
   AND iss.is_authoritative
),
integration_counts AS (
  SELECT
    aa.identity_id,
    COUNT(DISTINCT (aa.source_kind, aa.source_name))::bigint AS integration_count
  FROM all_active_accounts aa
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
activity_stats AS (
  SELECT
    aa.identity_id,
    MAX(aa.last_observed_at) AS last_seen_at,
    MIN(aa.created_at) AS first_seen_at
  FROM all_active_accounts aa
  GROUP BY aa.identity_id
),
status_stats AS (
  SELECT
    aa.identity_id,
    COUNT(*)::bigint AS account_count,
    BOOL_OR(aa.normalized_status IN ('active', 'enabled')) AS has_active,
    BOOL_OR(aa.normalized_status IN ('suspended', 'disabled', 'inactive', 'locked')) AS has_suspended,
    BOOL_AND(aa.normalized_status IN ('deleted', 'deprovisioned', 'terminated')) AS all_deleted
  FROM all_active_accounts aa
  GROUP BY aa.identity_id
),
link_stats AS (
  SELECT
    aa.identity_id,
    MIN(aa.confidence)::real AS min_link_confidence,
    COUNT(DISTINCT lower(aa.link_reason)) FILTER (WHERE aa.link_reason <> '')::bigint AS reason_kinds,
    MIN(lower(aa.link_reason)) FILTER (WHERE aa.link_reason <> '') AS single_reason
  FROM all_active_accounts aa
  GROUP BY aa.identity_id
),
primary_source AS (
  SELECT DISTINCT ON (sa.identity_id)
    sa.identity_id,
    sa.source_kind,
    sa.source_name
  FROM source_accounts sa
  LEFT JOIN identity_source_settings iss
    ON iss.source_kind = sa.source_kind
   AND iss.source_name = sa.source_name
   AND iss.is_authoritative
  ORDER BY sa.identity_id, (iss.is_authoritative IS NOT TRUE), sa.account_id
),
base_metrics AS (
  SELECT
    i.id,
    i.display_name,
    i.primary_email,
    i.kind AS identity_type,
    (mi.identity_id IS NOT NULL)::boolean AS managed,
    COALESCE(ps.source_kind, '') AS source_kind,
    COALESCE(ps.source_name, '') AS source_name,
    COALESCE(ic.integration_count, 0)::bigint AS integration_count,
    COALESCE(pc.privileged_roles, 0)::bigint AS privileged_roles,
    ast.last_seen_at::timestamptz AS last_seen_at,
    COALESCE(ast.first_seen_at, i.created_at)::timestamptz AS first_seen_at,
    CASE
      WHEN COALESCE(ss.account_count, 0) = 0 THEN 'orphaned'
      WHEN COALESCE(ss.has_active, FALSE) THEN 'active'
      WHEN COALESCE(ss.all_deleted, FALSE) THEN 'deleted'
      WHEN COALESCE(ss.has_suspended, FALSE) THEN 'suspended'
      ELSE 'unknown'
    END AS status,
    CASE
      WHEN ast.last_seen_at IS NULL THEN 'never_seen'
      WHEN ast.last_seen_at >= now() - interval '30 days' THEN 'recent'
      WHEN ast.last_seen_at >= now() - interval '90 days' THEN 'aging'
      ELSE 'stale'
    END AS activity_state,
    CASE
      WHEN ls.min_link_confidence IS NULL THEN 'unknown'
      WHEN ls.min_link_confidence >= 0.95 THEN 'high'
      WHEN ls.min_link_confidence >= 0.80 THEN 'medium'
      ELSE 'low'
    END AS link_quality,
    CASE
      WHEN COALESCE(ls.reason_kinds, 0) = 0 THEN '—'
      WHEN ls.reason_kinds = 1 THEN COALESCE(ls.single_reason, '—')
      ELSE 'mixed'
    END AS link_reason,
    COALESCE(ls.min_link_confidence, 0)::real AS min_link_confidence
  FROM candidate_identities ci
  JOIN identities i ON i.id = ci.id
  LEFT JOIN managed_identities mi ON mi.identity_id = ci.id
  LEFT JOIN primary_source ps ON ps.identity_id = ci.id
  LEFT JOIN integration_counts ic ON ic.identity_id = ci.id
  LEFT JOIN privileged_counts pc ON pc.identity_id = ci.id
  LEFT JOIN activity_stats ast ON ast.identity_id = ci.id
  LEFT JOIN status_stats ss ON ss.identity_id = ci.id
  LEFT JOIN link_stats ls ON ls.identity_id = ci.id
),
base AS (
  SELECT
    bm.*,
    CASE
      WHEN (NOT bm.managed) AND bm.privileged_roles > 0 THEN 'action_required'
      WHEN bm.privileged_roles > 0 AND bm.activity_state IN ('stale', 'never_seen') THEN 'action_required'
      WHEN NOT bm.managed THEN 'review'
      WHEN bm.activity_state IN ('aging', 'stale', 'never_seen') THEN 'review'
      WHEN bm.link_quality = 'low' THEN 'review'
      ELSE 'healthy'
    END AS row_state
  FROM base_metrics bm
)
SELECT COUNT(*)
FROM base b
WHERE
  (
    sqlc.arg(managed_state)::text = ''
    OR (
      sqlc.arg(managed_state)::text = 'managed'
      AND b.managed
    )
    OR (
      sqlc.arg(managed_state)::text = 'unmanaged'
      AND NOT b.managed
    )
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
    sqlc.arg(link_quality)::text = ''
    OR b.link_quality = sqlc.arg(link_quality)::text
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
    lower(trim(COALESCE(NULLIF(a.status, ''), NULLIF(a.raw_json->>'status', ''), 'unknown'))) AS normalized_status,
    ia.confidence,
    trim(ia.link_reason) AS link_reason
  FROM identity_accounts ia
  JOIN accounts a ON a.id = ia.account_id
  JOIN configured_sources cs
    ON cs.source_kind = a.source_kind
   AND cs.source_name = a.source_name
  WHERE a.expired_at IS NULL
    AND a.last_observed_run_id IS NOT NULL
),
filtered_identities AS (
  SELECT i.id
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
    )
),
source_accounts AS (
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
candidate_identities AS (
  SELECT fi.id
  FROM filtered_identities fi
  WHERE (
      sqlc.arg(source_kind)::text = ''
      AND sqlc.arg(source_name)::text = ''
    )
    OR EXISTS (
      SELECT 1
      FROM source_accounts sa
      WHERE sa.identity_id = fi.id
    )
),
managed_identities AS (
  SELECT DISTINCT aa.identity_id
  FROM all_active_accounts aa
  JOIN identity_source_settings iss
    ON iss.source_kind = aa.source_kind
   AND iss.source_name = aa.source_name
   AND iss.is_authoritative
),
integration_counts AS (
  SELECT
    aa.identity_id,
    COUNT(DISTINCT (aa.source_kind, aa.source_name))::bigint AS integration_count
  FROM all_active_accounts aa
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
activity_stats AS (
  SELECT
    aa.identity_id,
    MAX(aa.last_observed_at) AS last_seen_at,
    MIN(aa.created_at) AS first_seen_at
  FROM all_active_accounts aa
  GROUP BY aa.identity_id
),
status_stats AS (
  SELECT
    aa.identity_id,
    COUNT(*)::bigint AS account_count,
    BOOL_OR(aa.normalized_status IN ('active', 'enabled')) AS has_active,
    BOOL_OR(aa.normalized_status IN ('suspended', 'disabled', 'inactive', 'locked')) AS has_suspended,
    BOOL_AND(aa.normalized_status IN ('deleted', 'deprovisioned', 'terminated')) AS all_deleted
  FROM all_active_accounts aa
  GROUP BY aa.identity_id
),
link_stats AS (
  SELECT
    aa.identity_id,
    MIN(aa.confidence)::real AS min_link_confidence,
    COUNT(DISTINCT lower(aa.link_reason)) FILTER (WHERE aa.link_reason <> '')::bigint AS reason_kinds,
    MIN(lower(aa.link_reason)) FILTER (WHERE aa.link_reason <> '') AS single_reason
  FROM all_active_accounts aa
  GROUP BY aa.identity_id
),
primary_source AS (
  SELECT DISTINCT ON (sa.identity_id)
    sa.identity_id,
    sa.source_kind,
    sa.source_name
  FROM source_accounts sa
  LEFT JOIN identity_source_settings iss
    ON iss.source_kind = sa.source_kind
   AND iss.source_name = sa.source_name
   AND iss.is_authoritative
  ORDER BY sa.identity_id, (iss.is_authoritative IS NOT TRUE), sa.account_id
),
base_metrics AS (
  SELECT
    i.id,
    i.display_name,
    i.primary_email,
    i.kind AS identity_type,
    (mi.identity_id IS NOT NULL)::boolean AS managed,
    COALESCE(ps.source_kind, '') AS source_kind,
    COALESCE(ps.source_name, '') AS source_name,
    COALESCE(ic.integration_count, 0)::bigint AS integration_count,
    COALESCE(pc.privileged_roles, 0)::bigint AS privileged_roles,
    ast.last_seen_at::timestamptz AS last_seen_at,
    COALESCE(ast.first_seen_at, i.created_at)::timestamptz AS first_seen_at,
    CASE
      WHEN COALESCE(ss.account_count, 0) = 0 THEN 'orphaned'
      WHEN COALESCE(ss.has_active, FALSE) THEN 'active'
      WHEN COALESCE(ss.all_deleted, FALSE) THEN 'deleted'
      WHEN COALESCE(ss.has_suspended, FALSE) THEN 'suspended'
      ELSE 'unknown'
    END AS status,
    CASE
      WHEN ast.last_seen_at IS NULL THEN 'never_seen'
      WHEN ast.last_seen_at >= now() - interval '30 days' THEN 'recent'
      WHEN ast.last_seen_at >= now() - interval '90 days' THEN 'aging'
      ELSE 'stale'
    END AS activity_state,
    CASE
      WHEN ls.min_link_confidence IS NULL THEN 'unknown'
      WHEN ls.min_link_confidence >= 0.95 THEN 'high'
      WHEN ls.min_link_confidence >= 0.80 THEN 'medium'
      ELSE 'low'
    END AS link_quality,
    CASE
      WHEN COALESCE(ls.reason_kinds, 0) = 0 THEN '—'
      WHEN ls.reason_kinds = 1 THEN COALESCE(ls.single_reason, '—')
      ELSE 'mixed'
    END AS link_reason,
    COALESCE(ls.min_link_confidence, 0)::real AS min_link_confidence
  FROM candidate_identities ci
  JOIN identities i ON i.id = ci.id
  LEFT JOIN managed_identities mi ON mi.identity_id = ci.id
  LEFT JOIN primary_source ps ON ps.identity_id = ci.id
  LEFT JOIN integration_counts ic ON ic.identity_id = ci.id
  LEFT JOIN privileged_counts pc ON pc.identity_id = ci.id
  LEFT JOIN activity_stats ast ON ast.identity_id = ci.id
  LEFT JOIN status_stats ss ON ss.identity_id = ci.id
  LEFT JOIN link_stats ls ON ls.identity_id = ci.id
),
base AS (
  SELECT
    bm.*,
    CASE
      WHEN (NOT bm.managed) AND bm.privileged_roles > 0 THEN 'action_required'
      WHEN bm.privileged_roles > 0 AND bm.activity_state IN ('stale', 'never_seen') THEN 'action_required'
      WHEN NOT bm.managed THEN 'review'
      WHEN bm.activity_state IN ('aging', 'stale', 'never_seen') THEN 'review'
      WHEN bm.link_quality = 'low' THEN 'review'
      ELSE 'healthy'
    END AS row_state
  FROM base_metrics bm
)
SELECT
  b.id,
  b.display_name,
  b.primary_email,
  b.identity_type,
  b.managed,
  b.source_kind,
  b.source_name,
  b.integration_count,
  b.privileged_roles,
  b.last_seen_at,
  b.first_seen_at,
  b.status,
  b.activity_state,
  b.link_quality,
  b.link_reason,
  b.min_link_confidence,
  b.row_state
FROM base b
WHERE
  (
    sqlc.arg(managed_state)::text = ''
    OR (
      sqlc.arg(managed_state)::text = 'managed'
      AND b.managed
    )
    OR (
      sqlc.arg(managed_state)::text = 'unmanaged'
      AND NOT b.managed
    )
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
    sqlc.arg(link_quality)::text = ''
    OR b.link_quality = sqlc.arg(link_quality)::text
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
    WHEN sqlc.arg(sort_by)::text = 'managed'
      AND sqlc.arg(sort_dir)::text = 'asc'
    THEN CASE WHEN b.managed THEN 1 ELSE 0 END
  END ASC,
  CASE
    WHEN sqlc.arg(sort_by)::text = 'managed'
      AND sqlc.arg(sort_dir)::text = 'desc'
    THEN CASE WHEN b.managed THEN 1 ELSE 0 END
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

-- name: GetIdentitySummaryByID :one
WITH authoritative_identities AS (
  SELECT DISTINCT ia.identity_id
  FROM identity_accounts ia
  JOIN accounts anchor ON anchor.id = ia.account_id
  JOIN identity_source_settings iss
    ON iss.source_kind = anchor.source_kind
   AND iss.source_name = anchor.source_name
   AND iss.is_authoritative
  WHERE anchor.expired_at IS NULL
    AND anchor.last_observed_run_id IS NOT NULL
)
SELECT
  i.*,
  (ai.identity_id IS NOT NULL)::boolean AS managed,
  COUNT(ia.account_id) AS linked_accounts
FROM identities i
LEFT JOIN identity_accounts ia ON ia.identity_id = i.id
LEFT JOIN authoritative_identities ai ON ai.identity_id = i.id
WHERE i.id = $1
GROUP BY i.id, ai.identity_id;

-- name: DeleteIdentityIfUnlinked :execrows
DELETE FROM identities i
WHERE i.id = $1
  AND NOT EXISTS (
    SELECT 1
    FROM identity_accounts ia
    WHERE ia.identity_id = i.id
  );

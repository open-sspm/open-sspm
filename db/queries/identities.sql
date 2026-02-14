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

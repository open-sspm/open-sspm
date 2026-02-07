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

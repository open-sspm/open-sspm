-- name: ListAuthoritativeSourcesByConfiguredSources :many
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
)
SELECT iss.*
FROM identity_source_settings iss
JOIN configured_sources cs
  ON cs.source_kind = iss.source_kind
 AND cs.source_name = iss.source_name
WHERE iss.is_authoritative = TRUE
ORDER BY iss.source_kind, iss.source_name;

-- name: ListIdentitySourceSettings :many
SELECT *
FROM identity_source_settings
ORDER BY source_kind, source_name;

-- name: UpsertIdentitySourceSetting :one
INSERT INTO identity_source_settings (source_kind, source_name, is_authoritative, updated_at)
VALUES (
  sqlc.arg(source_kind)::text,
  sqlc.arg(source_name)::text,
  sqlc.arg(is_authoritative)::boolean,
  now()
)
ON CONFLICT (source_kind, source_name) DO UPDATE SET
  is_authoritative = EXCLUDED.is_authoritative,
  updated_at = EXCLUDED.updated_at
RETURNING *;

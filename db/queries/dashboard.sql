-- name: ListDashboardSourceAccountSummaries :many
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name,
    k.ord AS source_ord
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
),
authoritative_identities AS MATERIALIZED (
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
),
active_accounts AS MATERIALIZED (
  SELECT
    a.id,
    a.source_kind,
    a.source_name,
    ia.identity_id
  FROM accounts a
  JOIN configured_sources cs
    ON cs.source_kind = a.source_kind
   AND cs.source_name = a.source_name
  LEFT JOIN identity_accounts ia
    ON ia.account_id = a.id
  WHERE a.expired_at IS NULL
    AND a.last_observed_run_id IS NOT NULL
)
SELECT
  cs.source_kind::text AS source_kind,
  cs.source_name::text AS source_name,
  COUNT(DISTINCT aa.identity_id) FILTER (WHERE aa.identity_id IS NOT NULL)::bigint AS identity_count,
  COUNT(aa.id)::bigint AS account_count,
  COUNT(aa.id) FILTER (
    WHERE aa.identity_id IS NOT NULL
      AND ai.identity_id IS NOT NULL
  )::bigint AS managed_account_count,
  COUNT(aa.id) FILTER (
    WHERE aa.identity_id IS NULL
       OR ai.identity_id IS NULL
  )::bigint AS unmanaged_account_count
FROM configured_sources cs
LEFT JOIN active_accounts aa
  ON aa.source_kind = cs.source_kind
 AND aa.source_name = cs.source_name
LEFT JOIN authoritative_identities ai
  ON ai.identity_id = aa.identity_id
GROUP BY cs.source_kind, cs.source_name, cs.source_ord
ORDER BY cs.source_ord;

-- name: ListDashboardPrivilegedAccessBuckets :many
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
),
active_entitlements AS (
  SELECT
    a.source_kind,
    a.source_name,
    a.id AS account_id,
    ia.identity_id,
    e.id AS entitlement_id,
    e.kind,
    e.resource,
    e.permission,
    e.raw_json,
    lower(trim(e.permission)) AS permission_key,
    lower(trim(COALESCE(
      NULLIF(e.raw_json->>'role_name', ''),
      NULLIF(e.raw_json->>'role_display_name', ''),
      NULLIF(e.raw_json->>'app_role_display_name', ''),
      NULLIF(e.raw_json->>'app_role_value', ''),
      NULLIF(split_part(e.resource, ':', 2), '')
    ))) AS role_key,
    COALESCE(
      NULLIF(trim(e.raw_json->>'role_name'), ''),
      NULLIF(trim(e.raw_json->>'role_display_name'), ''),
      NULLIF(trim(e.raw_json->>'app_role_display_name'), ''),
      NULLIF(trim(e.raw_json->>'app_role_value'), ''),
      NULLIF(trim(e.permission), ''),
      NULLIF(trim(split_part(e.resource, ':', 2)), '')
    ) AS raw_label
  FROM entitlements e
  JOIN accounts a ON a.id = e.app_user_id
  JOIN configured_sources cs
    ON cs.source_kind = a.source_kind
   AND cs.source_name = a.source_name
  LEFT JOIN identity_accounts ia ON ia.account_id = a.id
  WHERE a.expired_at IS NULL
    AND a.last_observed_run_id IS NOT NULL
    AND e.expired_at IS NULL
    AND e.last_observed_run_id IS NOT NULL
),
classified AS (
  SELECT
    source_kind,
    source_name,
    account_id,
    identity_id,
    entitlement_id,
    CASE
      WHEN kind = 'github_team_repo_permission' AND permission_key = 'admin' THEN 'Admin repositories'
      WHEN kind = 'github_team_repo_permission' AND permission_key = 'maintain' THEN 'Maintain repositories'
      WHEN kind = 'datadog_role' THEN COALESCE(NULLIF(raw_label, ''), 'Privileged Datadog role')
      WHEN kind = 'aws_permission_set' THEN COALESCE(NULLIF(raw_label, ''), 'Privileged permission set')
      WHEN kind = 'google_admin_role' THEN COALESCE(NULLIF(raw_label, ''), 'Google admin role')
      WHEN kind = 'google_group_member' AND permission_key = 'owner' THEN 'Group owners'
      WHEN kind = 'google_group_member' AND permission_key = 'manager' THEN 'Group managers'
      WHEN kind = 'entra_directory_role' THEN COALESCE(NULLIF(raw_label, ''), 'Directory role')
      WHEN kind = 'entra_app_role' THEN COALESCE(NULLIF(raw_label, ''), 'Application role')
      WHEN kind IN ('vault_entity_policy', 'vault_group_policy', 'vault_auth_role_policy') THEN COALESCE(NULLIF(split_part(resource, ':', 2), ''), 'Vault policy')
      ELSE ''
    END::text AS bucket_label,
    CASE
      WHEN permission_key IN ('admin', 'owner', 'root', 'super_admin')
        OR role_key LIKE '%super admin%'
        OR role_key LIKE '%administrator%'
        OR role_key LIKE '%admin%'
        OR role_key LIKE '%owner%'
        OR role_key LIKE '%root%' THEN 'critical'
      WHEN permission_key IN ('maintain', 'manager')
        OR role_key LIKE '%poweruser%'
        OR role_key LIKE '%power user%'
        OR role_key LIKE '%sudo%' THEN 'high'
      ELSE 'medium'
    END::text AS severity
  FROM active_entitlements
  WHERE
    (
      kind = 'github_team_repo_permission'
      AND permission_key IN ('admin', 'maintain')
    )
    OR (
      kind = 'datadog_role'
      AND (
        role_key LIKE '%admin%'
        OR role_key LIKE '%administrator%'
        OR role_key LIKE '%owner%'
      )
    )
    OR (
      kind = 'aws_permission_set'
      AND (
        permission_key LIKE '%admin%'
        OR permission_key LIKE '%administrator%'
        OR permission_key LIKE '%poweruser%'
        OR permission_key LIKE '%power user%'
        OR permission_key LIKE '%owner%'
        OR permission_key LIKE '%root%'
      )
    )
    OR kind = 'google_admin_role'
    OR (
      kind = 'google_group_member'
      AND permission_key IN ('owner', 'manager')
    )
    OR (
      kind IN ('entra_directory_role', 'entra_app_role')
      AND (
        role_key LIKE '%admin%'
        OR role_key LIKE '%administrator%'
        OR role_key LIKE '%owner%'
        OR role_key LIKE '%privileged%'
      )
    )
    OR (
      kind IN ('vault_entity_policy', 'vault_group_policy', 'vault_auth_role_policy')
      AND (
        lower(trim(split_part(resource, ':', 2))) LIKE '%admin%'
        OR lower(trim(split_part(resource, ':', 2))) LIKE '%root%'
        OR lower(trim(split_part(resource, ':', 2))) LIKE '%sudo%'
      )
    )
),
aggregated AS (
  SELECT
    source_kind,
    source_name,
    lower(trim(bucket_label)) AS bucket_key,
    bucket_label,
    severity,
    COUNT(DISTINCT COALESCE('identity:' || identity_id::text, 'account:' || account_id::text))::bigint AS affected_count,
    COUNT(DISTINCT entitlement_id)::bigint AS entitlement_count
  FROM classified
  WHERE trim(bucket_label) <> ''
  GROUP BY source_kind, source_name, lower(trim(bucket_label)), bucket_label, severity
),
ranked AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY source_kind, source_name
      ORDER BY affected_count DESC, entitlement_count DESC, bucket_label ASC
    ) AS bucket_rank
  FROM aggregated
)
SELECT
  source_kind,
  source_name,
  bucket_key,
  bucket_label,
  severity,
  affected_count,
  entitlement_count
FROM ranked
WHERE bucket_rank <= sqlc.arg(bucket_limit)::int
ORDER BY source_kind, source_name, bucket_rank, bucket_label;

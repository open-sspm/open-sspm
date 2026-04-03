-- name: UpsertSaaSAppsBulk :execrows
WITH input AS (
  SELECT
    i,
    (sqlc.arg(canonical_keys)::text[])[i] AS canonical_key,
    (sqlc.arg(display_names)::text[])[i] AS display_name,
    (sqlc.arg(primary_domains)::text[])[i] AS primary_domain,
    (sqlc.arg(vendor_names)::text[])[i] AS vendor_name,
    (sqlc.arg(first_seen_ats)::timestamptz[])[i] AS first_seen_at,
    (sqlc.arg(last_seen_ats)::timestamptz[])[i] AS last_seen_at
  FROM generate_subscripts(sqlc.arg(canonical_keys)::text[], 1) AS s(i)
),
dedup AS (
  SELECT DISTINCT ON (canonical_key)
    canonical_key,
    display_name,
    primary_domain,
    vendor_name,
    first_seen_at,
    last_seen_at
  FROM input
  ORDER BY canonical_key, i DESC
)
INSERT INTO saas_apps (
  canonical_key,
  display_name,
  primary_domain,
  vendor_name,
  first_seen_at,
  last_seen_at,
  updated_at
)
SELECT
  d.canonical_key,
  d.display_name,
  d.primary_domain,
  d.vendor_name,
  COALESCE(d.first_seen_at, now()),
  COALESCE(d.last_seen_at, now()),
  now()
FROM dedup d
ON CONFLICT (canonical_key) DO UPDATE SET
  display_name = CASE
    WHEN trim(EXCLUDED.display_name) <> '' THEN EXCLUDED.display_name
    ELSE saas_apps.display_name
  END,
  primary_domain = CASE
    WHEN trim(EXCLUDED.primary_domain) <> '' THEN EXCLUDED.primary_domain
    ELSE saas_apps.primary_domain
  END,
  vendor_name = CASE
    WHEN trim(EXCLUDED.vendor_name) <> '' THEN EXCLUDED.vendor_name
    ELSE saas_apps.vendor_name
  END,
  first_seen_at = LEAST(saas_apps.first_seen_at, COALESCE(EXCLUDED.first_seen_at, saas_apps.first_seen_at)),
  last_seen_at = GREATEST(saas_apps.last_seen_at, COALESCE(EXCLUDED.last_seen_at, saas_apps.last_seen_at)),
  updated_at = now();

-- name: CountSaaSAppsByFilters :one
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
),
scoped_app_ids AS (
  SELECT DISTINCT sas.saas_app_id
  FROM saas_app_sources sas
  JOIN configured_sources cfg
    ON lower(trim(cfg.source_kind)) = lower(trim(sas.source_kind))
   AND lower(trim(cfg.source_name)) = lower(trim(sas.source_name))
  WHERE sas.expired_at IS NULL
    AND sas.last_observed_run_id IS NOT NULL
    AND (
      sqlc.arg(source_kind)::text = ''
      OR lower(trim(sas.source_kind)) = lower(trim(sqlc.arg(source_kind)::text))
    )
    AND (
      sqlc.arg(source_name)::text = ''
      OR lower(trim(sas.source_name)) = lower(trim(sqlc.arg(source_name)::text))
    )
),
posture_inputs AS (
  SELECT
    spi.*,
    CASE spi.bound_connector_kind
      WHEN 'okta' THEN sqlc.arg(okta_fresh_after)::timestamptz
      WHEN 'entra' THEN sqlc.arg(entra_fresh_after)::timestamptz
      WHEN 'google_workspace' THEN sqlc.arg(google_workspace_fresh_after)::timestamptz
      WHEN 'github' THEN sqlc.arg(github_fresh_after)::timestamptz
      WHEN 'datadog' THEN sqlc.arg(datadog_fresh_after)::timestamptz
      WHEN 'aws_identity_center' THEN sqlc.arg(aws_fresh_after)::timestamptz
      ELSE sqlc.arg(default_fresh_after)::timestamptz
    END AS fresh_after
  FROM saas_app_posture_inputs_v spi
  JOIN scoped_app_ids sai ON sai.saas_app_id = spi.id
  WHERE (
    sqlc.arg(query)::text = ''
    OR spi.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR spi.primary_domain ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR spi.vendor_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR spi.canonical_key ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
),
posture_state AS (
  SELECT
    pi.*,
    CASE
      WHEN pi.bound_connector_kind = '' OR pi.bound_connector_source_name = '' THEN 'unmanaged'
      WHEN NOT pi.connector_configured THEN 'unmanaged'
      WHEN NOT pi.connector_enabled THEN 'unmanaged'
      WHEN pi.last_success_at IS NULL THEN 'unmanaged'
      WHEN pi.last_success_at < pi.fresh_after THEN 'unmanaged'
      ELSE 'managed'
    END AS managed_state,
    CASE
      WHEN pi.bound_connector_kind = '' OR pi.bound_connector_source_name = '' THEN 'no_binding'
      WHEN NOT pi.connector_configured THEN 'connector_not_configured'
      WHEN NOT pi.connector_enabled THEN 'connector_disabled'
      WHEN pi.last_success_at IS NULL THEN 'stale_sync'
      WHEN pi.last_success_at < pi.fresh_after THEN 'stale_sync'
      ELSE 'active_binding_fresh_sync'
    END AS managed_reason
  FROM posture_inputs pi
),
posture_with_risk AS (
  SELECT
    ps.*,
    LEAST(100,
      CASE WHEN ps.managed_state <> 'managed' THEN 45 ELSE 0 END
      + CASE WHEN ps.has_privileged_scope THEN 20 ELSE 0 END
      + CASE WHEN ps.owner_identity_id = 0 THEN 15 ELSE 0 END
      + CASE WHEN ps.actors_30d >= 50 THEN 10 ELSE 0 END
      + CASE WHEN ps.managed_state <> 'managed' AND ps.effective_business_criticality IN ('high', 'critical') THEN 10 ELSE 0 END
      + CASE WHEN ps.managed_state <> 'managed' AND ps.effective_data_classification IN ('confidential', 'restricted') THEN 5 ELSE 0 END
    )::int AS risk_score
  FROM posture_state ps
),
posture_rows AS (
  SELECT
    pwr.*,
    CASE
      WHEN pwr.risk_score >= 80 THEN 'critical'
      WHEN pwr.risk_score >= 60 THEN 'high'
      WHEN pwr.risk_score >= 30 THEN 'medium'
      ELSE 'low'
    END AS risk_level
  FROM posture_with_risk pwr
)
SELECT count(*)
FROM posture_rows pr
WHERE (
    sqlc.arg(managed_state)::text = ''
    OR pr.managed_state = sqlc.arg(managed_state)::text
  )
  AND (
    sqlc.arg(risk_level)::text = ''
    OR pr.risk_level = sqlc.arg(risk_level)::text
  );

-- name: ListSaaSAppsPageByFilters :many
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
),
scoped_app_ids AS (
  SELECT DISTINCT sas.saas_app_id
  FROM saas_app_sources sas
  JOIN configured_sources cfg
    ON lower(trim(cfg.source_kind)) = lower(trim(sas.source_kind))
   AND lower(trim(cfg.source_name)) = lower(trim(sas.source_name))
  WHERE sas.expired_at IS NULL
    AND sas.last_observed_run_id IS NOT NULL
    AND (
      sqlc.arg(source_kind)::text = ''
      OR lower(trim(sas.source_kind)) = lower(trim(sqlc.arg(source_kind)::text))
    )
    AND (
      sqlc.arg(source_name)::text = ''
      OR lower(trim(sas.source_name)) = lower(trim(sqlc.arg(source_name)::text))
    )
),
posture_inputs AS (
  SELECT
    spi.*,
    CASE spi.bound_connector_kind
      WHEN 'okta' THEN sqlc.arg(okta_fresh_after)::timestamptz
      WHEN 'entra' THEN sqlc.arg(entra_fresh_after)::timestamptz
      WHEN 'google_workspace' THEN sqlc.arg(google_workspace_fresh_after)::timestamptz
      WHEN 'github' THEN sqlc.arg(github_fresh_after)::timestamptz
      WHEN 'datadog' THEN sqlc.arg(datadog_fresh_after)::timestamptz
      WHEN 'aws_identity_center' THEN sqlc.arg(aws_fresh_after)::timestamptz
      ELSE sqlc.arg(default_fresh_after)::timestamptz
    END AS fresh_after
  FROM saas_app_posture_inputs_v spi
  JOIN scoped_app_ids sai ON sai.saas_app_id = spi.id
  WHERE (
    sqlc.arg(query)::text = ''
    OR spi.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR spi.primary_domain ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR spi.vendor_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR spi.canonical_key ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
),
posture_state AS (
  SELECT
    pi.*,
    CASE
      WHEN pi.bound_connector_kind = '' OR pi.bound_connector_source_name = '' THEN 'unmanaged'
      WHEN NOT pi.connector_configured THEN 'unmanaged'
      WHEN NOT pi.connector_enabled THEN 'unmanaged'
      WHEN pi.last_success_at IS NULL THEN 'unmanaged'
      WHEN pi.last_success_at < pi.fresh_after THEN 'unmanaged'
      ELSE 'managed'
    END AS managed_state,
    CASE
      WHEN pi.bound_connector_kind = '' OR pi.bound_connector_source_name = '' THEN 'no_binding'
      WHEN NOT pi.connector_configured THEN 'connector_not_configured'
      WHEN NOT pi.connector_enabled THEN 'connector_disabled'
      WHEN pi.last_success_at IS NULL THEN 'stale_sync'
      WHEN pi.last_success_at < pi.fresh_after THEN 'stale_sync'
      ELSE 'active_binding_fresh_sync'
    END AS managed_reason
  FROM posture_inputs pi
),
posture_with_risk AS (
  SELECT
    ps.*,
    LEAST(100,
      CASE WHEN ps.managed_state <> 'managed' THEN 45 ELSE 0 END
      + CASE WHEN ps.has_privileged_scope THEN 20 ELSE 0 END
      + CASE WHEN ps.owner_identity_id = 0 THEN 15 ELSE 0 END
      + CASE WHEN ps.actors_30d >= 50 THEN 10 ELSE 0 END
      + CASE WHEN ps.managed_state <> 'managed' AND ps.effective_business_criticality IN ('high', 'critical') THEN 10 ELSE 0 END
      + CASE WHEN ps.managed_state <> 'managed' AND ps.effective_data_classification IN ('confidential', 'restricted') THEN 5 ELSE 0 END
    )::int AS risk_score
  FROM posture_state ps
),
posture_rows AS (
  SELECT
    pwr.*,
    CASE
      WHEN pwr.risk_score >= 80 THEN 'critical'
      WHEN pwr.risk_score >= 60 THEN 'high'
      WHEN pwr.risk_score >= 30 THEN 'medium'
      ELSE 'low'
    END AS risk_level
  FROM posture_with_risk pwr
)
SELECT
  pr.id,
  pr.canonical_key,
  pr.display_name,
  pr.primary_domain,
  pr.vendor_name,
  pr.managed_state,
  pr.managed_reason,
  pr.bound_connector_kind,
  pr.bound_connector_source_name,
  pr.risk_score,
  pr.risk_level,
  pr.suggested_business_criticality,
  pr.suggested_data_classification,
  pr.first_seen_at,
  pr.last_seen_at,
  pr.created_at,
  pr.updated_at,
  COALESCE(owner.display_name, '') AS owner_display_name,
  COALESCE(owner.primary_email, '') AS owner_primary_email,
  pr.actors_30d
FROM posture_rows pr
LEFT JOIN identities owner ON owner.id = NULLIF(pr.owner_identity_id, 0)
WHERE (
    sqlc.arg(managed_state)::text = ''
    OR pr.managed_state = sqlc.arg(managed_state)::text
  )
  AND (
    sqlc.arg(risk_level)::text = ''
    OR pr.risk_level = sqlc.arg(risk_level)::text
  )
ORDER BY
  pr.risk_score DESC,
  pr.last_seen_at DESC,
  lower(COALESCE(NULLIF(trim(pr.display_name), ''), pr.canonical_key)) ASC,
  pr.id ASC
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: GetSaaSAppByID :one
WITH posture_inputs AS (
  SELECT
    spi.*,
    CASE spi.bound_connector_kind
      WHEN 'okta' THEN sqlc.arg(okta_fresh_after)::timestamptz
      WHEN 'entra' THEN sqlc.arg(entra_fresh_after)::timestamptz
      WHEN 'google_workspace' THEN sqlc.arg(google_workspace_fresh_after)::timestamptz
      WHEN 'github' THEN sqlc.arg(github_fresh_after)::timestamptz
      WHEN 'datadog' THEN sqlc.arg(datadog_fresh_after)::timestamptz
      WHEN 'aws_identity_center' THEN sqlc.arg(aws_fresh_after)::timestamptz
      ELSE sqlc.arg(default_fresh_after)::timestamptz
    END AS fresh_after
  FROM saas_app_posture_inputs_v spi
  WHERE spi.id = sqlc.arg(id)::bigint
),
posture_state AS (
  SELECT
    pi.*,
    CASE
      WHEN pi.bound_connector_kind = '' OR pi.bound_connector_source_name = '' THEN 'unmanaged'
      WHEN NOT pi.connector_configured THEN 'unmanaged'
      WHEN NOT pi.connector_enabled THEN 'unmanaged'
      WHEN pi.last_success_at IS NULL THEN 'unmanaged'
      WHEN pi.last_success_at < pi.fresh_after THEN 'unmanaged'
      ELSE 'managed'
    END AS managed_state,
    CASE
      WHEN pi.bound_connector_kind = '' OR pi.bound_connector_source_name = '' THEN 'no_binding'
      WHEN NOT pi.connector_configured THEN 'connector_not_configured'
      WHEN NOT pi.connector_enabled THEN 'connector_disabled'
      WHEN pi.last_success_at IS NULL THEN 'stale_sync'
      WHEN pi.last_success_at < pi.fresh_after THEN 'stale_sync'
      ELSE 'active_binding_fresh_sync'
    END AS managed_reason
  FROM posture_inputs pi
),
posture_with_risk AS (
  SELECT
    ps.*,
    LEAST(100,
      CASE WHEN ps.managed_state <> 'managed' THEN 45 ELSE 0 END
      + CASE WHEN ps.has_privileged_scope THEN 20 ELSE 0 END
      + CASE WHEN ps.owner_identity_id = 0 THEN 15 ELSE 0 END
      + CASE WHEN ps.actors_30d >= 50 THEN 10 ELSE 0 END
      + CASE WHEN ps.managed_state <> 'managed' AND ps.effective_business_criticality IN ('high', 'critical') THEN 10 ELSE 0 END
      + CASE WHEN ps.managed_state <> 'managed' AND ps.effective_data_classification IN ('confidential', 'restricted') THEN 5 ELSE 0 END
    )::int AS risk_score
  FROM posture_state ps
),
posture_rows AS (
  SELECT
    pwr.*,
    CASE
      WHEN pwr.risk_score >= 80 THEN 'critical'
      WHEN pwr.risk_score >= 60 THEN 'high'
      WHEN pwr.risk_score >= 30 THEN 'medium'
      ELSE 'low'
    END AS risk_level
  FROM posture_with_risk pwr
)
SELECT
  id,
  canonical_key,
  display_name,
  primary_domain,
  vendor_name,
  managed_state,
  managed_reason,
  bound_connector_kind,
  bound_connector_source_name,
  risk_score,
  risk_level,
  suggested_business_criticality,
  suggested_data_classification,
  first_seen_at,
  last_seen_at,
  created_at,
  updated_at
FROM posture_rows;

-- name: ListSaaSAppHotspots :many
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
),
scoped_app_ids AS (
  SELECT DISTINCT sas.saas_app_id
  FROM saas_app_sources sas
  JOIN configured_sources cfg
    ON lower(trim(cfg.source_kind)) = lower(trim(sas.source_kind))
   AND lower(trim(cfg.source_name)) = lower(trim(sas.source_name))
  WHERE sas.expired_at IS NULL
    AND sas.last_observed_run_id IS NOT NULL
    AND (
      sqlc.arg(source_kind)::text = ''
      OR lower(trim(sas.source_kind)) = lower(trim(sqlc.arg(source_kind)::text))
    )
    AND (
      sqlc.arg(source_name)::text = ''
      OR lower(trim(sas.source_name)) = lower(trim(sqlc.arg(source_name)::text))
    )
),
posture_inputs AS (
  SELECT
    spi.*,
    CASE spi.bound_connector_kind
      WHEN 'okta' THEN sqlc.arg(okta_fresh_after)::timestamptz
      WHEN 'entra' THEN sqlc.arg(entra_fresh_after)::timestamptz
      WHEN 'google_workspace' THEN sqlc.arg(google_workspace_fresh_after)::timestamptz
      WHEN 'github' THEN sqlc.arg(github_fresh_after)::timestamptz
      WHEN 'datadog' THEN sqlc.arg(datadog_fresh_after)::timestamptz
      WHEN 'aws_identity_center' THEN sqlc.arg(aws_fresh_after)::timestamptz
      ELSE sqlc.arg(default_fresh_after)::timestamptz
    END AS fresh_after
  FROM saas_app_posture_inputs_v spi
  JOIN scoped_app_ids sai ON sai.saas_app_id = spi.id
),
posture_state AS (
  SELECT
    pi.*,
    CASE
      WHEN pi.bound_connector_kind = '' OR pi.bound_connector_source_name = '' THEN 'unmanaged'
      WHEN NOT pi.connector_configured THEN 'unmanaged'
      WHEN NOT pi.connector_enabled THEN 'unmanaged'
      WHEN pi.last_success_at IS NULL THEN 'unmanaged'
      WHEN pi.last_success_at < pi.fresh_after THEN 'unmanaged'
      ELSE 'managed'
    END AS managed_state,
    CASE
      WHEN pi.bound_connector_kind = '' OR pi.bound_connector_source_name = '' THEN 'no_binding'
      WHEN NOT pi.connector_configured THEN 'connector_not_configured'
      WHEN NOT pi.connector_enabled THEN 'connector_disabled'
      WHEN pi.last_success_at IS NULL THEN 'stale_sync'
      WHEN pi.last_success_at < pi.fresh_after THEN 'stale_sync'
      ELSE 'active_binding_fresh_sync'
    END AS managed_reason
  FROM posture_inputs pi
),
posture_with_risk AS (
  SELECT
    ps.*,
    LEAST(100,
      CASE WHEN ps.managed_state <> 'managed' THEN 45 ELSE 0 END
      + CASE WHEN ps.has_privileged_scope THEN 20 ELSE 0 END
      + CASE WHEN ps.owner_identity_id = 0 THEN 15 ELSE 0 END
      + CASE WHEN ps.actors_30d >= 50 THEN 10 ELSE 0 END
      + CASE WHEN ps.managed_state <> 'managed' AND ps.effective_business_criticality IN ('high', 'critical') THEN 10 ELSE 0 END
      + CASE WHEN ps.managed_state <> 'managed' AND ps.effective_data_classification IN ('confidential', 'restricted') THEN 5 ELSE 0 END
    )::int AS risk_score
  FROM posture_state ps
),
posture_rows AS (
  SELECT
    pwr.*,
    CASE
      WHEN pwr.risk_score >= 80 THEN 'critical'
      WHEN pwr.risk_score >= 60 THEN 'high'
      WHEN pwr.risk_score >= 30 THEN 'medium'
      ELSE 'low'
    END AS risk_level
  FROM posture_with_risk pwr
)
SELECT
  pr.id,
  pr.canonical_key,
  pr.display_name,
  pr.primary_domain,
  pr.vendor_name,
  pr.managed_state,
  pr.managed_reason,
  pr.bound_connector_kind,
  pr.bound_connector_source_name,
  pr.risk_score,
  pr.risk_level,
  pr.suggested_business_criticality,
  pr.suggested_data_classification,
  pr.first_seen_at,
  pr.last_seen_at,
  pr.created_at,
  pr.updated_at,
  COALESCE(owner.display_name, '') AS owner_display_name,
  COALESCE(owner.primary_email, '') AS owner_primary_email,
  pr.actors_30d
FROM posture_rows pr
LEFT JOIN identities owner ON owner.id = NULLIF(pr.owner_identity_id, 0)
WHERE pr.risk_score >= 60
ORDER BY pr.risk_score DESC, pr.last_seen_at DESC, pr.id ASC
LIMIT sqlc.arg(limit_rows)::int;

-- name: CountSaaSAppsGroupedByManagedState :many
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
),
scoped_app_ids AS (
  SELECT DISTINCT sas.saas_app_id
  FROM saas_app_sources sas
  JOIN configured_sources cfg
    ON lower(trim(cfg.source_kind)) = lower(trim(sas.source_kind))
   AND lower(trim(cfg.source_name)) = lower(trim(sas.source_name))
  WHERE sas.expired_at IS NULL
    AND sas.last_observed_run_id IS NOT NULL
),
posture_inputs AS (
  SELECT
    spi.*,
    CASE spi.bound_connector_kind
      WHEN 'okta' THEN sqlc.arg(okta_fresh_after)::timestamptz
      WHEN 'entra' THEN sqlc.arg(entra_fresh_after)::timestamptz
      WHEN 'google_workspace' THEN sqlc.arg(google_workspace_fresh_after)::timestamptz
      WHEN 'github' THEN sqlc.arg(github_fresh_after)::timestamptz
      WHEN 'datadog' THEN sqlc.arg(datadog_fresh_after)::timestamptz
      WHEN 'aws_identity_center' THEN sqlc.arg(aws_fresh_after)::timestamptz
      ELSE sqlc.arg(default_fresh_after)::timestamptz
    END AS fresh_after
  FROM saas_app_posture_inputs_v spi
  JOIN scoped_app_ids sai ON sai.saas_app_id = spi.id
),
posture_rows AS (
  SELECT
    CASE
      WHEN pi.bound_connector_kind = '' OR pi.bound_connector_source_name = '' THEN 'unmanaged'
      WHEN NOT pi.connector_configured THEN 'unmanaged'
      WHEN NOT pi.connector_enabled THEN 'unmanaged'
      WHEN pi.last_success_at IS NULL THEN 'unmanaged'
      WHEN pi.last_success_at < pi.fresh_after THEN 'unmanaged'
      ELSE 'managed'
    END AS managed_state
  FROM posture_inputs pi
)
SELECT pr.managed_state, count(*) AS app_count
FROM posture_rows pr
GROUP BY pr.managed_state
ORDER BY pr.managed_state;

-- name: CountSaaSAppsGroupedByRiskLevel :many
WITH configured_sources AS (
  SELECT
    k.kind AS source_kind,
    n.name AS source_name
  FROM unnest(sqlc.arg(configured_source_kinds)::text[]) WITH ORDINALITY AS k(kind, ord)
  JOIN unnest(sqlc.arg(configured_source_names)::text[]) WITH ORDINALITY AS n(name, ord) USING (ord)
),
scoped_app_ids AS (
  SELECT DISTINCT sas.saas_app_id
  FROM saas_app_sources sas
  JOIN configured_sources cfg
    ON lower(trim(cfg.source_kind)) = lower(trim(sas.source_kind))
   AND lower(trim(cfg.source_name)) = lower(trim(sas.source_name))
  WHERE sas.expired_at IS NULL
    AND sas.last_observed_run_id IS NOT NULL
),
posture_inputs AS (
  SELECT
    spi.*,
    CASE spi.bound_connector_kind
      WHEN 'okta' THEN sqlc.arg(okta_fresh_after)::timestamptz
      WHEN 'entra' THEN sqlc.arg(entra_fresh_after)::timestamptz
      WHEN 'google_workspace' THEN sqlc.arg(google_workspace_fresh_after)::timestamptz
      WHEN 'github' THEN sqlc.arg(github_fresh_after)::timestamptz
      WHEN 'datadog' THEN sqlc.arg(datadog_fresh_after)::timestamptz
      WHEN 'aws_identity_center' THEN sqlc.arg(aws_fresh_after)::timestamptz
      ELSE sqlc.arg(default_fresh_after)::timestamptz
    END AS fresh_after
  FROM saas_app_posture_inputs_v spi
  JOIN scoped_app_ids sai ON sai.saas_app_id = spi.id
),
posture_state AS (
  SELECT
    pi.*,
    CASE
      WHEN pi.bound_connector_kind = '' OR pi.bound_connector_source_name = '' THEN 'unmanaged'
      WHEN NOT pi.connector_configured THEN 'unmanaged'
      WHEN NOT pi.connector_enabled THEN 'unmanaged'
      WHEN pi.last_success_at IS NULL THEN 'unmanaged'
      WHEN pi.last_success_at < pi.fresh_after THEN 'unmanaged'
      ELSE 'managed'
    END AS managed_state
  FROM posture_inputs pi
),
posture_with_risk AS (
  SELECT
    LEAST(100,
      CASE WHEN ps.managed_state <> 'managed' THEN 45 ELSE 0 END
      + CASE WHEN ps.has_privileged_scope THEN 20 ELSE 0 END
      + CASE WHEN ps.owner_identity_id = 0 THEN 15 ELSE 0 END
      + CASE WHEN ps.actors_30d >= 50 THEN 10 ELSE 0 END
      + CASE WHEN ps.managed_state <> 'managed' AND ps.effective_business_criticality IN ('high', 'critical') THEN 10 ELSE 0 END
      + CASE WHEN ps.managed_state <> 'managed' AND ps.effective_data_classification IN ('confidential', 'restricted') THEN 5 ELSE 0 END
    )::int AS risk_score
  FROM posture_state ps
),
posture_rows AS (
  SELECT
    CASE
      WHEN pwr.risk_score >= 80 THEN 'critical'
      WHEN pwr.risk_score >= 60 THEN 'high'
      WHEN pwr.risk_score >= 30 THEN 'medium'
      ELSE 'low'
    END AS risk_level
  FROM posture_with_risk pwr
)
SELECT pr.risk_level, count(*) AS app_count
FROM posture_rows pr
GROUP BY pr.risk_level
ORDER BY pr.risk_level;

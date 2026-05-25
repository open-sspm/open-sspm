-- name: UpsertOktaGroupsBulk :execrows
WITH input AS (
  SELECT
    i,
    (sqlc.arg(external_ids)::text[])[i] AS external_id,
    (sqlc.arg(names)::text[])[i] AS name,
    (sqlc.arg(types)::text[])[i] AS type,
    (sqlc.arg(raw_jsons)::jsonb[])[i] AS raw_json
  FROM generate_subscripts(sqlc.arg(external_ids)::text[], 1) AS s(i)
),
dedup AS (
  SELECT DISTINCT ON (external_id)
    external_id,
    name,
    type,
    raw_json
  FROM input
  ORDER BY external_id, i DESC
)
INSERT INTO okta_groups (
  external_id,
  name,
  type,
  raw_json,
  seen_in_run_id,
  seen_at,
  updated_at
)
SELECT
  input.external_id,
  input.name,
  input.type,
  input.raw_json,
  sqlc.arg(seen_in_run_id)::bigint,
  now(),
  now()
FROM dedup input
ON CONFLICT (external_id) DO UPDATE SET
  name = CASE
    WHEN trim(EXCLUDED.name) <> '' THEN EXCLUDED.name
    ELSE okta_groups.name
  END,
  type = CASE
    WHEN trim(EXCLUDED.type) <> '' THEN EXCLUDED.type
    ELSE okta_groups.type
  END,
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  updated_at = now()
;

-- name: UpsertOktaAppsBulk :execrows
WITH input AS (
  SELECT
    i,
    (sqlc.arg(external_ids)::text[])[i] AS external_id,
    (sqlc.arg(labels)::text[])[i] AS label,
    (sqlc.arg(names)::text[])[i] AS name,
    (sqlc.arg(statuses)::text[])[i] AS status,
    (sqlc.arg(sign_on_modes)::text[])[i] AS sign_on_mode,
    (sqlc.arg(raw_jsons)::jsonb[])[i] AS raw_json
  FROM generate_subscripts(sqlc.arg(external_ids)::text[], 1) AS s(i)
),
dedup AS (
  SELECT DISTINCT ON (external_id)
    external_id,
    label,
    name,
    status,
    sign_on_mode,
    raw_json
  FROM input
  ORDER BY external_id, i DESC
)
INSERT INTO okta_apps (
  external_id,
  label,
  name,
  status,
  sign_on_mode,
  raw_json,
  seen_in_run_id,
  seen_at,
  updated_at
)
SELECT
  input.external_id,
  input.label,
  input.name,
  input.status,
  input.sign_on_mode,
  input.raw_json,
  sqlc.arg(seen_in_run_id)::bigint,
  now(),
  now()
FROM dedup input
ON CONFLICT (external_id) DO UPDATE SET
  label = CASE
    WHEN trim(EXCLUDED.label) <> '' THEN EXCLUDED.label
    ELSE okta_apps.label
  END,
  name = CASE
    WHEN trim(EXCLUDED.name) <> '' THEN EXCLUDED.name
    ELSE okta_apps.name
  END,
  status = CASE
    WHEN trim(EXCLUDED.status) <> '' THEN EXCLUDED.status
    ELSE okta_apps.status
  END,
  sign_on_mode = CASE
    WHEN trim(EXCLUDED.sign_on_mode) <> '' THEN EXCLUDED.sign_on_mode
    ELSE okta_apps.sign_on_mode
  END,
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  updated_at = now()
;

-- name: UpsertOktaGroupMembershipsBulkByOktaAccountExternalIDs :execrows
WITH run_source AS (
  SELECT sr.source_name
  FROM sync_runs sr
  WHERE sr.id = sqlc.arg(seen_in_run_id)::bigint
  LIMIT 1
),
input AS (
  SELECT
    i,
    (sqlc.arg(okta_account_external_ids)::text[])[i] AS okta_account_external_id,
    (sqlc.arg(okta_group_external_ids)::text[])[i] AS okta_group_external_id
  FROM generate_subscripts(sqlc.arg(okta_account_external_ids)::text[], 1) AS s(i)
),
dedup AS (
  SELECT DISTINCT ON (okta_account_external_id, okta_group_external_id)
    okta_account_external_id,
    okta_group_external_id
  FROM input
  ORDER BY okta_account_external_id, okta_group_external_id, i DESC
)
INSERT INTO okta_user_groups (okta_user_account_id, okta_group_id, seen_in_run_id, seen_at)
SELECT
  iu.id,
  og.id,
  sqlc.arg(seen_in_run_id)::bigint,
  now()
FROM dedup d
JOIN run_source rs ON TRUE
JOIN accounts iu
  ON iu.source_kind = 'okta'
  AND iu.source_name = rs.source_name
  AND iu.external_id = d.okta_account_external_id
  AND (iu.expired_at IS NULL OR iu.seen_in_run_id = sqlc.arg(seen_in_run_id)::bigint)
  AND (
    iu.last_observed_run_id IS NOT NULL
    OR iu.seen_in_run_id = sqlc.arg(seen_in_run_id)::bigint
  )
JOIN okta_groups og ON og.external_id = d.okta_group_external_id
  AND (og.expired_at IS NULL OR og.seen_in_run_id = sqlc.arg(seen_in_run_id)::bigint)
  AND (
    og.last_observed_run_id IS NOT NULL
    OR og.seen_in_run_id = sqlc.arg(seen_in_run_id)::bigint
  )
ON CONFLICT (okta_user_account_id, okta_group_id) DO UPDATE SET
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at
;

-- name: UpsertOktaAppAssignmentsBulkByOktaAccountExternalIDs :execrows
WITH run_source AS (
  SELECT sr.source_name
  FROM sync_runs sr
  WHERE sr.id = sqlc.arg(seen_in_run_id)::bigint
  LIMIT 1
),
input AS (
  SELECT
    i,
    (sqlc.arg(okta_account_external_ids)::text[])[i] AS okta_account_external_id,
    (sqlc.arg(okta_app_external_ids)::text[])[i] AS okta_app_external_id,
    (sqlc.arg(scopes)::text[])[i] AS scope,
    (sqlc.arg(profile_jsons)::jsonb[])[i] AS profile_json,
    (sqlc.arg(raw_jsons)::jsonb[])[i] AS raw_json
  FROM generate_subscripts(sqlc.arg(okta_account_external_ids)::text[], 1) AS s(i)
),
dedup AS (
  SELECT DISTINCT ON (okta_account_external_id, okta_app_external_id)
    okta_account_external_id,
    okta_app_external_id,
    scope,
    profile_json,
    raw_json
  FROM input
  ORDER BY okta_account_external_id, okta_app_external_id, i DESC
)
INSERT INTO okta_user_app_assignments (
  okta_user_account_id,
  okta_app_id,
  scope,
  profile_json,
  raw_json,
  seen_in_run_id,
  seen_at,
  updated_at
)
SELECT
  iu.id,
  oa.id,
  input.scope,
  input.profile_json,
  input.raw_json,
  sqlc.arg(seen_in_run_id)::bigint,
  now(),
  now()
FROM dedup input
JOIN run_source rs ON TRUE
JOIN accounts iu
  ON iu.source_kind = 'okta'
  AND iu.source_name = rs.source_name
  AND iu.external_id = input.okta_account_external_id
  AND (iu.expired_at IS NULL OR iu.seen_in_run_id = sqlc.arg(seen_in_run_id)::bigint)
  AND (
    iu.last_observed_run_id IS NOT NULL
    OR iu.seen_in_run_id = sqlc.arg(seen_in_run_id)::bigint
  )
JOIN okta_apps oa ON oa.external_id = input.okta_app_external_id
  AND (oa.expired_at IS NULL OR oa.seen_in_run_id = sqlc.arg(seen_in_run_id)::bigint)
  AND (
    oa.last_observed_run_id IS NOT NULL
    OR oa.seen_in_run_id = sqlc.arg(seen_in_run_id)::bigint
  )
ON CONFLICT (okta_user_account_id, okta_app_id) DO UPDATE SET
  scope = EXCLUDED.scope,
  profile_json = EXCLUDED.profile_json,
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  updated_at = now()
;

-- name: UpsertOktaAppGroupAssignmentsBulkByExternalIDs :execrows
WITH input AS (
  SELECT
    i,
    (sqlc.arg(okta_app_external_ids)::text[])[i] AS okta_app_external_id,
    (sqlc.arg(okta_group_external_ids)::text[])[i] AS okta_group_external_id,
    (sqlc.arg(priorities)::int[])[i] AS priority,
    (sqlc.arg(profile_jsons)::jsonb[])[i] AS profile_json,
    (sqlc.arg(raw_jsons)::jsonb[])[i] AS raw_json
  FROM generate_subscripts(sqlc.arg(okta_app_external_ids)::text[], 1) AS s(i)
),
dedup AS (
  SELECT DISTINCT ON (okta_app_external_id, okta_group_external_id)
    okta_app_external_id,
    okta_group_external_id,
    priority,
    profile_json,
    raw_json
  FROM input
  ORDER BY okta_app_external_id, okta_group_external_id, i DESC
)
INSERT INTO okta_app_group_assignments (
  okta_app_id,
  okta_group_id,
  priority,
  profile_json,
  raw_json,
  seen_in_run_id,
  seen_at,
  updated_at
)
SELECT
  oa.id,
  og.id,
  input.priority,
  input.profile_json,
  input.raw_json,
  sqlc.arg(seen_in_run_id)::bigint,
  now(),
  now()
FROM dedup input
JOIN okta_apps oa ON oa.external_id = input.okta_app_external_id
  AND (oa.expired_at IS NULL OR oa.seen_in_run_id = sqlc.arg(seen_in_run_id)::bigint)
  AND (
    oa.last_observed_run_id IS NOT NULL
    OR oa.seen_in_run_id = sqlc.arg(seen_in_run_id)::bigint
  )
JOIN okta_groups og ON og.external_id = input.okta_group_external_id
  AND (og.expired_at IS NULL OR og.seen_in_run_id = sqlc.arg(seen_in_run_id)::bigint)
  AND (
    og.last_observed_run_id IS NOT NULL
    OR og.seen_in_run_id = sqlc.arg(seen_in_run_id)::bigint
  )
ON CONFLICT (okta_app_id, okta_group_id) DO UPDATE SET
  priority = EXCLUDED.priority,
  profile_json = EXCLUDED.profile_json,
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  updated_at = now()
;

-- name: ListOktaGroupsForOktaAccount :many
SELECT og.*
FROM okta_groups og
JOIN okta_user_groups ug ON ug.okta_group_id = og.id
WHERE ug.okta_user_account_id = $1
  AND og.expired_at IS NULL
  AND og.last_observed_run_id IS NOT NULL
  AND ug.expired_at IS NULL
  AND ug.last_observed_run_id IS NOT NULL
ORDER BY og.name, og.external_id;

-- name: ListOktaAppAssignmentsForOktaAccount :many
SELECT
  ouaa.okta_user_account_id AS okta_account_id,
  ouaa.okta_app_id,
  ouaa.scope,
  ouaa.profile_json,
  ouaa.raw_json AS assignment_raw_json,
  oa.external_id AS okta_app_external_id,
  oa.label AS app_label,
  oa.name AS app_name,
  oa.status AS app_status,
  oa.sign_on_mode AS app_sign_on_mode,
  COALESCE(m.integration_kind, '') AS integration_kind
FROM okta_user_app_assignments ouaa
JOIN okta_apps oa ON oa.id = ouaa.okta_app_id
LEFT JOIN integration_okta_app_map m ON m.okta_app_external_id = oa.external_id
WHERE ouaa.okta_user_account_id = $1
  AND ouaa.expired_at IS NULL
  AND ouaa.last_observed_run_id IS NOT NULL
  AND oa.expired_at IS NULL
  AND oa.last_observed_run_id IS NOT NULL
ORDER BY oa.label, oa.name, oa.external_id;

-- name: ListOktaAppGroupAssignmentsByAppIDs :many
SELECT
  oga.okta_app_id,
  oga.okta_group_id,
  og.name AS okta_group_name,
  og.external_id AS okta_group_external_id
FROM okta_app_group_assignments oga
LEFT JOIN okta_groups og
  ON og.id = oga.okta_group_id
  AND og.expired_at IS NULL
  AND og.last_observed_run_id IS NOT NULL
WHERE oga.okta_app_id = ANY($1::bigint[])
  AND oga.expired_at IS NULL
  AND oga.last_observed_run_id IS NOT NULL
ORDER BY oga.okta_app_id, og.name, og.external_id;

-- name: CountOktaApps :one
SELECT count(*)
FROM okta_apps
WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

-- name: CountOktaAppsFiltered :one
SELECT count(*)
FROM okta_apps oa
LEFT JOIN integration_okta_app_map m ON m.okta_app_external_id = oa.external_id
WHERE
  oa.expired_at IS NULL
  AND oa.last_observed_run_id IS NOT NULL
  AND (
    sqlc.arg(query)::text = ''
    OR oa.label ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR oa.name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR oa.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
  AND (
    sqlc.arg(status_filter)::text = ''
    OR UPPER(TRIM(oa.status)) = UPPER(TRIM(sqlc.arg(status_filter)::text))
  )
  AND (
    sqlc.arg(integration_filter)::text = ''
    OR (sqlc.arg(integration_filter)::text = 'connected' AND m.integration_kind IS NOT NULL)
    OR (sqlc.arg(integration_filter)::text = 'not_connected' AND m.integration_kind IS NULL)
  );

-- name: ListOktaAppsPageFiltered :many
SELECT
  oa.external_id,
  oa.label,
  oa.name,
  oa.status,
  oa.sign_on_mode,
  COALESCE(m.integration_kind, '') AS integration_kind
FROM okta_apps oa
LEFT JOIN integration_okta_app_map m ON m.okta_app_external_id = oa.external_id
WHERE
  oa.expired_at IS NULL
  AND oa.last_observed_run_id IS NOT NULL
  AND (
    sqlc.arg(query)::text = ''
    OR oa.label ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR oa.name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR oa.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
  AND (
    sqlc.arg(status_filter)::text = ''
    OR UPPER(TRIM(oa.status)) = UPPER(TRIM(sqlc.arg(status_filter)::text))
  )
  AND (
    sqlc.arg(integration_filter)::text = ''
    OR (sqlc.arg(integration_filter)::text = 'connected' AND m.integration_kind IS NOT NULL)
    OR (sqlc.arg(integration_filter)::text = 'not_connected' AND m.integration_kind IS NULL)
  )
ORDER BY (m.integration_kind IS NULL), oa.label, oa.name, oa.external_id
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: ListDistinctOktaAppStatuses :many
SELECT DISTINCT UPPER(TRIM(oa.status))::text AS status
FROM okta_apps oa
WHERE oa.expired_at IS NULL
  AND oa.last_observed_run_id IS NOT NULL
  AND TRIM(oa.status) != ''
ORDER BY status;

-- name: GetOktaAppByExternalIDWithIntegration :one
SELECT
  oa.id,
  oa.external_id,
  oa.label,
  oa.name,
  oa.status,
  oa.sign_on_mode,
  oa.raw_json,
  oa.created_at,
  oa.updated_at,
  COALESCE(m.integration_kind, '') AS integration_kind
FROM okta_apps oa
LEFT JOIN integration_okta_app_map m ON m.okta_app_external_id = oa.external_id
WHERE oa.external_id = $1
  AND oa.expired_at IS NULL
  AND oa.last_observed_run_id IS NOT NULL;

-- name: CountOktaAppAssignedAccountsByQuery :one
SELECT count(*)
FROM okta_user_app_assignments ouaa
JOIN accounts u ON u.id = ouaa.okta_user_account_id
WHERE
  ouaa.okta_app_id = sqlc.arg(okta_app_id)
  AND ouaa.expired_at IS NULL
  AND ouaa.last_observed_run_id IS NOT NULL
  AND u.expired_at IS NULL
  AND u.last_observed_run_id IS NOT NULL
  AND (
    sqlc.arg(state)::text = ''
    OR (sqlc.arg(state)::text = 'active' AND u.status = 'ACTIVE')
    OR (sqlc.arg(state)::text = 'inactive' AND u.status <> 'ACTIVE')
  )
  AND (
    sqlc.arg(query)::text = ''
    OR u.email ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR u.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR u.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
  );

-- name: ListOktaAppAssignedAccountsPageByQuery :many
SELECT
  u.id AS okta_account_id,
  u.external_id AS okta_account_external_id,
  u.email AS okta_account_email,
  u.display_name AS okta_account_display_name,
  u.status AS okta_account_status,
  ouaa.scope,
  ouaa.profile_json
FROM okta_user_app_assignments ouaa
JOIN accounts u ON u.id = ouaa.okta_user_account_id
WHERE
  ouaa.okta_app_id = sqlc.arg(okta_app_id)
  AND ouaa.expired_at IS NULL
  AND ouaa.last_observed_run_id IS NOT NULL
  AND u.expired_at IS NULL
  AND u.last_observed_run_id IS NOT NULL
  AND (
    sqlc.arg(state)::text = ''
    OR (sqlc.arg(state)::text = 'active' AND u.status = 'ACTIVE')
    OR (sqlc.arg(state)::text = 'inactive' AND u.status <> 'ACTIVE')
  )
  AND (
    sqlc.arg(query)::text = ''
    OR u.email ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR u.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR u.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
ORDER BY (u.display_name = ''), u.display_name, u.email, u.external_id
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: ListOktaAppGrantingGroupsForOktaAccounts :many
SELECT
  ug.okta_user_account_id AS okta_account_id,
  og.name AS okta_group_name,
  og.external_id AS okta_group_external_id
FROM okta_user_groups ug
JOIN okta_app_group_assignments oga ON oga.okta_group_id = ug.okta_group_id
JOIN okta_groups og ON og.id = ug.okta_group_id
WHERE
  oga.okta_app_id = sqlc.arg(okta_app_id)
  AND ug.expired_at IS NULL
  AND ug.last_observed_run_id IS NOT NULL
  AND oga.expired_at IS NULL
  AND oga.last_observed_run_id IS NOT NULL
  AND og.expired_at IS NULL
  AND og.last_observed_run_id IS NOT NULL
  AND ug.okta_user_account_id = ANY(sqlc.arg(okta_account_ids)::bigint[])
ORDER BY ug.okta_user_account_id, og.name, og.external_id;

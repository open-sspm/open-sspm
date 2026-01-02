-- name: UpsertOktaGroup :one
INSERT INTO okta_groups (external_id, name, type, raw_json, seen_in_run_id, seen_at, updated_at)
VALUES ($1, $2, $3, $4, $5, now(), now())
ON CONFLICT (external_id) DO UPDATE SET
  name = EXCLUDED.name,
  type = EXCLUDED.type,
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  updated_at = now()
RETURNING *;

-- name: UpsertOktaApp :one
INSERT INTO okta_apps (external_id, label, name, status, sign_on_mode, raw_json, seen_in_run_id, seen_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, now(), now())
ON CONFLICT (external_id) DO UPDATE SET
  label = EXCLUDED.label,
  name = EXCLUDED.name,
  status = EXCLUDED.status,
  sign_on_mode = EXCLUDED.sign_on_mode,
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  updated_at = now()
RETURNING *;

-- name: DeleteOktaUserGroupsForIdpUser :exec
DELETE FROM okta_user_groups WHERE idp_user_id = $1;

-- name: InsertOktaUserGroup :exec
INSERT INTO okta_user_groups (idp_user_id, okta_group_id, seen_in_run_id, seen_at)
VALUES ($1, $2, $3, now())
ON CONFLICT (idp_user_id, okta_group_id) DO UPDATE SET
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at;

-- name: DeleteOktaUserAppAssignmentsForIdpUser :exec
DELETE FROM okta_user_app_assignments WHERE idp_user_id = $1;

-- name: InsertOktaUserAppAssignment :exec
INSERT INTO okta_user_app_assignments (idp_user_id, okta_app_id, scope, profile_json, raw_json, seen_in_run_id, seen_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, now(), now())
ON CONFLICT (idp_user_id, okta_app_id) DO UPDATE SET
  scope = EXCLUDED.scope,
  profile_json = EXCLUDED.profile_json,
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  updated_at = now();

-- name: DeleteOktaAppGroupAssignmentsForApp :exec
DELETE FROM okta_app_group_assignments WHERE okta_app_id = $1;

-- name: InsertOktaAppGroupAssignment :exec
INSERT INTO okta_app_group_assignments (okta_app_id, okta_group_id, priority, profile_json, raw_json, seen_in_run_id, seen_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, now(), now())
ON CONFLICT (okta_app_id, okta_group_id) DO UPDATE SET
  priority = EXCLUDED.priority,
  profile_json = EXCLUDED.profile_json,
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  updated_at = now();

-- name: ListOktaGroupsForIdpUser :many
SELECT og.*
FROM okta_groups og
JOIN okta_user_groups ug ON ug.okta_group_id = og.id
WHERE ug.idp_user_id = $1
  AND og.expired_at IS NULL
  AND og.last_observed_run_id IS NOT NULL
  AND ug.expired_at IS NULL
  AND ug.last_observed_run_id IS NOT NULL
ORDER BY og.name, og.external_id;

-- name: ListOktaUserAppAssignmentsForIdpUser :many
SELECT
  ouaa.idp_user_id,
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
WHERE ouaa.idp_user_id = $1
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

-- name: ListOktaApps :many
SELECT *
FROM okta_apps
WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL
ORDER BY label, name, external_id;

-- name: CountOktaApps :one
SELECT count(*)
FROM okta_apps
WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

-- name: ListOktaAppsPage :many
SELECT
  oa.external_id,
  oa.label,
  oa.name,
  oa.status,
  oa.sign_on_mode,
  COALESCE(m.integration_kind, '') AS integration_kind
FROM okta_apps oa
LEFT JOIN integration_okta_app_map m ON m.okta_app_external_id = oa.external_id
WHERE oa.expired_at IS NULL AND oa.last_observed_run_id IS NOT NULL
ORDER BY (m.integration_kind IS NULL), oa.label, oa.name, oa.external_id
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: ListOktaAppsForCommand :many
SELECT
  oa.external_id,
  oa.label,
  oa.name,
  oa.status,
  oa.sign_on_mode,
  COALESCE(m.integration_kind, '') AS integration_kind
FROM okta_apps oa
LEFT JOIN integration_okta_app_map m ON m.okta_app_external_id = oa.external_id
WHERE oa.expired_at IS NULL AND oa.last_observed_run_id IS NOT NULL
ORDER BY (m.integration_kind IS NULL), oa.label, oa.name, oa.external_id;

-- name: CountOktaAppsByQuery :one
SELECT count(*)
FROM okta_apps oa
WHERE
  oa.expired_at IS NULL
  AND oa.last_observed_run_id IS NOT NULL
  AND (
    sqlc.arg(query)::text = ''
    OR oa.label ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR oa.name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR oa.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
  );

-- name: ListOktaAppsPageByQuery :many
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
ORDER BY (m.integration_kind IS NULL), oa.label, oa.name, oa.external_id
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

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

-- name: CountOktaAppUserAssignmentsByQuery :one
SELECT count(*)
FROM okta_user_app_assignments ouaa
JOIN idp_users u ON u.id = ouaa.idp_user_id
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

-- name: ListOktaAppUserAssignmentsPageByQuery :many
SELECT
  u.id AS idp_user_id,
  u.external_id AS idp_user_external_id,
  u.email AS idp_user_email,
  u.display_name AS idp_user_display_name,
  u.status AS idp_user_status,
  ouaa.scope,
  ouaa.profile_json
FROM okta_user_app_assignments ouaa
JOIN idp_users u ON u.id = ouaa.idp_user_id
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

-- name: ListOktaAppGrantingGroupsForIdpUsers :many
SELECT
  ug.idp_user_id,
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
  AND ug.idp_user_id = ANY(sqlc.arg(idp_user_ids)::bigint[])
ORDER BY ug.idp_user_id, og.name, og.external_id;

-- name: GetOktaUserAppAssignmentForIdpUserByOktaAppExternalID :one
SELECT
  ouaa.idp_user_id,
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
WHERE
  ouaa.idp_user_id = sqlc.arg(idp_user_id)
  AND ouaa.expired_at IS NULL
  AND ouaa.last_observed_run_id IS NOT NULL
  AND oa.external_id = sqlc.arg(okta_app_external_id)
  AND oa.expired_at IS NULL
  AND oa.last_observed_run_id IS NOT NULL
LIMIT 1;

-- name: ListOktaAppGrantingGroupsForIdpUserByOktaAppExternalID :many
SELECT
  og.name AS okta_group_name,
  og.external_id AS okta_group_external_id
FROM okta_user_groups ug
JOIN okta_app_group_assignments oga ON oga.okta_group_id = ug.okta_group_id
JOIN okta_groups og ON og.id = ug.okta_group_id
JOIN okta_apps oa ON oa.id = oga.okta_app_id
WHERE
  ug.idp_user_id = sqlc.arg(idp_user_id)
  AND ug.expired_at IS NULL
  AND ug.last_observed_run_id IS NOT NULL
  AND oga.expired_at IS NULL
  AND oga.last_observed_run_id IS NOT NULL
  AND og.expired_at IS NULL
  AND og.last_observed_run_id IS NOT NULL
  AND oa.external_id = sqlc.arg(okta_app_external_id)
  AND oa.expired_at IS NULL
  AND oa.last_observed_run_id IS NOT NULL
ORDER BY og.name, og.external_id;

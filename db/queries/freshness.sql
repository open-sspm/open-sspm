-- name: PromoteIdPUsersSeenInRun :execrows
UPDATE idp_users
SET
  last_observed_run_id = $1,
  last_observed_at = now(),
  expired_at = NULL,
  expired_run_id = NULL
WHERE seen_in_run_id = $1;

-- name: ExpireIdPUsersNotSeenInRun :execrows
UPDATE idp_users
SET
  expired_at = now(),
  expired_run_id = $1
WHERE expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
  AND (seen_in_run_id <> $1 OR seen_in_run_id IS NULL);

-- name: PromoteOktaGroupsSeenInRun :execrows
UPDATE okta_groups
SET
  last_observed_run_id = $1,
  last_observed_at = now(),
  expired_at = NULL,
  expired_run_id = NULL
WHERE seen_in_run_id = $1;

-- name: ExpireOktaGroupsNotSeenInRun :execrows
UPDATE okta_groups
SET
  expired_at = now(),
  expired_run_id = $1
WHERE expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
  AND (seen_in_run_id <> $1 OR seen_in_run_id IS NULL);

-- name: PromoteOktaUserGroupsSeenInRun :execrows
UPDATE okta_user_groups
SET
  last_observed_run_id = $1,
  last_observed_at = now(),
  expired_at = NULL,
  expired_run_id = NULL
WHERE seen_in_run_id = $1;

-- name: ExpireOktaUserGroupsNotSeenInRun :execrows
UPDATE okta_user_groups
SET
  expired_at = now(),
  expired_run_id = $1
WHERE expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
  AND (seen_in_run_id <> $1 OR seen_in_run_id IS NULL);

-- name: PromoteOktaAppsSeenInRun :execrows
UPDATE okta_apps
SET
  last_observed_run_id = $1,
  last_observed_at = now(),
  expired_at = NULL,
  expired_run_id = NULL
WHERE seen_in_run_id = $1;

-- name: ExpireOktaAppsNotSeenInRun :execrows
UPDATE okta_apps
SET
  expired_at = now(),
  expired_run_id = $1
WHERE expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
  AND (seen_in_run_id <> $1 OR seen_in_run_id IS NULL);

-- name: PromoteOktaUserAppAssignmentsSeenInRun :execrows
UPDATE okta_user_app_assignments
SET
  last_observed_run_id = $1,
  last_observed_at = now(),
  expired_at = NULL,
  expired_run_id = NULL
WHERE seen_in_run_id = $1;

-- name: ExpireOktaUserAppAssignmentsNotSeenInRun :execrows
UPDATE okta_user_app_assignments
SET
  expired_at = now(),
  expired_run_id = $1
WHERE expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
  AND (seen_in_run_id <> $1 OR seen_in_run_id IS NULL);

-- name: PromoteOktaAppGroupAssignmentsSeenInRun :execrows
UPDATE okta_app_group_assignments
SET
  last_observed_run_id = $1,
  last_observed_at = now(),
  expired_at = NULL,
  expired_run_id = NULL
WHERE seen_in_run_id = $1;

-- name: ExpireOktaAppGroupAssignmentsNotSeenInRun :execrows
UPDATE okta_app_group_assignments
SET
  expired_at = now(),
  expired_run_id = $1
WHERE expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
  AND (seen_in_run_id <> $1 OR seen_in_run_id IS NULL);

-- name: PromoteAppUsersSeenInRun :execrows
UPDATE app_users
SET
  last_observed_run_id = $1,
  last_observed_at = now(),
  expired_at = NULL,
  expired_run_id = NULL
WHERE source_kind = $2
  AND source_name = $3
  AND seen_in_run_id = $1;

-- name: ExpireAppUsersNotSeenInRun :execrows
UPDATE app_users
SET
  expired_at = now(),
  expired_run_id = $1
WHERE source_kind = $2
  AND source_name = $3
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
  AND (seen_in_run_id <> $1 OR seen_in_run_id IS NULL);

-- name: PromoteEntitlementsSeenInRunBySource :execrows
UPDATE entitlements e
SET
  last_observed_run_id = $1,
  last_observed_at = now(),
  expired_at = NULL,
  expired_run_id = NULL
FROM app_users au
WHERE au.id = e.app_user_id
  AND au.source_kind = $2
  AND au.source_name = $3
  AND e.seen_in_run_id = $1;

-- name: ExpireEntitlementsNotSeenInRunBySource :execrows
UPDATE entitlements e
SET
  expired_at = now(),
  expired_run_id = $1
FROM app_users au
WHERE au.id = e.app_user_id
  AND au.source_kind = $2
  AND au.source_name = $3
  AND e.expired_at IS NULL
  AND e.last_observed_run_id IS NOT NULL
  AND (e.seen_in_run_id <> $1 OR e.seen_in_run_id IS NULL);


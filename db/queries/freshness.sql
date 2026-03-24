-- name: PromoteOktaAccountsSeenInRun :execrows
UPDATE accounts
SET
  last_observed_run_id = $1,
  last_observed_at = now(),
  expired_at = NULL,
  expired_run_id = NULL
WHERE source_kind = 'okta'
  AND seen_in_run_id = $1;

-- name: ExpireOktaAccountsNotSeenInRun :execrows
UPDATE accounts
SET
  expired_at = now(),
  expired_run_id = $1
WHERE source_kind = 'okta'
  AND expired_at IS NULL
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

-- name: PromoteOktaGroupMembershipsSeenInRun :execrows
UPDATE okta_user_groups
SET
  last_observed_run_id = $1,
  last_observed_at = now(),
  expired_at = NULL,
  expired_run_id = NULL
WHERE seen_in_run_id = $1;

-- name: ExpireOktaGroupMembershipsNotSeenInRun :execrows
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

-- name: PromoteOktaAppAssignmentsSeenInRun :execrows
UPDATE okta_user_app_assignments
SET
  last_observed_run_id = $1,
  last_observed_at = now(),
  expired_at = NULL,
  expired_run_id = NULL
WHERE seen_in_run_id = $1;

-- name: ExpireOktaAppAssignmentsNotSeenInRun :execrows
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

-- name: PromoteSourceAccountsSeenInRun :execrows
UPDATE accounts
SET
  last_observed_run_id = $1,
  last_observed_at = now(),
  expired_at = NULL,
  expired_run_id = NULL
WHERE source_kind = $2
  AND source_name = $3
  AND seen_in_run_id = $1;

-- name: ExpireSourceAccountsNotSeenInRun :execrows
UPDATE accounts
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
FROM accounts au
WHERE au.id = e.app_user_id
  AND au.source_kind = $2
  AND au.source_name = $3
  AND e.seen_in_run_id = $1;

-- name: ExpireEntitlementsNotSeenInRunBySource :execrows
UPDATE entitlements e
SET
  expired_at = now(),
  expired_run_id = $1
FROM accounts au
WHERE au.id = e.app_user_id
  AND au.source_kind = $2
  AND au.source_name = $3
  AND e.expired_at IS NULL
  AND e.last_observed_run_id IS NOT NULL
  AND (e.seen_in_run_id <> $1 OR e.seen_in_run_id IS NULL);

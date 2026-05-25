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
-- PHASE-TWO-DELETE: unscoped Okta freshness helper retained for legacy direct-write compatibility; record projection uses source-scoped expiration.
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
-- PHASE-TWO-DELETE: unscoped Okta group expiration retained for legacy direct-write compatibility; record projection uses source-scoped expiration.
UPDATE okta_groups
SET
  expired_at = now(),
  expired_run_id = $1
WHERE expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
  AND (seen_in_run_id <> $1 OR seen_in_run_id IS NULL);

-- name: PromoteOktaGroupsSeenInRunBySource :execrows
UPDATE okta_groups
SET
  last_observed_run_id = sqlc.arg(last_observed_run_id)::bigint,
  last_observed_at = now(),
  expired_at = NULL,
  expired_run_id = NULL
WHERE source_kind = sqlc.arg(source_kind)::text
  AND source_name = sqlc.arg(source_name)::text
  AND seen_in_run_id = sqlc.arg(last_observed_run_id)::bigint;

-- name: ExpireOktaGroupsNotSeenInRunBySource :execrows
UPDATE okta_groups
SET
  expired_at = now(),
  expired_run_id = sqlc.arg(expired_run_id)::bigint
WHERE source_kind = sqlc.arg(source_kind)::text
  AND source_name = sqlc.arg(source_name)::text
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
  AND (
    seen_in_run_id <> sqlc.arg(expired_run_id)::bigint
    OR seen_in_run_id IS NULL
  );

-- name: PromoteOktaGroupMembershipsSeenInRun :execrows
UPDATE okta_user_groups
SET
  last_observed_run_id = $1,
  last_observed_at = now(),
  expired_at = NULL,
  expired_run_id = NULL
WHERE seen_in_run_id = $1;

-- name: PromoteOktaGroupMembershipsSeenInRunBySource :execrows
UPDATE okta_user_groups ug
SET
  last_observed_run_id = sqlc.arg(last_observed_run_id)::bigint,
  last_observed_at = now(),
  expired_at = NULL,
  expired_run_id = NULL
FROM accounts au
WHERE au.id = ug.okta_user_account_id
  AND au.source_kind = sqlc.arg(source_kind)::text
  AND au.source_name = sqlc.arg(source_name)::text
  AND ug.seen_in_run_id = sqlc.arg(last_observed_run_id)::bigint;

-- name: ExpireOktaGroupMembershipsNotSeenInRun :execrows
-- PHASE-TWO-DELETE: unscoped Okta membership expiration retained for legacy direct-write compatibility; record projection uses source-scoped expiration.
UPDATE okta_user_groups
SET
  expired_at = now(),
  expired_run_id = $1
WHERE expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
  AND (seen_in_run_id <> $1 OR seen_in_run_id IS NULL);

-- name: ExpireOktaGroupMembershipsNotSeenInRunBySource :execrows
UPDATE okta_user_groups ug
SET
  expired_at = now(),
  expired_run_id = sqlc.arg(expired_run_id)::bigint
FROM accounts au
WHERE au.id = ug.okta_user_account_id
  AND au.source_kind = sqlc.arg(source_kind)::text
  AND au.source_name = sqlc.arg(source_name)::text
  AND ug.expired_at IS NULL
  AND ug.last_observed_run_id IS NOT NULL
  AND (
    ug.seen_in_run_id <> sqlc.arg(expired_run_id)::bigint
    OR ug.seen_in_run_id IS NULL
  );

-- name: PromoteOktaAppsSeenInRun :execrows
UPDATE okta_apps
SET
  last_observed_run_id = $1,
  last_observed_at = now(),
  expired_at = NULL,
  expired_run_id = NULL
WHERE seen_in_run_id = $1;

-- name: ExpireOktaAppsNotSeenInRun :execrows
-- PHASE-TWO-DELETE: unscoped Okta app expiration retained for legacy direct-write compatibility; record projection uses source-scoped expiration.
UPDATE okta_apps
SET
  expired_at = now(),
  expired_run_id = $1
WHERE expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
  AND (seen_in_run_id <> $1 OR seen_in_run_id IS NULL);

-- name: PromoteOktaAppsSeenInRunBySource :execrows
UPDATE okta_apps
SET
  last_observed_run_id = sqlc.arg(last_observed_run_id)::bigint,
  last_observed_at = now(),
  expired_at = NULL,
  expired_run_id = NULL
WHERE source_kind = sqlc.arg(source_kind)::text
  AND source_name = sqlc.arg(source_name)::text
  AND seen_in_run_id = sqlc.arg(last_observed_run_id)::bigint;

-- name: ExpireOktaAppsNotSeenInRunBySource :execrows
UPDATE okta_apps
SET
  expired_at = now(),
  expired_run_id = sqlc.arg(expired_run_id)::bigint
WHERE source_kind = sqlc.arg(source_kind)::text
  AND source_name = sqlc.arg(source_name)::text
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
  AND (
    seen_in_run_id <> sqlc.arg(expired_run_id)::bigint
    OR seen_in_run_id IS NULL
  );

-- name: PromoteOktaAppAssignmentsSeenInRun :execrows
UPDATE okta_user_app_assignments
SET
  last_observed_run_id = $1,
  last_observed_at = now(),
  expired_at = NULL,
  expired_run_id = NULL
WHERE seen_in_run_id = $1;

-- name: PromoteOktaAppAssignmentsSeenInRunBySource :execrows
UPDATE okta_user_app_assignments ua
SET
  last_observed_run_id = sqlc.arg(last_observed_run_id)::bigint,
  last_observed_at = now(),
  expired_at = NULL,
  expired_run_id = NULL
FROM accounts au
WHERE au.id = ua.okta_user_account_id
  AND au.source_kind = sqlc.arg(source_kind)::text
  AND au.source_name = sqlc.arg(source_name)::text
  AND ua.seen_in_run_id = sqlc.arg(last_observed_run_id)::bigint;

-- name: ExpireOktaAppAssignmentsNotSeenInRun :execrows
-- PHASE-TWO-DELETE: unscoped Okta app assignment expiration retained for legacy direct-write compatibility; record projection uses source-scoped expiration.
UPDATE okta_user_app_assignments
SET
  expired_at = now(),
  expired_run_id = $1
WHERE expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
  AND (seen_in_run_id <> $1 OR seen_in_run_id IS NULL);

-- name: ExpireOktaAppAssignmentsNotSeenInRunBySource :execrows
UPDATE okta_user_app_assignments ua
SET
  expired_at = now(),
  expired_run_id = sqlc.arg(expired_run_id)::bigint
FROM accounts au
WHERE au.id = ua.okta_user_account_id
  AND au.source_kind = sqlc.arg(source_kind)::text
  AND au.source_name = sqlc.arg(source_name)::text
  AND ua.expired_at IS NULL
  AND ua.last_observed_run_id IS NOT NULL
  AND (
    ua.seen_in_run_id <> sqlc.arg(expired_run_id)::bigint
    OR ua.seen_in_run_id IS NULL
  );

-- name: PromoteOktaAppGroupAssignmentsSeenInRun :execrows
UPDATE okta_app_group_assignments
SET
  last_observed_run_id = $1,
  last_observed_at = now(),
  expired_at = NULL,
  expired_run_id = NULL
WHERE seen_in_run_id = $1;

-- name: ExpireOktaAppGroupAssignmentsNotSeenInRun :execrows
-- PHASE-TWO-DELETE: unscoped Okta app group assignment expiration retained for legacy direct-write compatibility; record projection uses source-scoped expiration.
UPDATE okta_app_group_assignments
SET
  expired_at = now(),
  expired_run_id = $1
WHERE expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
  AND (seen_in_run_id <> $1 OR seen_in_run_id IS NULL);

-- name: PromoteOktaAppGroupAssignmentsSeenInRunBySource :execrows
UPDATE okta_app_group_assignments
SET
  last_observed_run_id = sqlc.arg(last_observed_run_id)::bigint,
  last_observed_at = now(),
  expired_at = NULL,
  expired_run_id = NULL
WHERE source_kind = sqlc.arg(source_kind)::text
  AND source_name = sqlc.arg(source_name)::text
  AND seen_in_run_id = sqlc.arg(last_observed_run_id)::bigint;

-- name: ExpireOktaAppGroupAssignmentsNotSeenInRunBySource :execrows
UPDATE okta_app_group_assignments
SET
  expired_at = now(),
  expired_run_id = sqlc.arg(expired_run_id)::bigint
WHERE source_kind = sqlc.arg(source_kind)::text
  AND source_name = sqlc.arg(source_name)::text
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
  AND (
    seen_in_run_id <> sqlc.arg(expired_run_id)::bigint
    OR seen_in_run_id IS NULL
  );

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

-- name: ExpireSourceAccountsByExternalIDs :execrows
UPDATE accounts
SET
  expired_at = now(),
  expired_run_id = sqlc.arg(expired_run_id)::bigint
WHERE source_kind = sqlc.arg(source_kind)::text
  AND source_name = sqlc.arg(source_name)::text
  AND expired_at IS NULL
  AND last_observed_run_id IS NOT NULL
  AND external_id = ANY(sqlc.arg(external_ids)::text[]);

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

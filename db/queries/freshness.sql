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

-- Demo breadth seed: AWS Identity Center and Vault coverage.

BEGIN;

CREATE TEMP TABLE demo_seed_ctx_v5 (
  now_ts TIMESTAMPTZ NOT NULL,
  aws_region TEXT NOT NULL,
  vault_source_name TEXT NOT NULL,
  aws_run_id BIGINT NOT NULL,
  vault_run_id BIGINT NOT NULL
) ON COMMIT DROP;

TRUNCATE demo_seed_ctx_v5;

WITH
  aws_existing AS (
    SELECT id
    FROM sync_runs
    WHERE source_kind = 'aws'
      AND source_name = 'eu-west-1'
      AND status = 'success'
    ORDER BY finished_at DESC NULLS LAST, id DESC
    LIMIT 1
  ),
  aws_ins AS (
    INSERT INTO sync_runs (source_kind, source_name, status, started_at, finished_at, message, stats, error_kind)
    SELECT 'aws', 'eu-west-1', 'success', now() - interval '13 minutes', now() - interval '8 minutes', 'seeded fallback AWS sync', '{}'::jsonb, ''
    WHERE NOT EXISTS (SELECT 1 FROM aws_existing)
    RETURNING id
  ),
  vault_existing AS (
    SELECT id
    FROM sync_runs
    WHERE source_kind = 'vault'
      AND source_name = 'demo-vault'
      AND status = 'success'
    ORDER BY finished_at DESC NULLS LAST, id DESC
    LIMIT 1
  ),
  vault_ins AS (
    INSERT INTO sync_runs (source_kind, source_name, status, started_at, finished_at, message, stats, error_kind)
    SELECT 'vault', 'demo-vault', 'success', now() - interval '14 minutes', now() - interval '9 minutes', 'seeded fallback Vault sync', '{}'::jsonb, ''
    WHERE NOT EXISTS (SELECT 1 FROM vault_existing)
    RETURNING id
  )
INSERT INTO demo_seed_ctx_v5 (now_ts, aws_region, vault_source_name, aws_run_id, vault_run_id)
SELECT
  now(),
  'eu-west-1',
  'demo-vault',
  COALESCE((SELECT id FROM aws_existing), (SELECT id FROM aws_ins)),
  COALESCE((SELECT id FROM vault_existing), (SELECT id FROM vault_ins))
;

-- AWS Identity Center users.
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v5),
  users AS (
    SELECT
      i,
      format('aws-user-%s', to_char(i, 'FM000')) AS external_id,
      CASE
        WHEN i <= 35 THEN format('demo.user%s@example.com', to_char(i, 'FM000'))
        ELSE format('aws.contractor%s@example.com', to_char(i, 'FM000'))
      END AS email,
      format('AWS User %s', to_char(i, 'FM000')) AS display_name,
      CASE
        WHEN (i % 13) = 0 THEN 'inactive'
        ELSE 'active'
      END AS status
    FROM generate_series(1, 50) AS s(i)
  )
INSERT INTO accounts (
  source_kind,
  source_name,
  external_id,
  email,
  display_name,
  status,
  account_kind,
  entity_category,
  raw_json,
  last_login_at,
  last_login_ip,
  last_login_region,
  seen_in_run_id,
  seen_at,
  last_observed_run_id,
  last_observed_at,
  expired_at,
  expired_run_id,
  updated_at
)
SELECT
  'aws',
  ctx.aws_region,
  users.external_id,
  lower(users.email),
  users.display_name,
  users.status,
  'human',
  'user',
  jsonb_build_object(
    'user_id', users.external_id,
    'email', users.email,
    'display_name', users.display_name,
    'status', users.status,
    'entity_category', 'user'
  ),
  NULL::timestamptz,
  ''::text,
  ''::text,
  ctx.aws_run_id,
  ctx.now_ts,
  ctx.aws_run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint,
  ctx.now_ts
FROM users
CROSS JOIN ctx
ON CONFLICT (source_kind, source_name, external_id) DO UPDATE SET
  email = EXCLUDED.email,
  display_name = EXCLUDED.display_name,
  status = EXCLUDED.status,
  account_kind = EXCLUDED.account_kind,
  entity_category = EXCLUDED.entity_category,
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  last_observed_run_id = EXCLUDED.last_observed_run_id,
  last_observed_at = EXCLUDED.last_observed_at,
  expired_at = NULL,
  expired_run_id = NULL,
  updated_at = EXCLUDED.updated_at
;

-- About 140 AWS permission-set assignments across 12 accounts and 3 permission sets.
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v5),
  aws_users AS (
    SELECT
      id,
      regexp_replace(external_id, '^aws-user-', '')::int AS user_ord
    FROM accounts
    CROSS JOIN ctx
    WHERE source_kind = 'aws'
      AND source_name = ctx.aws_region
      AND external_id LIKE 'aws-user-%'
      AND expired_at IS NULL
  ),
  aws_accounts (account_ord, account_id) AS (
    VALUES
      ( 1, '410000000001'),
      ( 2, '410000000002'),
      ( 3, '410000000003'),
      ( 4, '410000000004'),
      ( 5, '410000000005'),
      ( 6, '410000000006'),
      ( 7, '410000000007'),
      ( 8, '410000000008'),
      ( 9, '410000000009'),
      (10, '410000000010'),
      (11, '410000000011'),
      (12, '410000000012')
  ),
  combos AS (
    SELECT
      aws_users.id AS app_user_id,
      format('aws_account:%s', aws_accounts.account_id) AS resource,
      CASE ((aws_users.user_ord + aws_accounts.account_ord) % 3)
        WHEN 0 THEN 'AdminAccess'
        WHEN 1 THEN 'PowerUserAccess'
        ELSE 'ReadOnlyAccess'
      END AS permission,
      row_number() OVER (ORDER BY aws_users.user_ord, aws_accounts.account_ord) AS rn
    FROM aws_users
    CROSS JOIN aws_accounts
    WHERE ((aws_users.user_ord + aws_accounts.account_ord) % 4) <> 0
  )
INSERT INTO entitlements (
  app_user_id,
  kind,
  resource,
  permission,
  raw_json,
  seen_in_run_id,
  seen_at,
  last_observed_run_id,
  last_observed_at,
  expired_at,
  expired_run_id,
  updated_at
)
SELECT
  combos.app_user_id,
  'aws_permission_set',
  combos.resource,
  combos.permission,
  jsonb_build_object(
    'account', combos.resource,
    'permission_set', combos.permission
  ),
  ctx.aws_run_id,
  ctx.now_ts,
  ctx.aws_run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint,
  ctx.now_ts
FROM combos
CROSS JOIN ctx
WHERE combos.rn <= 140
ON CONFLICT (app_user_id, kind, resource, permission) DO UPDATE SET
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  last_observed_run_id = EXCLUDED.last_observed_run_id,
  last_observed_at = EXCLUDED.last_observed_at,
  expired_at = NULL,
  expired_run_id = NULL,
  updated_at = EXCLUDED.updated_at
;

-- Link AWS users to existing identities by email.
WITH
  authoritative_identities AS (
    SELECT DISTINCT ia.identity_id
    FROM identity_accounts ia
    JOIN accounts anchor ON anchor.id = ia.account_id
    JOIN identity_source_settings iss
      ON iss.source_kind = anchor.source_kind
     AND iss.source_name = anchor.source_name
     AND iss.is_authoritative
    WHERE anchor.expired_at IS NULL
      AND anchor.last_observed_run_id IS NOT NULL
  ),
  aws_accounts AS (
    SELECT
      a.id AS account_id,
      lower(a.email) AS email
    FROM accounts a
    WHERE a.source_kind = 'aws'
      AND a.source_name = (SELECT aws_region FROM demo_seed_ctx_v5)
      AND a.entity_category = 'user'
      AND a.email <> ''
      AND a.expired_at IS NULL
  ),
  matches AS (
    SELECT
      aws_accounts.account_id,
      identity_match.id AS identity_id
    FROM aws_accounts
    JOIN LATERAL (
      SELECT i.id
      FROM identities i
      LEFT JOIN authoritative_identities ai ON ai.identity_id = i.id
      WHERE lower(i.primary_email) = aws_accounts.email
      ORDER BY (ai.identity_id IS NOT NULL) DESC, i.id ASC
      LIMIT 1
    ) identity_match ON TRUE
  )
INSERT INTO identity_accounts (identity_id, account_id, link_reason, confidence, updated_at)
SELECT
  matches.identity_id,
  matches.account_id,
  'demo_aws_email',
  1.0,
  now()
FROM matches
ON CONFLICT (account_id) DO UPDATE SET
  identity_id = EXCLUDED.identity_id,
  link_reason = EXCLUDED.link_reason,
  confidence = EXCLUDED.confidence,
  updated_at = EXCLUDED.updated_at
;

-- Vault principals: humans, service principals, bots, groups, and auth roles.
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v5),
  entity_principals (external_id, email, display_name, account_kind, status) AS (
    VALUES
      ('entity:vault-user-001', 'demo.user040@example.com', 'Vault User 001', 'human', 'active'),
      ('entity:vault-user-002', 'demo.user041@example.com', 'Vault User 002', 'human', 'active'),
      ('entity:vault-user-003', 'demo.user042@example.com', 'Vault User 003', 'human', 'active'),
      ('entity:vault-user-004', 'demo.user043@example.com', 'Vault User 004', 'human', 'active'),
      ('entity:vault-service-payments', '', 'Vault Payments Service', 'service', 'active'),
      ('entity:vault-service-build', '', 'Vault Build Service', 'service', 'active'),
      ('entity:vault-bot-rotation', '', 'Vault Rotation Bot', 'bot', 'active'),
      ('entity:vault-bot-sync', '', 'Vault Sync Bot', 'bot', 'active')
  ),
  groups (external_id, display_name) AS (
    VALUES
      ('group:g-platform', 'Platform Admins'),
      ('group:g-secrets', 'Secrets Operators'),
      ('group:g-breakglass', 'Breakglass Reviewers')
  ),
  auth_roles (external_id, display_name) AS (
    VALUES
      ('role:oidc:oidc:platform-admin', 'platform-admin'),
      ('role:oidc:oidc:security-auditor', 'security-auditor'),
      ('role:approle:approle:ci-deployer', 'ci-deployer'),
      ('role:approle:approle:release-bot', 'release-bot'),
      ('role:kubernetes:kubernetes:payments-service', 'payments-service'),
      ('role:jwt:jwt:analytics-reader', 'analytics-reader')
  ),
  all_principals AS (
    SELECT
      external_id,
      email,
      display_name,
      account_kind,
      'entity'::text AS entity_category,
      status,
      jsonb_build_object('status', status, 'entity_category', 'entity') AS raw_json
    FROM entity_principals

    UNION ALL

    SELECT
      external_id,
      '' AS email,
      display_name,
      'service',
      'group',
      'active',
      jsonb_build_object('status', 'active', 'entity_category', 'group')
    FROM groups

    UNION ALL

    SELECT
      external_id,
      '' AS email,
      display_name,
      'service',
      'auth_role',
      'active',
      jsonb_build_object('status', 'active', 'entity_category', 'auth_role')
    FROM auth_roles
  )
INSERT INTO accounts (
  source_kind,
  source_name,
  external_id,
  email,
  display_name,
  status,
  account_kind,
  entity_category,
  raw_json,
  last_login_at,
  last_login_ip,
  last_login_region,
  seen_in_run_id,
  seen_at,
  last_observed_run_id,
  last_observed_at,
  expired_at,
  expired_run_id,
  updated_at
)
SELECT
  'vault',
  ctx.vault_source_name,
  all_principals.external_id,
  lower(all_principals.email),
  all_principals.display_name,
  all_principals.status,
  all_principals.account_kind,
  all_principals.entity_category,
  all_principals.raw_json,
  NULL::timestamptz,
  ''::text,
  ''::text,
  ctx.vault_run_id,
  ctx.now_ts,
  ctx.vault_run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint,
  ctx.now_ts
FROM all_principals
CROSS JOIN ctx
ON CONFLICT (source_kind, source_name, external_id) DO UPDATE SET
  email = EXCLUDED.email,
  display_name = EXCLUDED.display_name,
  status = EXCLUDED.status,
  account_kind = EXCLUDED.account_kind,
  entity_category = EXCLUDED.entity_category,
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  last_observed_run_id = EXCLUDED.last_observed_run_id,
  last_observed_at = EXCLUDED.last_observed_at,
  expired_at = NULL,
  expired_run_id = NULL,
  updated_at = EXCLUDED.updated_at
;

-- Vault policy, group membership, and auth-role entitlements.
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v5),
  vault_accounts AS (
    SELECT id, external_id
    FROM accounts
    CROSS JOIN ctx
    WHERE source_kind = 'vault'
      AND source_name = ctx.vault_source_name
      AND expired_at IS NULL
  ),
  ents (account_external_id, kind, resource, permission, raw_json) AS (
    VALUES
      ('entity:vault-user-001', 'vault_entity_policy', 'vault_policy:platform-admin', 'attached', jsonb_build_object('policy', 'platform-admin')),
      ('entity:vault-user-002', 'vault_entity_policy', 'vault_policy:security-audit', 'attached', jsonb_build_object('policy', 'security-audit')),
      ('entity:vault-user-003', 'vault_entity_policy', 'vault_policy:secrets-read', 'attached', jsonb_build_object('policy', 'secrets-read')),
      ('entity:vault-user-004', 'vault_entity_policy', 'vault_policy:breakglass-review', 'attached', jsonb_build_object('policy', 'breakglass-review')),
      ('entity:vault-service-payments', 'vault_entity_policy', 'vault_policy:payments-service', 'attached', jsonb_build_object('policy', 'payments-service')),
      ('entity:vault-service-build', 'vault_entity_policy', 'vault_policy:build-service', 'attached', jsonb_build_object('policy', 'build-service')),
      ('entity:vault-bot-rotation', 'vault_entity_policy', 'vault_policy:rotation-bot', 'attached', jsonb_build_object('policy', 'rotation-bot')),
      ('entity:vault-bot-sync', 'vault_entity_policy', 'vault_policy:sync-bot', 'attached', jsonb_build_object('policy', 'sync-bot')),
      ('entity:vault-user-001', 'vault_group_member', 'vault_group:Platform Admins', 'member', jsonb_build_object('group_name', 'Platform Admins')),
      ('entity:vault-user-002', 'vault_group_member', 'vault_group:Secrets Operators', 'member', jsonb_build_object('group_name', 'Secrets Operators')),
      ('entity:vault-user-003', 'vault_group_member', 'vault_group:Breakglass Reviewers', 'member', jsonb_build_object('group_name', 'Breakglass Reviewers')),
      ('entity:vault-service-payments', 'vault_group_member', 'vault_group:Secrets Operators', 'member', jsonb_build_object('group_name', 'Secrets Operators')),
      ('entity:vault-user-001', 'vault_group_policy', 'vault_policy:platform-admin', 'attached', jsonb_build_object('group_name', 'Platform Admins', 'policy', 'platform-admin')),
      ('entity:vault-user-002', 'vault_group_policy', 'vault_policy:secrets-read', 'attached', jsonb_build_object('group_name', 'Secrets Operators', 'policy', 'secrets-read')),
      ('entity:vault-user-003', 'vault_group_policy', 'vault_policy:breakglass-review', 'attached', jsonb_build_object('group_name', 'Breakglass Reviewers', 'policy', 'breakglass-review')),
      ('entity:vault-service-payments', 'vault_group_policy', 'vault_policy:payments-service', 'attached', jsonb_build_object('group_name', 'Secrets Operators', 'policy', 'payments-service')),
      ('role:oidc:oidc:platform-admin', 'vault_auth_role_policy', 'vault_policy:platform-admin', 'attached', jsonb_build_object('role', 'platform-admin')),
      ('role:oidc:oidc:security-auditor', 'vault_auth_role_policy', 'vault_policy:security-audit', 'attached', jsonb_build_object('role', 'security-auditor')),
      ('role:approle:approle:ci-deployer', 'vault_auth_role_policy', 'vault_policy:build-service', 'attached', jsonb_build_object('role', 'ci-deployer')),
      ('role:approle:approle:release-bot', 'vault_auth_role_policy', 'vault_policy:release-bot', 'attached', jsonb_build_object('role', 'release-bot')),
      ('role:kubernetes:kubernetes:payments-service', 'vault_auth_role_policy', 'vault_policy:payments-service', 'attached', jsonb_build_object('role', 'payments-service')),
      ('role:jwt:jwt:analytics-reader', 'vault_auth_role_policy', 'vault_policy:analytics-read', 'attached', jsonb_build_object('role', 'analytics-reader'))
  )
INSERT INTO entitlements (
  app_user_id,
  kind,
  resource,
  permission,
  raw_json,
  seen_in_run_id,
  seen_at,
  last_observed_run_id,
  last_observed_at,
  expired_at,
  expired_run_id,
  updated_at
)
SELECT
  vault_accounts.id,
  ents.kind,
  ents.resource,
  ents.permission,
  ents.raw_json,
  ctx.vault_run_id,
  ctx.now_ts,
  ctx.vault_run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint,
  ctx.now_ts
FROM ents
JOIN vault_accounts ON vault_accounts.external_id = ents.account_external_id
CROSS JOIN ctx
ON CONFLICT (app_user_id, kind, resource, permission) DO UPDATE SET
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  last_observed_run_id = EXCLUDED.last_observed_run_id,
  last_observed_at = EXCLUDED.last_observed_at,
  expired_at = NULL,
  expired_run_id = NULL,
  updated_at = EXCLUDED.updated_at
;

-- Vault mounts and auth-role assets.
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v5),
  assets (asset_kind, external_id, parent_external_id, display_name, status, raw_json) AS (
    VALUES
      ('vault_auth_mount', 'oidc/', '', 'OIDC auth mount', 'oidc', jsonb_build_object('type', 'oidc', 'path', 'oidc/')),
      ('vault_auth_mount', 'approle/', '', 'AppRole auth mount', 'approle', jsonb_build_object('type', 'approle', 'path', 'approle/')),
      ('vault_secrets_mount', 'kv/', '', 'Shared KV mount', 'kv', jsonb_build_object('type', 'kv', 'path', 'kv/')),
      ('vault_secrets_mount', 'payments/', '', 'Payments secrets mount', 'kv', jsonb_build_object('type', 'kv', 'path', 'payments/')),
      ('vault_secrets_mount', 'shared/', '', 'Shared engineering secrets', 'kv', jsonb_build_object('type', 'kv', 'path', 'shared/')),
      ('vault_auth_role', 'role:oidc:oidc:platform-admin', 'oidc/', 'platform-admin', 'oidc', jsonb_build_object('auth_type', 'oidc', 'mount', 'oidc/', 'role', 'platform-admin')),
      ('vault_auth_role', 'role:oidc:oidc:security-auditor', 'oidc/', 'security-auditor', 'oidc', jsonb_build_object('auth_type', 'oidc', 'mount', 'oidc/', 'role', 'security-auditor')),
      ('vault_auth_role', 'role:approle:approle:ci-deployer', 'approle/', 'ci-deployer', 'approle', jsonb_build_object('auth_type', 'approle', 'mount', 'approle/', 'role', 'ci-deployer')),
      ('vault_auth_role', 'role:approle:approle:release-bot', 'approle/', 'release-bot', 'approle', jsonb_build_object('auth_type', 'approle', 'mount', 'approle/', 'role', 'release-bot')),
      ('vault_auth_role', 'role:kubernetes:kubernetes:payments-service', 'kubernetes/', 'payments-service', 'kubernetes', jsonb_build_object('auth_type', 'kubernetes', 'mount', 'kubernetes/', 'role', 'payments-service')),
      ('vault_auth_role', 'role:jwt:jwt:analytics-reader', 'jwt/', 'analytics-reader', 'jwt', jsonb_build_object('auth_type', 'jwt', 'mount', 'jwt/', 'role', 'analytics-reader'))
  )
INSERT INTO app_assets (
  source_kind,
  source_name,
  asset_kind,
  external_id,
  parent_external_id,
  display_name,
  status,
  created_at_source,
  updated_at_source,
  raw_json,
  seen_in_run_id,
  seen_at,
  last_observed_run_id,
  last_observed_at,
  expired_at,
  expired_run_id,
  updated_at
)
SELECT
  'vault',
  ctx.vault_source_name,
  assets.asset_kind,
  assets.external_id,
  assets.parent_external_id,
  assets.display_name,
  assets.status,
  NULL::timestamptz,
  NULL::timestamptz,
  assets.raw_json,
  ctx.vault_run_id,
  ctx.now_ts,
  ctx.vault_run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint,
  ctx.now_ts
FROM assets
CROSS JOIN ctx
ON CONFLICT (source_kind, source_name, asset_kind, external_id) DO UPDATE SET
  parent_external_id = EXCLUDED.parent_external_id,
  display_name = EXCLUDED.display_name,
  status = EXCLUDED.status,
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  last_observed_run_id = EXCLUDED.last_observed_run_id,
  last_observed_at = EXCLUDED.last_observed_at,
  expired_at = NULL,
  expired_run_id = NULL,
  updated_at = EXCLUDED.updated_at
;

-- Governance coverage for Vault app assets.
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v5),
  authoritative_identities AS (
    SELECT DISTINCT ia.identity_id
    FROM identity_accounts ia
    JOIN accounts anchor ON anchor.id = ia.account_id
    JOIN identity_source_settings iss
      ON iss.source_kind = anchor.source_kind
     AND iss.source_name = anchor.source_name
     AND iss.is_authoritative
    WHERE anchor.expired_at IS NULL
      AND anchor.last_observed_run_id IS NOT NULL
  ),
  app_defs (asset_kind, external_id, governance_state, owner_email, notes) AS (
    VALUES
      ('vault_auth_mount', 'oidc/', 'approved', 'demo.user040@example.com', 'OIDC auth mount is approved for the demo baseline.'),
      ('vault_secrets_mount', 'payments/', 'action_required', 'demo.user041@example.com', 'Payments mount needs tighter policy scoping.'),
      ('vault_auth_role', 'role:approle:approle:release-bot', 'in_review', 'demo.user042@example.com', 'Release bot role is under review.'),
      ('vault_auth_role', 'role:kubernetes:kubernetes:payments-service', 'ticketed', 'demo.user043@example.com', 'Tracked in vault-hardening queue.')
  )
INSERT INTO governance_subject_overrides (
  subject_kind,
  subject_id,
  governance_state,
  owner_identity_id,
  business_criticality,
  data_classification,
  ticket_ref,
  notes,
  updated_by_auth_user_id,
  updated_at
)
SELECT
  'app_asset',
  aa.id,
  app_defs.governance_state,
  owner.id,
  CASE
    WHEN app_defs.asset_kind = 'vault_secrets_mount' THEN 'high'
    ELSE 'medium'
  END,
  CASE
    WHEN app_defs.asset_kind = 'vault_secrets_mount' THEN 'restricted'
    ELSE 'internal'
  END,
  CASE
    WHEN app_defs.governance_state = 'ticketed' THEN 'VAULT-302'
    ELSE ''
  END,
  app_defs.notes,
  NULL::bigint,
  ctx.now_ts
FROM app_defs
CROSS JOIN ctx
JOIN app_assets aa
  ON aa.source_kind = 'vault'
  AND aa.source_name = ctx.vault_source_name
  AND aa.asset_kind = app_defs.asset_kind
  AND aa.external_id = app_defs.external_id
LEFT JOIN LATERAL (
  SELECT i.id
  FROM identities i
  LEFT JOIN authoritative_identities ai ON ai.identity_id = i.id
  WHERE lower(i.primary_email) = lower(app_defs.owner_email)
  ORDER BY (ai.identity_id IS NOT NULL) DESC, i.id ASC
  LIMIT 1
) owner ON TRUE
ON CONFLICT (subject_kind, subject_id) DO UPDATE SET
  governance_state = EXCLUDED.governance_state,
  owner_identity_id = EXCLUDED.owner_identity_id,
  business_criticality = EXCLUDED.business_criticality,
  data_classification = EXCLUDED.data_classification,
  ticket_ref = EXCLUDED.ticket_ref,
  notes = EXCLUDED.notes,
  updated_by_auth_user_id = EXCLUDED.updated_by_auth_user_id,
  updated_at = EXCLUDED.updated_at
;

COMMIT;

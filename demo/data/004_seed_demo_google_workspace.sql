-- Demo breadth seed: Google Workspace users, groups, and OAuth inventory.

BEGIN;

CREATE TEMP TABLE demo_seed_ctx_v4 (
  now_ts TIMESTAMPTZ NOT NULL,
  google_customer_id TEXT NOT NULL,
  google_primary_domain TEXT NOT NULL,
  google_run_id BIGINT NOT NULL
) ON COMMIT DROP;

TRUNCATE demo_seed_ctx_v4;

WITH
  existing AS (
    SELECT id
    FROM sync_runs
    WHERE source_kind = 'google_workspace'
      AND source_name = 'C0123'
      AND status = 'success'
    ORDER BY finished_at DESC NULLS LAST, id DESC
    LIMIT 1
  ),
  ins AS (
    INSERT INTO sync_runs (source_kind, source_name, status, started_at, finished_at, message, stats, error_kind)
    SELECT 'google_workspace', 'C0123', 'success', now() - interval '9 minutes', now() - interval '4 minutes', 'seeded fallback Google Workspace sync', '{}'::jsonb, ''
    WHERE NOT EXISTS (SELECT 1 FROM existing)
    RETURNING id
  )
INSERT INTO demo_seed_ctx_v4 (now_ts, google_customer_id, google_primary_domain, google_run_id)
SELECT
  now(),
  'C0123',
  'workspace.demo.example.com',
  COALESCE((SELECT id FROM existing), (SELECT id FROM ins))
;

-- Google Workspace users.
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v4),
  users AS (
    SELECT
      i,
      format('gw-user-%s', to_char(i, 'FM000')) AS external_id,
      CASE
        WHEN i <= 60 THEN format('demo.user%s@example.com', to_char(i, 'FM000'))
        ELSE format('workspace.contractor%s@workspace.demo.example.com', to_char(i, 'FM000'))
      END AS email,
      format('Workspace User %s', to_char(i, 'FM000')) AS display_name,
      CASE
        WHEN (i % 17) = 0 THEN 'suspended'
        ELSE 'active'
      END AS status
    FROM generate_series(1, 80) AS s(i)
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
  'google_workspace',
  ctx.google_customer_id,
  users.external_id,
  lower(users.email),
  users.display_name,
  users.status,
  'human',
  'user',
  jsonb_build_object(
    'id', users.external_id,
    'primaryEmail', users.email,
    'name', jsonb_build_object('fullName', users.display_name),
    'suspended', users.status <> 'active',
    'entity_category', 'user'
  ),
  NULL::timestamptz,
  ''::text,
  ''::text,
  ctx.google_run_id,
  ctx.now_ts,
  ctx.google_run_id,
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

-- Google Workspace groups.
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v4),
  groups (ord, external_id, email, display_name) AS (
    VALUES
      (1, 'gw-group-eng', 'eng@workspace.demo.example.com', 'Workspace Engineering'),
      (2, 'gw-group-security', 'security@workspace.demo.example.com', 'Workspace Security'),
      (3, 'gw-group-finance', 'finance@workspace.demo.example.com', 'Workspace Finance'),
      (4, 'gw-group-it', 'it@workspace.demo.example.com', 'Workspace IT Admins'),
      (5, 'gw-group-sales', 'sales@workspace.demo.example.com', 'Workspace Sales'),
      (6, 'gw-group-support', 'support@workspace.demo.example.com', 'Workspace Support'),
      (7, 'gw-group-people', 'people@workspace.demo.example.com', 'Workspace People Ops'),
      (8, 'gw-group-platform', 'platform@workspace.demo.example.com', 'Workspace Platform'),
      (9, 'gw-group-data', 'data@workspace.demo.example.com', 'Workspace Data'),
      (10, 'gw-group-contractors', 'contractors@workspace.demo.example.com', 'Workspace Contractors')
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
  'google_workspace',
  ctx.google_customer_id,
  groups.external_id,
  lower(groups.email),
  groups.display_name,
  'active',
  'unknown',
  'group',
  jsonb_build_object(
    'id', groups.external_id,
    'email', groups.email,
    'name', groups.display_name,
    'entity_category', 'group'
  ),
  NULL::timestamptz,
  ''::text,
  ''::text,
  ctx.google_run_id,
  ctx.now_ts,
  ctx.google_run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint,
  ctx.now_ts
FROM groups
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

-- About 160 group membership entitlements.
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v4),
  google_users AS (
    SELECT
      a.id,
      regexp_replace(a.external_id, '^gw-user-', '')::int AS user_ord
    FROM accounts a
    WHERE a.source_kind = 'google_workspace'
      AND a.source_name = ctx.google_customer_id
      AND a.external_id LIKE 'gw-user-%'
      AND a.expired_at IS NULL
  ),
  group_refs AS (
    SELECT
      external_id,
      row_number() OVER (ORDER BY external_id) AS group_ord
    FROM accounts
    WHERE source_kind = 'google_workspace'
      AND source_name = (SELECT google_customer_id FROM ctx)
      AND entity_category = 'group'
      AND expired_at IS NULL
  ),
  ents AS (
    SELECT
      google_users.id AS app_user_id,
      'google_group_member'::text AS kind,
      format('google_group:%s', primary_group.external_id) AS resource,
      'member'::text AS permission,
      jsonb_build_object('group_id', primary_group.external_id, 'membership', 'primary') AS raw_json
    FROM google_users
    JOIN group_refs AS primary_group
      ON primary_group.group_ord = ((google_users.user_ord - 1) % 10) + 1

    UNION ALL

    SELECT
      google_users.id,
      'google_group_member',
      format('google_group:%s', secondary_group.external_id),
      CASE
        WHEN (google_users.user_ord % 20) = 0 THEN 'owner'
        WHEN (google_users.user_ord % 9) = 0 THEN 'manager'
        ELSE 'member'
      END,
      jsonb_build_object('group_id', secondary_group.external_id, 'membership', 'secondary')
    FROM google_users
    JOIN group_refs AS secondary_group
      ON secondary_group.group_ord = ((google_users.user_ord + 2) % 10) + 1
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
  ents.app_user_id,
  ents.kind,
  ents.resource,
  ents.permission,
  ents.raw_json,
  ctx.google_run_id,
  ctx.now_ts,
  ctx.google_run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint,
  ctx.now_ts
FROM ents
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

-- About 18 Google admin role assignments.
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v4),
  google_users AS (
    SELECT
      a.id,
      a.external_id,
      regexp_replace(a.external_id, '^gw-user-', '')::int AS user_ord
    FROM accounts a
    WHERE a.source_kind = 'google_workspace'
      AND a.source_name = ctx.google_customer_id
      AND a.external_id LIKE 'gw-user-%'
      AND regexp_replace(a.external_id, '^gw-user-', '')::int <= 18
      AND a.expired_at IS NULL
  ),
  roles (role_ord, role_id, role_name) AS (
    VALUES
      (1, 'role-user-management', 'User Management'),
      (2, 'role-groups-admin', 'Groups Admin'),
      (3, 'role-security-admin', 'Security Admin'),
      (4, 'role-helpdesk-admin', 'Help Desk Admin'),
      (5, 'role-apps-admin', 'Apps Admin'),
      (6, 'role-device-admin', 'Device Admin')
  ),
  ents AS (
    SELECT
      google_users.id AS app_user_id,
      'google_admin_role'::text AS kind,
      format('google_admin_role:%s', roles.role_id) AS resource,
      CASE
        WHEN (google_users.user_ord % 4) = 0 THEN 'delegated'
        ELSE 'global'
      END AS permission,
      jsonb_build_object(
        'role_id', roles.role_id,
        'role_name', roles.role_name,
        'assigned_to', google_users.external_id
      ) AS raw_json
    FROM google_users
    JOIN roles
      ON roles.role_ord = ((google_users.user_ord - 1) % 6) + 1
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
  ents.app_user_id,
  ents.kind,
  ents.resource,
  ents.permission,
  ents.raw_json,
  ctx.google_run_id,
  ctx.now_ts,
  ctx.google_run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint,
  ctx.now_ts
FROM ents
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

-- Link Google Workspace users to existing demo identities by email.
WITH
  google_accounts AS (
    SELECT
      a.id AS account_id,
      lower(a.email) AS email
    FROM accounts a
    WHERE a.source_kind = 'google_workspace'
      AND a.source_name = (SELECT google_customer_id FROM demo_seed_ctx_v4)
      AND a.entity_category = 'user'
      AND a.email <> ''
      AND a.expired_at IS NULL
  ),
  matches AS (
    SELECT
      google_accounts.account_id,
      identities.id AS identity_id
    FROM google_accounts
    JOIN identities
      ON lower(identities.primary_email) = google_accounts.email
  )
INSERT INTO identity_accounts (identity_id, account_id, link_reason, confidence, updated_at)
SELECT
  matches.identity_id,
  matches.account_id,
  'demo_google_workspace_email',
  1.0,
  now()
FROM matches
ON CONFLICT (account_id) DO UPDATE SET
  identity_id = EXCLUDED.identity_id,
  link_reason = EXCLUDED.link_reason,
  confidence = EXCLUDED.confidence,
  updated_at = EXCLUDED.updated_at
;

-- Google OAuth clients, aligned with discovery evidence for a subset of clients.
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v4),
  oauth_apps (
    app_ord,
    external_id,
    display_name,
    primary_domain,
    status
  ) AS (
    VALUES
      ( 1, 'gw-hotspot-finance-sync.apps.googleusercontent.com', 'Finance Sync Audit Bot', 'finance-sync.demo.example.com', 'active'),
      ( 2, 'gw-hotspot-drive-mirror.apps.googleusercontent.com', 'Drive Mirror Exporter', 'drive-mirror.demo.example.com', 'active'),
      ( 3, 'gw-hotspot-hr-ops.apps.googleusercontent.com', 'HR Ops Directory Sync', 'hr-ops.demo.example.com', 'active'),
      ( 4, 'gw-hotspot-mail-ops.apps.googleusercontent.com', 'Mail Ops Automation', 'mail-ops.demo.example.com', 'active'),
      ( 5, 'gw-client-analytics-studio.apps.googleusercontent.com', 'Analytics Studio', 'analytics.demo.example.com', 'active'),
      ( 6, 'gw-client-ops-dashboard.apps.googleusercontent.com', 'Ops Dashboard', 'ops-dashboard.demo.example.com', 'active'),
      ( 7, 'gw-client-security-review.apps.googleusercontent.com', 'Security Review Workspace', 'security-review.demo.example.com', 'active'),
      ( 8, 'gw-client-sales-enablement.apps.googleusercontent.com', 'Sales Enablement', 'sales.demo.example.com', 'active'),
      ( 9, 'gw-client-marketing-automation.apps.googleusercontent.com', 'Marketing Automation', 'marketing.demo.example.com', 'active'),
      (10, 'gw-client-laptop-lifecycle.apps.googleusercontent.com', 'Laptop Lifecycle', 'lifecycle.demo.example.com', 'active'),
      (11, 'gw-client-employee-onboarding.apps.googleusercontent.com', 'Employee Onboarding', 'onboarding.demo.example.com', 'active'),
      (12, 'gw-client-vendor-access.apps.googleusercontent.com', 'Vendor Access Gateway', 'vendor.demo.example.com', 'inactive'),
      (13, 'gw-client-incident-desk.apps.googleusercontent.com', 'Incident Desk', 'incident-desk.demo.example.com', 'active'),
      (14, 'gw-client-release-tracker.apps.googleusercontent.com', 'Release Tracker', 'release-tracker.demo.example.com', 'active'),
      (15, 'gw-client-shared-calendar.apps.googleusercontent.com', 'Shared Calendar Broker', 'calendar.demo.example.com', 'active'),
      (16, 'gw-client-knowledge-base.apps.googleusercontent.com', 'Knowledge Base Sync', 'kb.demo.example.com', 'active'),
      (17, 'gw-client-field-ops.apps.googleusercontent.com', 'Field Ops Workspace', 'field-ops.demo.example.com', 'active'),
      (18, 'gw-client-customer-success.apps.googleusercontent.com', 'Customer Success Hub', 'success.demo.example.com', 'active')
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
  'google_workspace',
  ctx.google_customer_id,
  'google_oauth_client',
  oauth_apps.external_id,
  '',
  oauth_apps.display_name,
  oauth_apps.status,
  ctx.now_ts - make_interval(days => 120 + oauth_apps.app_ord),
  ctx.now_ts - make_interval(days => oauth_apps.app_ord % 15),
  jsonb_build_object(
    'client_id', oauth_apps.external_id,
    'name', oauth_apps.display_name,
    'publisher_domain', oauth_apps.primary_domain,
    'verified', oauth_apps.app_ord % 3 <> 0
  ),
  ctx.google_run_id,
  ctx.now_ts,
  ctx.google_run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint,
  ctx.now_ts
FROM oauth_apps
CROSS JOIN ctx
ON CONFLICT (source_kind, source_name, asset_kind, external_id) DO UPDATE SET
  parent_external_id = EXCLUDED.parent_external_id,
  display_name = EXCLUDED.display_name,
  status = EXCLUDED.status,
  created_at_source = EXCLUDED.created_at_source,
  updated_at_source = EXCLUDED.updated_at_source,
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  last_observed_run_id = EXCLUDED.last_observed_run_id,
  last_observed_at = EXCLUDED.last_observed_at,
  expired_at = NULL,
  expired_run_id = NULL,
  updated_at = EXCLUDED.updated_at
;

-- OAuth owners.
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v4),
  oauth_apps AS (
    SELECT
      id AS app_asset_id,
      external_id,
      row_number() OVER (ORDER BY external_id) AS app_ord
    FROM app_assets
    WHERE source_kind = 'google_workspace'
      AND source_name = ctx.google_customer_id
      AND asset_kind = 'google_oauth_client'
      AND expired_at IS NULL
  ),
  owners AS (
    SELECT
      oauth_apps.app_asset_id,
      format('gw-user-%s', to_char(((oauth_apps.app_ord - 1) % 18) + 1, 'FM000')) AS owner_external_id,
      format('Workspace User %s', to_char(((oauth_apps.app_ord - 1) % 18) + 1, 'FM000')) AS owner_display_name,
      format('demo.user%s@example.com', to_char(((oauth_apps.app_ord - 1) % 18) + 1, 'FM000')) AS owner_email
    FROM oauth_apps
  )
INSERT INTO app_asset_owners (
  app_asset_id,
  owner_kind,
  owner_external_id,
  owner_display_name,
  owner_email,
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
  owners.app_asset_id,
  'google_workspace_user',
  owners.owner_external_id,
  owners.owner_display_name,
  owners.owner_email,
  jsonb_build_object('source', 'demo_google_workspace_owner'),
  ctx.google_run_id,
  ctx.now_ts,
  ctx.google_run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint,
  ctx.now_ts
FROM owners
CROSS JOIN ctx
ON CONFLICT (app_asset_id, owner_kind, owner_external_id) DO UPDATE SET
  owner_display_name = EXCLUDED.owner_display_name,
  owner_email = EXCLUDED.owner_email,
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  last_observed_run_id = EXCLUDED.last_observed_run_id,
  last_observed_at = EXCLUDED.last_observed_at,
  expired_at = NULL,
  expired_run_id = NULL,
  updated_at = EXCLUDED.updated_at
;

-- About 110 Google OAuth grants.
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v4),
  oauth_apps AS (
    SELECT
      external_id,
      display_name,
      row_number() OVER (ORDER BY external_id) AS app_ord
    FROM app_assets
    WHERE source_kind = 'google_workspace'
      AND source_name = ctx.google_customer_id
      AND asset_kind = 'google_oauth_client'
      AND expired_at IS NULL
  ),
  google_users AS (
    SELECT
      external_id,
      email,
      display_name,
      row_number() OVER (ORDER BY external_id) AS user_ord
    FROM accounts
    WHERE source_kind = 'google_workspace'
      AND source_name = (SELECT google_customer_id FROM ctx)
      AND entity_category = 'user'
      AND expired_at IS NULL
  ),
  grants AS (
    SELECT
      oauth_apps.external_id AS client_external_id,
      oauth_apps.display_name AS client_display_name,
      oauth_apps.app_ord,
      grant_ord,
      google_users.external_id AS actor_external_id,
      google_users.email AS actor_email,
      google_users.display_name AS actor_display_name
    FROM oauth_apps
    CROSS JOIN generate_series(1, 6) AS grant_ord
    JOIN google_users
      ON google_users.user_ord = ((oauth_apps.app_ord * 7 + grant_ord * 3 - 1) % 80) + 1
  )
INSERT INTO credential_artifacts (
  source_kind,
  source_name,
  asset_ref_kind,
  asset_ref_external_id,
  credential_kind,
  external_id,
  display_name,
  fingerprint,
  scope_json,
  status,
  created_at_source,
  expires_at_source,
  last_used_at_source,
  created_by_kind,
  created_by_external_id,
  created_by_display_name,
  approved_by_kind,
  approved_by_external_id,
  approved_by_display_name,
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
  'google_workspace',
  ctx.google_customer_id,
  'google_oauth_client',
  format('google_oauth_client:%s', grants.client_external_id),
  'google_oauth_grant',
  format('gw-grant-%s-%s', lpad(grants.app_ord::text, 2, '0'), lpad(grants.grant_ord::text, 2, '0')),
  format('%s grant %s', grants.client_display_name, grants.grant_ord),
  substr(md5(grants.client_external_id || ':' || grants.grant_ord), 1, 32),
  CASE
    WHEN grants.app_ord <= 4 THEN jsonb_build_array('directory.readwrite.all', 'offline_access', 'mail.read')
    WHEN (grants.app_ord % 3) = 0 THEN jsonb_build_array('drive.file', 'calendar.readonly')
    ELSE jsonb_build_array('mail.read', 'profile')
  END,
  CASE
    WHEN grants.grant_ord = 6 AND (grants.app_ord % 4) = 0 THEN 'revoked'
    WHEN grants.grant_ord = 5 AND (grants.app_ord % 5) = 0 THEN 'expired'
    ELSE 'active'
  END,
  ctx.now_ts - make_interval(days => grants.app_ord * 4 + grants.grant_ord),
  CASE
    WHEN grants.grant_ord = 6 AND (grants.app_ord % 4) = 0 THEN ctx.now_ts - make_interval(days => grants.grant_ord)
    WHEN grants.grant_ord = 5 AND (grants.app_ord % 5) = 0 THEN ctx.now_ts - make_interval(days => 1)
    ELSE ctx.now_ts + make_interval(days => grants.app_ord + grants.grant_ord + 30)
  END,
  ctx.now_ts - make_interval(days => (grants.app_ord + grants.grant_ord) % 20),
  'google_workspace_user',
  grants.actor_external_id,
  grants.actor_display_name,
  CASE WHEN (grants.grant_ord % 4) = 0 THEN '' ELSE 'google_workspace_user' END,
  CASE WHEN (grants.grant_ord % 4) = 0 THEN '' ELSE format('gw-user-%s', to_char(((grants.app_ord + grants.grant_ord + 5 - 1) % 18) + 1, 'FM000')) END,
  CASE WHEN (grants.grant_ord % 4) = 0 THEN '' ELSE format('Workspace User %s', to_char(((grants.app_ord + grants.grant_ord + 5 - 1) % 18) + 1, 'FM000')) END,
  jsonb_build_object(
    'client_id', grants.client_external_id,
    'actor_email', grants.actor_email
  ),
  ctx.google_run_id,
  ctx.now_ts,
  ctx.google_run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint,
  ctx.now_ts
FROM grants
CROSS JOIN ctx
ON CONFLICT (source_kind, source_name, credential_kind, external_id, asset_ref_kind, asset_ref_external_id) DO UPDATE SET
  display_name = EXCLUDED.display_name,
  fingerprint = EXCLUDED.fingerprint,
  scope_json = EXCLUDED.scope_json,
  status = EXCLUDED.status,
  created_at_source = EXCLUDED.created_at_source,
  expires_at_source = EXCLUDED.expires_at_source,
  last_used_at_source = EXCLUDED.last_used_at_source,
  created_by_kind = EXCLUDED.created_by_kind,
  created_by_external_id = EXCLUDED.created_by_external_id,
  created_by_display_name = EXCLUDED.created_by_display_name,
  approved_by_kind = EXCLUDED.approved_by_kind,
  approved_by_external_id = EXCLUDED.approved_by_external_id,
  approved_by_display_name = EXCLUDED.approved_by_display_name,
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  last_observed_run_id = EXCLUDED.last_observed_run_id,
  last_observed_at = EXCLUDED.last_observed_at,
  expired_at = NULL,
  expired_run_id = NULL,
  updated_at = EXCLUDED.updated_at
;

-- OAuth audit events.
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v4),
  seeded_credentials AS (
    SELECT
      credential_artifacts.asset_ref_external_id,
      credential_artifacts.credential_kind,
      credential_artifacts.external_id,
      credential_artifacts.display_name,
      credential_artifacts.status,
      credential_artifacts.created_at_source,
      credential_artifacts.expires_at_source,
      credential_artifacts.last_used_at_source,
      credential_artifacts.created_by_kind,
      credential_artifacts.created_by_external_id,
      credential_artifacts.created_by_display_name,
      credential_artifacts.approved_by_kind,
      credential_artifacts.approved_by_external_id,
      credential_artifacts.approved_by_display_name,
      split_part(credential_artifacts.asset_ref_external_id, ':', 2) AS target_external_id
    FROM credential_artifacts
    WHERE credential_artifacts.source_kind = 'google_workspace'
      AND credential_artifacts.source_name = (SELECT google_customer_id FROM ctx)
      AND credential_artifacts.credential_kind = 'google_oauth_grant'
  ),
  targets AS (
    SELECT
      seeded_credentials.*,
      COALESCE(app_assets.display_name, seeded_credentials.target_external_id) AS target_display_name
    FROM seeded_credentials
    LEFT JOIN app_assets
      ON app_assets.source_kind = 'google_workspace'
      AND app_assets.source_name = (SELECT google_customer_id FROM ctx)
      AND app_assets.asset_kind = 'google_oauth_client'
      AND app_assets.external_id = seeded_credentials.target_external_id
      AND app_assets.expired_at IS NULL
  ),
  events AS (
    SELECT
      format('gw:create:%s', targets.external_id) AS event_external_id,
      'credential.created'::text AS event_type,
      COALESCE(targets.created_at_source, ctx.now_ts - interval '30 days') AS event_time,
      COALESCE(NULLIF(targets.created_by_kind, ''), 'system') AS actor_kind,
      COALESCE(NULLIF(targets.created_by_external_id, ''), 'seed-bot') AS actor_external_id,
      COALESCE(NULLIF(targets.created_by_display_name, ''), 'Demo Seed Bot') AS actor_display_name,
      'google_oauth_client'::text AS target_kind,
      targets.target_external_id,
      targets.target_display_name,
      targets.credential_kind,
      targets.external_id AS credential_external_id,
      jsonb_build_object('dataset', 'demo_google_workspace', 'phase', 'created') AS raw_json
    FROM targets
    CROSS JOIN ctx

    UNION ALL

    SELECT
      format('gw:state:%s', targets.external_id),
      CASE
        WHEN lower(targets.status) = 'revoked' THEN 'credential.revoked'
        WHEN lower(targets.status) = 'expired' THEN 'credential.expired'
        ELSE 'credential.used'
      END,
      COALESCE(targets.last_used_at_source, targets.expires_at_source, ctx.now_ts - interval '5 days'),
      COALESCE(NULLIF(targets.created_by_kind, ''), 'system'),
      COALESCE(NULLIF(targets.created_by_external_id, ''), 'seed-bot'),
      COALESCE(NULLIF(targets.created_by_display_name, ''), 'Demo Seed Bot'),
      'google_oauth_client',
      targets.target_external_id,
      targets.target_display_name,
      targets.credential_kind,
      targets.external_id,
      jsonb_build_object('dataset', 'demo_google_workspace', 'phase', 'state', 'status', targets.status)
    FROM targets
    CROSS JOIN ctx
    WHERE lower(targets.status) IN ('revoked', 'expired')
       OR targets.last_used_at_source IS NOT NULL
  )
INSERT INTO credential_audit_events (
  source_kind,
  source_name,
  event_external_id,
  event_type,
  event_time,
  actor_kind,
  actor_external_id,
  actor_display_name,
  target_kind,
  target_external_id,
  target_display_name,
  credential_kind,
  credential_external_id,
  raw_json
)
SELECT
  'google_workspace',
  ctx.google_customer_id,
  events.event_external_id,
  events.event_type,
  events.event_time,
  events.actor_kind,
  events.actor_external_id,
  events.actor_display_name,
  events.target_kind,
  events.target_external_id,
  events.target_display_name,
  events.credential_kind,
  events.credential_external_id,
  events.raw_json
FROM events
CROSS JOIN ctx
ON CONFLICT (source_kind, source_name, event_external_id) DO UPDATE SET
  event_type = EXCLUDED.event_type,
  event_time = EXCLUDED.event_time,
  actor_kind = EXCLUDED.actor_kind,
  actor_external_id = EXCLUDED.actor_external_id,
  actor_display_name = EXCLUDED.actor_display_name,
  target_kind = EXCLUDED.target_kind,
  target_external_id = EXCLUDED.target_external_id,
  target_display_name = EXCLUDED.target_display_name,
  credential_kind = EXCLUDED.credential_kind,
  credential_external_id = EXCLUDED.credential_external_id,
  raw_json = EXCLUDED.raw_json
;

-- Governance coverage for Google OAuth clients.
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v4),
  app_defs (
    external_id,
    governance_state,
    owner_email,
    ticket_ref,
    notes
  ) AS (
    VALUES
      ('gw-hotspot-finance-sync.apps.googleusercontent.com', 'action_required', '', '', 'Privileged finance sync client requires immediate review.'),
      ('gw-hotspot-drive-mirror.apps.googleusercontent.com', 'ticketed', '', 'SEC-241', 'Security ticket tracks broader remediation.'),
      ('gw-hotspot-hr-ops.apps.googleusercontent.com', 'in_review', 'demo.user003@example.com', '', 'Owner is reviewing least-privilege scopes.'),
      ('gw-hotspot-mail-ops.apps.googleusercontent.com', 'approved', 'demo.user004@example.com', '', 'Approved after documented compensating controls.'),
      ('gw-client-analytics-studio.apps.googleusercontent.com', 'action_required', 'demo.user005@example.com', '', 'Pending reapproval because grants expanded.')
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
    WHEN app_defs.external_id LIKE 'gw-hotspot-%' THEN 'high'
    ELSE 'medium'
  END,
  CASE
    WHEN app_defs.external_id LIKE 'gw-hotspot-%' THEN 'restricted'
    ELSE 'confidential'
  END,
  app_defs.ticket_ref,
  app_defs.notes,
  NULL::bigint,
  ctx.now_ts
FROM app_defs
JOIN app_assets aa
  ON aa.source_kind = 'google_workspace'
  AND aa.source_name = ctx.google_customer_id
  AND aa.asset_kind = 'google_oauth_client'
  AND aa.external_id = app_defs.external_id
LEFT JOIN identities owner
  ON lower(owner.primary_email) = lower(app_defs.owner_email)
CROSS JOIN ctx
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

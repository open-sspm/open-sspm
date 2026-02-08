-- Demo seed data for Open-SSPM (upsert-only).
-- Safe to re-run: uses ON CONFLICT upserts and does not delete data.
--
-- Assumes schema migrations have been applied. Findings seeding will only
-- populate results if rulesets/rules exist (e.g. after `open-sspm seed-rules`).

BEGIN;

-- Context (single run_id reused across statements).
CREATE TEMP TABLE demo_seed_ctx (
  run_id BIGINT NOT NULL,
  okta_domain TEXT NOT NULL,
  github_org TEXT NOT NULL,
  datadog_site TEXT NOT NULL,
  now_ts TIMESTAMPTZ NOT NULL
) ON COMMIT DROP;

TRUNCATE demo_seed_ctx;

WITH
  existing AS (
    SELECT id
    FROM sync_runs
    WHERE source_kind = 'demo_seed' AND source_name = 'demo'
    ORDER BY id DESC
    LIMIT 1
  ),
  ins AS (
    INSERT INTO sync_runs (source_kind, source_name, status, started_at, finished_at, message, stats, error_kind)
    SELECT 'demo_seed', 'demo', 'success', now(), now(), 'seeded demo data', '{}'::jsonb, ''
    WHERE NOT EXISTS (SELECT 1 FROM existing)
    RETURNING id
  ),
  run AS (
    SELECT COALESCE((SELECT id FROM ins), (SELECT id FROM existing)) AS id
  )
INSERT INTO demo_seed_ctx (run_id, okta_domain, github_org, datadog_site, now_ts)
SELECT
  (SELECT id FROM run) AS run_id,
  'demo.okta.example.com' AS okta_domain,
  'open-sspm-demo' AS github_org,
  'datadoghq.com' AS datadog_site,
  now() AS now_ts;

UPDATE sync_runs
SET
  status = 'success',
  started_at = (SELECT now_ts FROM demo_seed_ctx),
  finished_at = (SELECT now_ts FROM demo_seed_ctx),
  message = 'seeded demo data',
  stats = '{}'::jsonb,
  error_kind = ''
WHERE id = (SELECT run_id FROM demo_seed_ctx);

-- ------------------------------------------------------------
-- Connector configs (mark Okta/GitHub/Datadog as enabled+configured with fake secrets).
-- ------------------------------------------------------------
WITH ctx AS (SELECT * FROM demo_seed_ctx)
INSERT INTO connector_configs (kind, enabled, config, updated_at)
VALUES
  ('okta', true, jsonb_build_object(
    'domain', (SELECT okta_domain FROM ctx),
    'token', 'demo_okta_token'
  ), (SELECT now_ts FROM ctx)),
  ('github', true, jsonb_build_object(
    'org', (SELECT github_org FROM ctx),
    'api_base', 'https://api.github.com',
    'enterprise', '',
    'scim_enabled', false,
    'token', 'demo_github_token'
  ), (SELECT now_ts FROM ctx)),
  ('datadog', true, jsonb_build_object(
    'site', (SELECT datadog_site FROM ctx),
    'api_key', 'demo_datadog_api_key',
    'app_key', 'demo_datadog_app_key'
  ), (SELECT now_ts FROM ctx))
ON CONFLICT (kind) DO UPDATE SET
  enabled = EXCLUDED.enabled,
  config = EXCLUDED.config,
  updated_at = EXCLUDED.updated_at
;

-- ------------------------------------------------------------
-- Okta users (IdP)
-- ------------------------------------------------------------
WITH
  ctx AS (SELECT * FROM demo_seed_ctx),
  first_names AS (
    SELECT ARRAY[
      'Alex','Sam','Jordan','Taylor','Morgan','Casey','Riley','Jamie','Avery','Quinn',
      'Cameron','Skyler','Parker','Emerson','Rowan','Reese','Hayden','Finley','Dakota','Robin'
    ]::text[] AS v
  ),
  last_names AS (
    SELECT ARRAY[
      'Lee','Kim','Patel','Nguyen','Garcia','Brown','Johnson','Martinez','Davis','Wilson',
      'Anderson','Thomas','Moore','Jackson','Martin','Thompson','White','Lopez','Clark','Lewis'
    ]::text[] AS v
  ),
  users AS (
    SELECT
      i,
      format('00u_demo_%s', to_char(i, 'FM000')) AS external_id,
      format('demo.user%s@example.com', to_char(i, 'FM000')) AS email,
      CASE WHEN (i % 7) = 0 THEN 'SUSPENDED' ELSE 'ACTIVE' END AS status
    FROM generate_series(1, 25) AS s(i)
  ),
  computed AS (
    SELECT
      u.external_id,
      u.email,
      format(
        '%s %s',
        (SELECT v[((u.i - 1) % array_length(v, 1)) + 1] FROM first_names),
        (SELECT v[((u.i - 1) % array_length(v, 1)) + 1] FROM last_names)
      ) AS display_name,
      u.status
    FROM users u
  )
INSERT INTO accounts (
  source_kind,
  source_name,
  external_id,
  email,
  display_name,
  status,
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
  'okta',
  ctx.okta_domain,
  c.external_id,
  lower(c.email),
  c.display_name,
  c.status,
  jsonb_build_object(
    'id', c.external_id,
    'status', c.status,
    'profile', jsonb_build_object(
      'email', c.email,
      'displayName', c.display_name
    )
  ) AS raw_json,
  NULL::timestamptz,
  ''::text,
  ''::text,
  ctx.run_id,
  ctx.now_ts,
  ctx.run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint,
  ctx.now_ts
FROM computed c
CROSS JOIN ctx
ON CONFLICT (source_kind, source_name, external_id) DO UPDATE SET
  email = EXCLUDED.email,
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

-- ------------------------------------------------------------
-- Okta groups
-- ------------------------------------------------------------
WITH
  ctx AS (SELECT * FROM demo_seed_ctx),
  groups(external_id, name, type) AS (
    VALUES
      ('00g_demo_eng', 'Engineering', 'OKTA_GROUP'),
      ('00g_demo_security', 'Security', 'OKTA_GROUP'),
      ('00g_demo_finance', 'Finance', 'OKTA_GROUP'),
      ('00g_demo_contractors', 'Contractors', 'OKTA_GROUP'),
      ('00g_demo_it_admins', 'IT Admins', 'OKTA_GROUP')
  )
INSERT INTO okta_groups (
  external_id,
  name,
  type,
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
  g.external_id,
  g.name,
  g.type,
  jsonb_build_object(
    'id', g.external_id,
    'type', g.type,
    'profile', jsonb_build_object('name', g.name)
  ) AS raw_json,
  ctx.run_id,
  ctx.now_ts,
  ctx.run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint,
  ctx.now_ts
FROM groups g
CROSS JOIN ctx
ON CONFLICT (external_id) DO UPDATE SET
  name = EXCLUDED.name,
  type = EXCLUDED.type,
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  last_observed_run_id = EXCLUDED.last_observed_run_id,
  last_observed_at = EXCLUDED.last_observed_at,
  expired_at = NULL,
  expired_run_id = NULL,
  updated_at = EXCLUDED.updated_at
;

-- ------------------------------------------------------------
-- Okta apps
-- ------------------------------------------------------------
WITH
  ctx AS (SELECT * FROM demo_seed_ctx),
  apps(external_id, label, name, status, sign_on_mode) AS (
    VALUES
      ('0oa_demo_github', 'GitHub', 'github', 'ACTIVE', 'SAML_2_0'),
      ('0oa_demo_datadog', 'Datadog', 'datadog', 'ACTIVE', 'SAML_2_0'),
      ('0oa_demo_slack', 'Slack', 'slack', 'ACTIVE', 'SAML_2_0'),
      ('0oa_demo_jira', 'Jira', 'jira', 'ACTIVE', 'SAML_2_0'),
      ('0oa_demo_confluence', 'Confluence', 'confluence', 'ACTIVE', 'SAML_2_0'),
      ('0oa_demo_google', 'Google Workspace', 'google', 'ACTIVE', 'SAML_2_0')
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
  last_observed_run_id,
  last_observed_at,
  expired_at,
  expired_run_id,
  updated_at
)
SELECT
  a.external_id,
  a.label,
  a.name,
  a.status,
  a.sign_on_mode,
  jsonb_build_object(
    'id', a.external_id,
    'label', a.label,
    'name', a.name,
    'status', a.status,
    'signOnMode', a.sign_on_mode
  ) AS raw_json,
  ctx.run_id,
  ctx.now_ts,
  ctx.run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint,
  ctx.now_ts
FROM apps a
CROSS JOIN ctx
ON CONFLICT (external_id) DO UPDATE SET
  label = EXCLUDED.label,
  name = EXCLUDED.name,
  status = EXCLUDED.status,
  sign_on_mode = EXCLUDED.sign_on_mode,
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  last_observed_run_id = EXCLUDED.last_observed_run_id,
  last_observed_at = EXCLUDED.last_observed_at,
  expired_at = NULL,
  expired_run_id = NULL,
  updated_at = EXCLUDED.updated_at
;

-- Map Okta apps to supported integrations (so /apps shows them as "integrated").
WITH ctx AS (SELECT * FROM demo_seed_ctx)
INSERT INTO integration_okta_app_map (integration_kind, okta_app_external_id, updated_at)
  VALUES
    ('github', '0oa_demo_github', (SELECT now_ts FROM ctx)),
    ('datadog', '0oa_demo_datadog', (SELECT now_ts FROM ctx))
ON CONFLICT (integration_kind) DO UPDATE SET
  okta_app_external_id = EXCLUDED.okta_app_external_id,
  updated_at = EXCLUDED.updated_at
;

-- Identity source settings (Okta authoritative; app sources non-authoritative).
WITH ctx AS (SELECT * FROM demo_seed_ctx)
INSERT INTO identity_source_settings (source_kind, source_name, is_authoritative, updated_at)
VALUES
  ('okta', (SELECT okta_domain FROM ctx), true, (SELECT now_ts FROM ctx)),
  ('github', (SELECT github_org FROM ctx), false, (SELECT now_ts FROM ctx)),
  ('datadog', (SELECT datadog_site FROM ctx), false, (SELECT now_ts FROM ctx))
ON CONFLICT (source_kind, source_name) DO UPDATE SET
  is_authoritative = EXCLUDED.is_authoritative,
  updated_at = EXCLUDED.updated_at
;

-- ------------------------------------------------------------
-- Okta user ↔ group memberships
-- ------------------------------------------------------------
WITH
  ctx AS (SELECT * FROM demo_seed_ctx),
  u AS (
    SELECT id, right(external_id, 3)::int AS n
    FROM accounts
    WHERE external_id LIKE '00u_demo_%'
      AND source_kind = 'okta'
      AND source_name = (SELECT okta_domain FROM ctx)
      AND expired_at IS NULL
  ),
  g AS (
    SELECT id, external_id
    FROM okta_groups
    WHERE external_id LIKE '00g_demo_%'
      AND expired_at IS NULL
  ),
  pairs AS (
    SELECT u.id AS okta_user_account_id, g.id AS okta_group_id
    FROM u
    JOIN g ON (
      (g.external_id = '00g_demo_eng' AND u.n BETWEEN 1 AND 15)
      OR (g.external_id = '00g_demo_security' AND u.n BETWEEN 1 AND 5)
      OR (g.external_id = '00g_demo_finance' AND u.n BETWEEN 16 AND 20)
      OR (g.external_id = '00g_demo_contractors' AND u.n BETWEEN 21 AND 25)
      OR (g.external_id = '00g_demo_it_admins' AND u.n IN (1, 2, 3))
    )
  )
INSERT INTO okta_user_groups (
  okta_user_account_id,
  okta_group_id,
  seen_in_run_id,
  seen_at,
  last_observed_run_id,
  last_observed_at,
  expired_at,
  expired_run_id
)
SELECT
  p.okta_user_account_id,
  p.okta_group_id,
  ctx.run_id,
  ctx.now_ts,
  ctx.run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint
FROM pairs p
CROSS JOIN ctx
ON CONFLICT (okta_user_account_id, okta_group_id) DO UPDATE SET
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  last_observed_run_id = EXCLUDED.last_observed_run_id,
  last_observed_at = EXCLUDED.last_observed_at,
  expired_at = NULL,
  expired_run_id = NULL
;

-- ------------------------------------------------------------
-- Okta app ↔ group assignments
-- ------------------------------------------------------------
WITH
  ctx AS (SELECT * FROM demo_seed_ctx),
  pairs(app_external_id, group_external_id, priority) AS (
    VALUES
      ('0oa_demo_github', '00g_demo_eng', 0),
      ('0oa_demo_github', '00g_demo_it_admins', 10),
      ('0oa_demo_datadog', '00g_demo_security', 0),
      ('0oa_demo_datadog', '00g_demo_it_admins', 10),
      ('0oa_demo_slack', '00g_demo_eng', 0),
      ('0oa_demo_jira', '00g_demo_eng', 0)
  )
INSERT INTO okta_app_group_assignments (
  okta_app_id,
  okta_group_id,
  priority,
  profile_json,
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
  oa.id,
  og.id,
  p.priority,
  jsonb_build_object('assignment', 'group') AS profile_json,
  jsonb_build_object('app', p.app_external_id, 'group', p.group_external_id) AS raw_json,
  ctx.run_id,
  ctx.now_ts,
  ctx.run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint,
  ctx.now_ts
FROM pairs p
JOIN okta_apps oa ON oa.external_id = p.app_external_id
JOIN okta_groups og ON og.external_id = p.group_external_id
CROSS JOIN ctx
ON CONFLICT (okta_app_id, okta_group_id) DO UPDATE SET
  priority = EXCLUDED.priority,
  profile_json = EXCLUDED.profile_json,
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  last_observed_run_id = EXCLUDED.last_observed_run_id,
  last_observed_at = EXCLUDED.last_observed_at,
  expired_at = NULL,
  expired_run_id = NULL,
  updated_at = EXCLUDED.updated_at
;

-- ------------------------------------------------------------
-- Okta user ↔ app assignments
-- ------------------------------------------------------------
WITH
  ctx AS (SELECT * FROM demo_seed_ctx),
  users AS (
    SELECT id, external_id, right(external_id, 3)::int AS n
    FROM accounts
    WHERE external_id LIKE '00u_demo_%'
      AND source_kind = 'okta'
      AND source_name = (SELECT okta_domain FROM ctx)
      AND expired_at IS NULL
  ),
  assignments(user_external_id, app_external_id, scope, profile_json) AS (
    -- Everyone gets Slack (direct)
    SELECT u.external_id, '0oa_demo_slack', 'USER', jsonb_build_object('role', 'member', 'source', 'direct')
    FROM users u

    UNION ALL
    -- Engineering gets Jira (direct)
    SELECT u.external_id, '0oa_demo_jira', 'USER', jsonb_build_object('role', 'user', 'source', 'direct')
    FROM users u
    WHERE u.n BETWEEN 1 AND 15

    UNION ALL
    -- Engineering gets GitHub (group)
    SELECT u.external_id, '0oa_demo_github', 'GROUP', jsonb_build_object('role', 'member', 'source', 'group')
    FROM users u
    WHERE u.n BETWEEN 1 AND 15

    UNION ALL
    -- Security gets Datadog (group)
    SELECT u.external_id, '0oa_demo_datadog', 'GROUP', jsonb_build_object('role', 'readonly', 'source', 'group')
    FROM users u
    WHERE u.n BETWEEN 1 AND 5

    UNION ALL
    -- A few direct Datadog users
    SELECT u.external_id, '0oa_demo_datadog', 'USER', jsonb_build_object('role', 'user', 'source', 'direct')
    FROM users u
    WHERE u.n BETWEEN 6 AND 10
  )
INSERT INTO okta_user_app_assignments (
  okta_user_account_id,
  okta_app_id,
  scope,
  profile_json,
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
  u.id,
  oa.id,
  a.scope,
  a.profile_json,
  jsonb_build_object('demo', true) AS raw_json,
  ctx.run_id,
  ctx.now_ts,
  ctx.run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint,
  ctx.now_ts
FROM assignments a
JOIN accounts u ON u.external_id = a.user_external_id
  AND u.source_kind = 'okta'
  AND u.source_name = (SELECT okta_domain FROM ctx)
JOIN okta_apps oa ON oa.external_id = a.app_external_id
CROSS JOIN ctx
ON CONFLICT (okta_user_account_id, okta_app_id) DO UPDATE SET
  scope = EXCLUDED.scope,
  profile_json = EXCLUDED.profile_json,
  raw_json = EXCLUDED.raw_json,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  last_observed_run_id = EXCLUDED.last_observed_run_id,
  last_observed_at = EXCLUDED.last_observed_at,
  expired_at = NULL,
  expired_run_id = NULL,
  updated_at = EXCLUDED.updated_at
;

-- ------------------------------------------------------------
-- GitHub app users
-- ------------------------------------------------------------
WITH
  ctx AS (SELECT * FROM demo_seed_ctx),
  users AS (
    SELECT
      i,
      format('demo-user%s', to_char(i, 'FM000')) AS login,
      CASE WHEN i <= 10 THEN format('demo.user%s@example.com', to_char(i, 'FM000')) ELSE '' END AS email,
      format('Demo User %s', to_char(i, 'FM000')) AS display_name
    FROM generate_series(1, 15) AS s(i)
  )
INSERT INTO accounts (
  source_kind,
  source_name,
  external_id,
  email,
  display_name,
  status,
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
  'github',
  ctx.github_org,
  u.login,
  lower(u.email),
  u.display_name,
  'active',
  jsonb_build_object('login', u.login, 'type', 'User', 'status', 'active') AS raw_json,
  NULL::timestamptz,
  ''::text,
  ''::text,
  ctx.run_id,
  ctx.now_ts,
  ctx.run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint,
  ctx.now_ts
FROM users u
CROSS JOIN ctx
ON CONFLICT (source_kind, source_name, external_id) DO UPDATE SET
  email = EXCLUDED.email,
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

-- GitHub entitlements
WITH
  ctx AS (SELECT * FROM demo_seed_ctx),
  gh AS (
    SELECT id, external_id, right(external_id, 3)::int AS n
    FROM accounts
    WHERE source_kind = 'github'
      AND source_name = (SELECT github_org FROM ctx)
      AND external_id ~ '^demo-user[0-9]{3}$'
      AND right(external_id, 3)::int <= 15
      AND expired_at IS NULL
  ),
  ents AS (
    -- org role
    SELECT
      gh.id AS app_user_id,
      'github_org_role'::text AS kind,
      (SELECT github_org FROM ctx)::text AS resource,
      CASE WHEN gh.n <= 3 THEN 'admin' ELSE 'member' END AS permission,
      jsonb_build_object('org', (SELECT github_org FROM ctx), 'role', CASE WHEN gh.n <= 3 THEN 'admin' ELSE 'member' END) AS raw_json
    FROM gh

    UNION ALL
    -- team membership
    SELECT
      gh.id,
      'github_team_member',
      'engineering',
      'member',
      jsonb_build_object('team', 'engineering') AS raw_json
    FROM gh
    WHERE gh.n <= 10

    UNION ALL
    -- repo permission (representative)
    SELECT
      gh.id,
      'github_team_repo_permission',
      format('%s/%s', (SELECT github_org FROM ctx), 'core'),
      CASE WHEN gh.n <= 5 THEN 'write' ELSE 'read' END,
      jsonb_build_object(
        'team', 'engineering',
        'repo', format('%s/%s', (SELECT github_org FROM ctx), 'core'),
        'permission', CASE WHEN gh.n <= 5 THEN 'write' ELSE 'read' END
      ) AS raw_json
    FROM gh
    WHERE gh.n <= 12
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
  e.app_user_id,
  e.kind,
  e.resource,
  e.permission,
  e.raw_json,
  ctx.run_id,
  ctx.now_ts,
  ctx.run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint,
  ctx.now_ts
FROM ents e
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

-- ------------------------------------------------------------
-- Datadog app users
-- ------------------------------------------------------------
WITH
  ctx AS (SELECT * FROM demo_seed_ctx),
  users AS (
    SELECT
      i,
      format('dd-demo-%s', to_char(i, 'FM000')) AS external_id,
      CASE WHEN i <= 8 THEN format('demo.user%s@example.com', to_char(i, 'FM000')) ELSE '' END AS email,
      format('Demo User %s', to_char(i, 'FM000')) AS display_name,
      CASE WHEN (i % 6) = 0 THEN 'inactive' ELSE 'active' END AS status
    FROM generate_series(1, 12) AS s(i)
  )
INSERT INTO accounts (
  source_kind,
  source_name,
  external_id,
  email,
  display_name,
  status,
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
  'datadog',
  ctx.datadog_site,
  u.external_id,
  lower(u.email),
  u.display_name,
  u.status,
  jsonb_build_object('status', u.status) AS raw_json,
  NULL::timestamptz,
  ''::text,
  ''::text,
  ctx.run_id,
  ctx.now_ts,
  ctx.run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint,
  ctx.now_ts
FROM users u
CROSS JOIN ctx
ON CONFLICT (source_kind, source_name, external_id) DO UPDATE SET
  email = EXCLUDED.email,
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

-- Datadog role entitlements
WITH
  ctx AS (SELECT * FROM demo_seed_ctx),
  dd AS (
    SELECT id, external_id, right(external_id, 3)::int AS n
    FROM accounts
    WHERE source_kind = 'datadog'
      AND source_name = (SELECT datadog_site FROM ctx)
      AND external_id LIKE 'dd-demo-%'
      AND expired_at IS NULL
  ),
  roles AS (
    SELECT dd.id AS app_user_id,
           'datadog_role'::text AS kind,
           CASE
             WHEN dd.n <= 2 THEN 'Admin'
             WHEN dd.n <= 6 THEN 'Standard'
             ELSE 'Read Only'
           END AS role_name
    FROM dd
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
  r.app_user_id,
  r.kind,
  r.role_name AS resource,
  'member' AS permission,
  jsonb_build_object('role_id', lower(replace(r.role_name, ' ', '_')), 'role_name', r.role_name) AS raw_json,
  ctx.run_id,
  ctx.now_ts,
  ctx.run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint,
  ctx.now_ts
FROM roles r
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

-- ------------------------------------------------------------
-- Identity graph linking (seed links by matching email).
-- ------------------------------------------------------------
WITH
  okta_accounts AS (
    SELECT
      a.id AS account_id,
      lower(a.email) AS email,
      a.display_name
    FROM accounts a
    WHERE a.source_kind = 'okta'
      AND a.source_name = (SELECT okta_domain FROM demo_seed_ctx)
      AND a.external_id LIKE '00u_demo_%'
      AND a.email <> ''
      AND a.expired_at IS NULL
  ),
  seeded_okta_identities AS (
    INSERT INTO identities (kind, display_name, primary_email)
    SELECT
      'human',
      oa.display_name,
      oa.email
    FROM okta_accounts oa
    LEFT JOIN identity_accounts ia ON ia.account_id = oa.account_id
    WHERE ia.account_id IS NULL
    RETURNING id
  ),
  okta_identity_links AS (
    SELECT
      oa.account_id,
      i.id AS identity_id
    FROM okta_accounts oa
    JOIN LATERAL (
      SELECT id
      FROM identities
      WHERE lower(primary_email) = oa.email
      ORDER BY id ASC
      LIMIT 1
    ) i ON TRUE
  ),
  seeded_okta_links AS (
    INSERT INTO identity_accounts (identity_id, account_id, link_reason, confidence, updated_at)
    SELECT
      oil.identity_id,
      oil.account_id,
      'demo_okta_seed',
      1.0,
      now()
    FROM okta_identity_links oil
    ON CONFLICT (account_id) DO UPDATE SET
      identity_id = EXCLUDED.identity_id,
      link_reason = EXCLUDED.link_reason,
      confidence = EXCLUDED.confidence,
      updated_at = EXCLUDED.updated_at
    RETURNING account_id
  ),
  seed_guard AS (
    SELECT count(*) AS n
    FROM seeded_okta_links
  ),
  matches AS (
    SELECT
      app.id AS account_id,
      okta_link.identity_id
    FROM accounts app
    JOIN seed_guard ON TRUE
    JOIN okta_accounts oa ON lower(app.email) = oa.email
    JOIN okta_identity_links okta_link ON okta_link.account_id = oa.account_id
    WHERE app.source_kind IN ('github', 'datadog')
      AND app.email <> ''
      AND app.expired_at IS NULL
  )
INSERT INTO identity_accounts (identity_id, account_id, link_reason, confidence, updated_at)
SELECT m.identity_id, m.account_id, 'demo_email', 1.0, now()
FROM matches m
ON CONFLICT (account_id) DO UPDATE SET
  identity_id = EXCLUDED.identity_id,
  link_reason = EXCLUDED.link_reason,
  confidence = EXCLUDED.confidence,
  updated_at = EXCLUDED.updated_at
;

-- ------------------------------------------------------------
-- Findings (Okta benchmark): seed a small mix of pass/fail results.
-- ------------------------------------------------------------
WITH
  ctx AS (SELECT * FROM demo_seed_ctx),
  rs AS (
    SELECT id
    FROM rulesets
    WHERE key = 'cis.okta.idaas_stig.v2'
    LIMIT 1
  ),
  picked AS (
    SELECT
      r.id AS rule_id,
      r.key AS rule_key,
      row_number() OVER (ORDER BY r.key) AS rn
    FROM rules r
    JOIN rs ON rs.id = r.ruleset_id
    WHERE r.is_active = true
    ORDER BY r.key
    LIMIT 24
  )
INSERT INTO rule_results_current (
  rule_id,
  scope_kind,
  source_kind,
  source_name,
  status,
  evaluated_at,
  sync_run_id,
  evidence_summary,
  evidence_json,
  affected_resource_ids,
  error_kind
)
SELECT
  p.rule_id,
  'connector_instance',
  'okta',
  ctx.okta_domain,
  CASE WHEN (p.rn % 2) = 0 THEN 'fail' ELSE 'pass' END,
  ctx.now_ts,
  ctx.run_id,
  CASE WHEN (p.rn % 2) = 0 THEN 'Demo seeded: failing control' ELSE 'Demo seeded: passing control' END,
  jsonb_build_object(
    'schema_version', 1,
    'rule', jsonb_build_object(
      'ruleset_key', 'cis.okta.idaas_stig.v2',
      'rule_key', p.rule_key
    ),
    'check', jsonb_build_object(
      'type', 'demo.seed'
    ),
    'params', jsonb_build_object(),
    'result', jsonb_build_object(
      'status', CASE WHEN (p.rn % 2) = 0 THEN 'fail' ELSE 'pass' END
    ),
    'selection', jsonb_build_object(
      'total', 10,
      'selected', CASE WHEN (p.rn % 2) = 0 THEN 2 ELSE 10 END
    ),
    'violations', CASE
      WHEN (p.rn % 2) = 0 THEN jsonb_build_array(
        jsonb_build_object('resource_id', 'demo:resource/1', 'display', 'Demo seeded violation #1'),
        jsonb_build_object('resource_id', 'demo:resource/2', 'display', 'Demo seeded violation #2')
      )
      ELSE '[]'::jsonb
    END,
    'violations_truncated', false,
    'demo', true,
    'seeded_at', ctx.now_ts
  ),
  '{}'::text[],
  ''::text
FROM picked p
CROSS JOIN ctx
ON CONFLICT (rule_id, scope_kind, source_kind, source_name) DO UPDATE SET
  status = EXCLUDED.status,
  evaluated_at = EXCLUDED.evaluated_at,
  sync_run_id = EXCLUDED.sync_run_id,
  evidence_summary = EXCLUDED.evidence_summary,
  evidence_json = EXCLUDED.evidence_json,
  affected_resource_ids = EXCLUDED.affected_resource_ids,
  error_kind = EXCLUDED.error_kind,
  updated_at = now()
;

COMMIT;

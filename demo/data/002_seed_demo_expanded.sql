-- Expanded demo seed data for Open-SSPM (upsert-only).
-- Adds high-volume records across identity, assignments, and programmatic access.

BEGIN;

CREATE TEMP TABLE demo_seed_ctx_v2 (
  run_id BIGINT NOT NULL,
  okta_domain TEXT NOT NULL,
  github_org TEXT NOT NULL,
  datadog_site TEXT NOT NULL,
  entra_tenant_id TEXT NOT NULL,
  now_ts TIMESTAMPTZ NOT NULL
) ON COMMIT DROP;

TRUNCATE demo_seed_ctx_v2;

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
    SELECT 'demo_seed', 'demo', 'success', now(), now(), 'seeded expanded demo data', '{}'::jsonb, ''
    WHERE NOT EXISTS (SELECT 1 FROM existing)
    RETURNING id
  ),
  run AS (
    SELECT COALESCE((SELECT id FROM ins), (SELECT id FROM existing)) AS id
  )
INSERT INTO demo_seed_ctx_v2 (run_id, okta_domain, github_org, datadog_site, entra_tenant_id, now_ts)
SELECT
  (SELECT id FROM run) AS run_id,
  'demo.okta.example.com' AS okta_domain,
  'open-sspm-demo' AS github_org,
  'datadoghq.com' AS datadog_site,
  '00000000-0000-4000-8000-000000000001' AS entra_tenant_id,
  now() AS now_ts;

UPDATE sync_runs
SET
  status = 'success',
  started_at = (SELECT now_ts FROM demo_seed_ctx_v2),
  finished_at = (SELECT now_ts FROM demo_seed_ctx_v2),
  message = 'seeded expanded demo data',
  stats = '{}'::jsonb,
  error_kind = ''
WHERE id = (SELECT run_id FROM demo_seed_ctx_v2);

-- ------------------------------------------------------------
-- Ensure Entra is configured+enabled so "All configured" has multiple sources.
-- ------------------------------------------------------------
WITH ctx AS (SELECT * FROM demo_seed_ctx_v2)
INSERT INTO connector_configs (kind, enabled, config, updated_at)
VALUES (
  'entra',
  true,
  jsonb_build_object(
    'tenant_id', (SELECT entra_tenant_id FROM ctx),
    'client_id', '00000000-0000-4000-8000-000000000002',
    'client_secret', 'demo_entra_client_secret'
  ),
  (SELECT now_ts FROM ctx)
)
ON CONFLICT (kind) DO UPDATE SET
  enabled = EXCLUDED.enabled,
  config = EXCLUDED.config,
  updated_at = EXCLUDED.updated_at
;

-- Identity source settings (Okta authoritative; app sources non-authoritative).
WITH ctx AS (SELECT * FROM demo_seed_ctx_v2)
INSERT INTO identity_source_settings (source_kind, source_name, is_authoritative, updated_at)
VALUES
  ('okta', (SELECT okta_domain FROM ctx), true, (SELECT now_ts FROM ctx)),
  ('github', (SELECT github_org FROM ctx), false, (SELECT now_ts FROM ctx)),
  ('datadog', (SELECT datadog_site FROM ctx), false, (SELECT now_ts FROM ctx)),
  ('entra', (SELECT entra_tenant_id FROM ctx), false, (SELECT now_ts FROM ctx))
ON CONFLICT (source_kind, source_name) DO UPDATE SET
  is_authoritative = EXCLUDED.is_authoritative,
  updated_at = EXCLUDED.updated_at
;

-- ------------------------------------------------------------
-- Additional Okta users.
-- ------------------------------------------------------------
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v2),
  first_names AS (
    SELECT ARRAY[
      'Alex','Sam','Jordan','Taylor','Morgan','Casey','Riley','Jamie','Avery','Quinn',
      'Cameron','Skyler','Parker','Emerson','Rowan','Reese','Hayden','Finley','Dakota','Robin',
      'Sasha','Blake','Harper','Logan','Elliot','Kendall','Phoenix','Remy','Shawn','Toni'
    ]::text[] AS v
  ),
  last_names AS (
    SELECT ARRAY[
      'Lee','Kim','Patel','Nguyen','Garcia','Brown','Johnson','Martinez','Davis','Wilson',
      'Anderson','Thomas','Moore','Jackson','Martin','Thompson','White','Lopez','Clark','Lewis',
      'Scott','Ramirez','Flores','Hughes','Ward','Price','Bennett','Brooks','Sanders','Reed'
    ]::text[] AS v
  ),
  users AS (
    SELECT
      i,
      format('00u_demo_%s', to_char(i, 'FM000')) AS external_id,
      format('demo.user%s@example.com', to_char(i, 'FM000')) AS email,
      CASE WHEN (i % 11) = 0 THEN 'SUSPENDED' ELSE 'ACTIVE' END AS status
    FROM generate_series(26, 220) AS s(i)
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
-- Additional Okta groups.
-- ------------------------------------------------------------
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v2),
  groups(external_id, name, type) AS (
    VALUES
      ('00g_demo_platform', 'Platform', 'OKTA_GROUP'),
      ('00g_demo_data', 'Data', 'OKTA_GROUP'),
      ('00g_demo_reliability', 'Reliability', 'OKTA_GROUP'),
      ('00g_demo_sales', 'Sales', 'OKTA_GROUP'),
      ('00g_demo_support', 'Support', 'OKTA_GROUP'),
      ('00g_demo_hr', 'Human Resources', 'OKTA_GROUP')
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
-- Expand Okta user ↔ group memberships across 220 users.
-- ------------------------------------------------------------
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v2),
  users AS (
    SELECT
      id,
      regexp_replace(external_id, '^00u_demo_', '')::int AS n
    FROM accounts
    WHERE external_id LIKE '00u_demo_%'
      AND source_kind = 'okta'
      AND source_name = (SELECT okta_domain FROM ctx)
      AND expired_at IS NULL
  ),
  groups AS (
    SELECT id, external_id
    FROM okta_groups
    WHERE external_id IN (
      '00g_demo_eng',
      '00g_demo_security',
      '00g_demo_finance',
      '00g_demo_contractors',
      '00g_demo_it_admins',
      '00g_demo_platform',
      '00g_demo_data',
      '00g_demo_reliability',
      '00g_demo_sales',
      '00g_demo_support',
      '00g_demo_hr'
    )
      AND expired_at IS NULL
  ),
  pairs AS (
    SELECT users.id AS okta_user_account_id, groups.id AS okta_group_id
    FROM users
    JOIN groups ON (
      (groups.external_id = '00g_demo_eng' AND users.n BETWEEN 1 AND 120)
      OR (groups.external_id = '00g_demo_security' AND users.n BETWEEN 1 AND 35)
      OR (groups.external_id = '00g_demo_finance' AND users.n BETWEEN 121 AND 150)
      OR (groups.external_id = '00g_demo_contractors' AND users.n BETWEEN 151 AND 180)
      OR (groups.external_id = '00g_demo_it_admins' AND users.n IN (1, 2, 3, 8, 13, 21, 34, 55, 89, 144, 200))
      OR (groups.external_id = '00g_demo_platform' AND users.n BETWEEN 15 AND 95)
      OR (groups.external_id = '00g_demo_data' AND users.n BETWEEN 60 AND 145)
      OR (groups.external_id = '00g_demo_reliability' AND users.n <= 200 AND (users.n % 4) = 0)
      OR (groups.external_id = '00g_demo_sales' AND users.n BETWEEN 141 AND 200)
      OR (groups.external_id = '00g_demo_support' AND users.n BETWEEN 181 AND 215)
      OR (groups.external_id = '00g_demo_hr' AND users.n BETWEEN 216 AND 220)
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
  pairs.okta_user_account_id,
  pairs.okta_group_id,
  ctx.run_id,
  ctx.now_ts,
  ctx.run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint
FROM pairs
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
-- Additional Okta apps.
-- ------------------------------------------------------------
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v2),
  apps(external_id, label, name, status, sign_on_mode) AS (
    VALUES
      ('0oa_demo_zoom', 'Zoom', 'zoom', 'ACTIVE', 'SAML_2_0'),
      ('0oa_demo_notion', 'Notion', 'notion', 'ACTIVE', 'SAML_2_0'),
      ('0oa_demo_servicenow', 'ServiceNow', 'servicenow', 'ACTIVE', 'SAML_2_0'),
      ('0oa_demo_miro', 'Miro', 'miro', 'ACTIVE', 'SAML_2_0'),
      ('0oa_demo_snowflake', 'Snowflake', 'snowflake', 'ACTIVE', 'SAML_2_0'),
      ('0oa_demo_aws_console', 'AWS Console', 'aws_console', 'ACTIVE', 'SAML_2_0'),
      ('0oa_demo_pagerduty', 'PagerDuty', 'pagerduty', 'ACTIVE', 'SAML_2_0')
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
  apps.external_id,
  apps.label,
  apps.name,
  apps.status,
  apps.sign_on_mode,
  jsonb_build_object(
    'id', apps.external_id,
    'label', apps.label,
    'name', apps.name,
    'status', apps.status,
    'signOnMode', apps.sign_on_mode
  ) AS raw_json,
  ctx.run_id,
  ctx.now_ts,
  ctx.run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint,
  ctx.now_ts
FROM apps
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

-- ------------------------------------------------------------
-- Expanded Okta app ↔ group assignments.
-- ------------------------------------------------------------
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v2),
  pairs(app_external_id, group_external_id, priority) AS (
    VALUES
      ('0oa_demo_zoom', '00g_demo_support', 0),
      ('0oa_demo_zoom', '00g_demo_sales', 10),
      ('0oa_demo_notion', '00g_demo_eng', 0),
      ('0oa_demo_notion', '00g_demo_data', 10),
      ('0oa_demo_servicenow', '00g_demo_reliability', 0),
      ('0oa_demo_servicenow', '00g_demo_it_admins', 10),
      ('0oa_demo_miro', '00g_demo_eng', 0),
      ('0oa_demo_miro', '00g_demo_platform', 10),
      ('0oa_demo_snowflake', '00g_demo_data', 0),
      ('0oa_demo_snowflake', '00g_demo_finance', 10),
      ('0oa_demo_aws_console', '00g_demo_platform', 0),
      ('0oa_demo_aws_console', '00g_demo_reliability', 10),
      ('0oa_demo_pagerduty', '00g_demo_reliability', 0),
      ('0oa_demo_pagerduty', '00g_demo_it_admins', 10),
      ('0oa_demo_github', '00g_demo_platform', 20),
      ('0oa_demo_datadog', '00g_demo_reliability', 20)
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
  apps.id,
  groups.id,
  pairs.priority,
  jsonb_build_object('assignment', 'group') AS profile_json,
  jsonb_build_object('app', pairs.app_external_id, 'group', pairs.group_external_id) AS raw_json,
  ctx.run_id,
  ctx.now_ts,
  ctx.run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint,
  ctx.now_ts
FROM pairs
JOIN okta_apps apps ON apps.external_id = pairs.app_external_id
JOIN okta_groups groups ON groups.external_id = pairs.group_external_id
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
-- Expanded Okta user ↔ app assignments.
-- ------------------------------------------------------------
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v2),
  users AS (
    SELECT id, external_id, regexp_replace(external_id, '^00u_demo_', '')::int AS n
    FROM accounts
    WHERE external_id LIKE '00u_demo_%'
      AND source_kind = 'okta'
      AND source_name = (SELECT okta_domain FROM ctx)
      AND expired_at IS NULL
  ),
  assignments(user_external_id, app_external_id, scope, profile_json) AS (
    SELECT users.external_id, '0oa_demo_slack', 'USER', jsonb_build_object('role', 'member', 'source', 'direct')
    FROM users

    UNION ALL

    SELECT users.external_id, '0oa_demo_jira', 'USER', jsonb_build_object('role', 'user', 'source', 'direct')
    FROM users
    WHERE users.n <= 150

    UNION ALL

    SELECT users.external_id, '0oa_demo_github', 'GROUP', jsonb_build_object('role', 'member', 'source', 'group')
    FROM users
    WHERE users.n <= 145

    UNION ALL

    SELECT users.external_id, '0oa_demo_datadog', 'GROUP', jsonb_build_object('role', 'readonly', 'source', 'group')
    FROM users
    WHERE users.n <= 80

    UNION ALL

    SELECT users.external_id, '0oa_demo_datadog', 'USER', jsonb_build_object('role', 'user', 'source', 'direct')
    FROM users
    WHERE users.n BETWEEN 81 AND 110

    UNION ALL

    SELECT users.external_id, '0oa_demo_confluence', 'USER', jsonb_build_object('role', 'licensed', 'source', 'direct')
    FROM users
    WHERE users.n <= 180

    UNION ALL

    SELECT users.external_id, '0oa_demo_zoom', 'USER', jsonb_build_object('role', 'licensed', 'source', 'group')
    FROM users
    WHERE users.n BETWEEN 100 AND 220

    UNION ALL

    SELECT users.external_id, '0oa_demo_notion', 'USER', jsonb_build_object('role', 'member', 'source', 'group')
    FROM users
    WHERE users.n <= 200

    UNION ALL

    SELECT users.external_id, '0oa_demo_snowflake', 'USER', jsonb_build_object('role', 'analyst', 'source', 'group')
    FROM users
    WHERE users.n BETWEEN 60 AND 160

    UNION ALL

    SELECT users.external_id, '0oa_demo_servicenow', 'USER', jsonb_build_object('role', 'itil', 'source', 'group')
    FROM users
    WHERE users.n BETWEEN 1 AND 120

    UNION ALL

    SELECT users.external_id, '0oa_demo_aws_console', 'USER', jsonb_build_object('role', 'developer', 'source', 'group')
    FROM users
    WHERE users.n BETWEEN 40 AND 190

    UNION ALL

    SELECT users.external_id, '0oa_demo_pagerduty', 'USER', jsonb_build_object('role', 'responder', 'source', 'group')
    FROM users
    WHERE users.n BETWEEN 30 AND 170
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
  users.id,
  apps.id,
  assignments.scope,
  assignments.profile_json,
  jsonb_build_object('demo', true, 'expansion', true) AS raw_json,
  ctx.run_id,
  ctx.now_ts,
  ctx.run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint,
  ctx.now_ts
FROM assignments
JOIN accounts users ON users.external_id = assignments.user_external_id
  AND users.source_kind = 'okta'
  AND users.source_name = (SELECT okta_domain FROM ctx)
JOIN okta_apps apps ON apps.external_id = assignments.app_external_id
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
-- Additional GitHub app users.
-- ------------------------------------------------------------
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v2),
  users AS (
    SELECT
      i,
      format('demo-user%s', to_char(i, 'FM000')) AS external_id,
      CASE WHEN i <= 180 THEN format('demo.user%s@example.com', to_char(i, 'FM000')) ELSE '' END AS email,
      format('Demo User %s', to_char(i, 'FM000')) AS display_name
    FROM generate_series(16, 220) AS s(i)
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
  users.external_id,
  lower(users.email),
  users.display_name,
  'active',
  jsonb_build_object('login', users.external_id, 'type', 'User', 'expansion', true) AS raw_json,
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
FROM users
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

-- Expanded GitHub entitlements for additional app users.
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v2),
  gh AS (
    SELECT
      id,
      right(external_id, 3)::int AS n
    FROM accounts
    WHERE source_kind = 'github'
      AND source_name = (SELECT github_org FROM ctx)
      AND external_id ~ '^demo-user[0-9]{3}$'
      AND right(external_id, 3)::int >= 16
      AND expired_at IS NULL
  ),
  ents AS (
    SELECT
      gh.id AS app_user_id,
      'github_org_role'::text AS kind,
      (SELECT github_org FROM ctx)::text AS resource,
      CASE WHEN gh.n <= 20 THEN 'admin' ELSE 'member' END AS permission,
      jsonb_build_object('org', (SELECT github_org FROM ctx), 'role', CASE WHEN gh.n <= 20 THEN 'admin' ELSE 'member' END) AS raw_json
    FROM gh

    UNION ALL

    SELECT
      gh.id,
      'github_team_member',
      'engineering',
      'member',
      jsonb_build_object('team', 'engineering') AS raw_json
    FROM gh
    WHERE gh.n <= 140

    UNION ALL

    SELECT
      gh.id,
      'github_team_member',
      'data',
      'member',
      jsonb_build_object('team', 'data') AS raw_json
    FROM gh
    WHERE gh.n BETWEEN 60 AND 145

    UNION ALL

    SELECT
      gh.id,
      'github_team_repo_permission',
      format('%s/%s', (SELECT github_org FROM ctx), 'core'),
      CASE WHEN gh.n <= 60 THEN 'write' ELSE 'read' END,
      jsonb_build_object('team', 'engineering', 'repo', format('%s/%s', (SELECT github_org FROM ctx), 'core'))
    FROM gh
    WHERE gh.n <= 180

    UNION ALL

    SELECT
      gh.id,
      'github_team_repo_permission',
      format('%s/%s', (SELECT github_org FROM ctx), 'platform'),
      CASE WHEN (gh.n % 5) = 0 THEN 'admin' WHEN (gh.n % 2) = 0 THEN 'write' ELSE 'read' END,
      jsonb_build_object('team', 'platform', 'repo', format('%s/%s', (SELECT github_org FROM ctx), 'platform'))
    FROM gh
    WHERE gh.n BETWEEN 20 AND 180

    UNION ALL

    SELECT
      gh.id,
      'github_team_repo_permission',
      format('%s/%s', (SELECT github_org FROM ctx), 'data-pipelines'),
      CASE WHEN (gh.n % 3) = 0 THEN 'write' ELSE 'read' END,
      jsonb_build_object('team', 'data', 'repo', format('%s/%s', (SELECT github_org FROM ctx), 'data-pipelines'))
    FROM gh
    WHERE gh.n BETWEEN 70 AND 170
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
  ctx.run_id,
  ctx.now_ts,
  ctx.run_id,
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

-- ------------------------------------------------------------
-- Additional Datadog app users and role entitlements.
-- ------------------------------------------------------------
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v2),
  users AS (
    SELECT
      i,
      format('dd-demo-%s', to_char(i, 'FM000')) AS external_id,
      CASE WHEN i <= 150 THEN format('demo.user%s@example.com', to_char(i, 'FM000')) ELSE '' END AS email,
      format('Demo User %s', to_char(i, 'FM000')) AS display_name,
      CASE WHEN (i % 9) = 0 THEN 'inactive' ELSE 'active' END AS status
    FROM generate_series(13, 180) AS s(i)
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
  users.external_id,
  lower(users.email),
  users.display_name,
  users.status,
  jsonb_build_object('status', users.status, 'expansion', true) AS raw_json,
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
FROM users
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

WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v2),
  dd AS (
    SELECT id, regexp_replace(external_id, '^dd-demo-', '')::int AS n
    FROM accounts
    WHERE source_kind = 'datadog'
      AND source_name = (SELECT datadog_site FROM ctx)
      AND external_id LIKE 'dd-demo-%'
      AND expired_at IS NULL
  ),
  roles AS (
    SELECT
      dd.id AS app_user_id,
      'datadog_role'::text AS kind,
      CASE
        WHEN dd.n <= 12 THEN 'Admin'
        WHEN dd.n <= 70 THEN 'Standard'
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
  roles.app_user_id,
  roles.kind,
  roles.role_name AS resource,
  'member' AS permission,
  jsonb_build_object('role_id', lower(replace(roles.role_name, ' ', '_')), 'role_name', roles.role_name, 'expansion', true) AS raw_json,
  ctx.run_id,
  ctx.now_ts,
  ctx.run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint,
  ctx.now_ts
FROM roles
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
-- Entra app users + directory-role entitlements for richer identity demos.
-- ------------------------------------------------------------
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v2),
  users AS (
    SELECT
      i,
      format('entra-user-%s', to_char(i, 'FM000')) AS external_id,
      CASE WHEN i <= 140 THEN format('demo.user%s@example.com', to_char(i, 'FM000')) ELSE '' END AS email,
      format('Entra User %s', to_char(i, 'FM000')) AS display_name
    FROM generate_series(1, 180) AS s(i)
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
  'entra',
  ctx.entra_tenant_id,
  users.external_id,
  lower(users.email),
  users.display_name,
  'active',
  jsonb_build_object('kind', 'user', 'expansion', true) AS raw_json,
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
FROM users
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

WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v2),
  entra_users AS (
    SELECT
      id,
      regexp_replace(external_id, '^entra-user-', '')::int AS n
    FROM accounts
    WHERE source_kind = 'entra'
      AND source_name = (SELECT entra_tenant_id FROM ctx)
      AND external_id LIKE 'entra-user-%'
      AND expired_at IS NULL
  ),
  ents AS (
    SELECT
      entra_users.id AS app_user_id,
      'entra_directory_role'::text AS kind,
      CASE
        WHEN entra_users.n <= 8 THEN 'Global Administrator'
        WHEN entra_users.n <= 35 THEN 'Application Administrator'
        WHEN entra_users.n <= 80 THEN 'Cloud Application Administrator'
        WHEN entra_users.n <= 120 THEN 'Security Reader'
        ELSE 'Directory Reader'
      END AS resource,
      'member'::text AS permission,
      jsonb_build_object('role_type', 'directory', 'expansion', true) AS raw_json
    FROM entra_users
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
  ctx.run_id,
  ctx.now_ts,
  ctx.run_id,
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

-- ------------------------------------------------------------
-- Identity graph linking for expanded app users by email.
-- ------------------------------------------------------------
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v2),
  okta_accounts AS (
    SELECT
      a.id AS account_id,
      lower(a.email) AS email,
      a.display_name
    FROM accounts a
    WHERE a.source_kind = 'okta'
      AND a.source_name = (SELECT okta_domain FROM ctx)
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
    WHERE app.source_kind IN ('github', 'datadog', 'entra')
      AND app.email <> ''
      AND app.expired_at IS NULL
  )
INSERT INTO identity_accounts (identity_id, account_id, link_reason, confidence, updated_at)
SELECT matches.identity_id, matches.account_id, 'demo_email', 1.0, now()
FROM matches
ON CONFLICT (account_id) DO UPDATE SET
  identity_id = EXCLUDED.identity_id,
  link_reason = EXCLUDED.link_reason,
  confidence = EXCLUDED.confidence,
  updated_at = EXCLUDED.updated_at
;

-- ------------------------------------------------------------
-- Programmatic access app assets: lots of GitHub + Entra data.
-- ------------------------------------------------------------
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v2),
  github_assets AS (
    SELECT
      'github'::text AS source_kind,
      (SELECT github_org FROM ctx)::text AS source_name,
      'github_app_installation'::text AS asset_kind,
      (500000 + i)::text AS external_id,
      format('demo-app-%s', to_char(((i - 1) % 60) + 1, 'FM000')) AS parent_external_id,
      format('Demo GitHub App Installation %s', to_char(i, 'FM000')) AS display_name,
      CASE WHEN (i % 19) = 0 THEN 'suspended' ELSE 'active' END AS status,
      (SELECT now_ts FROM ctx) - make_interval(days => (i % 700) + 5) AS created_at_source,
      (SELECT now_ts FROM ctx) - make_interval(days => (i % 30)) AS updated_at_source,
      jsonb_build_object(
        'kind', 'github_app_installation',
        'installation_id', (500000 + i),
        'app_slug', format('demo-app-%s', to_char(((i - 1) % 60) + 1, 'FM000')),
        'status', CASE WHEN (i % 19) = 0 THEN 'suspended' ELSE 'active' END,
        'expansion', true
      ) AS raw_json
    FROM generate_series(1, 360) AS s(i)
  ),
  entra_apps AS (
    SELECT
      'entra'::text AS source_kind,
      (SELECT entra_tenant_id FROM ctx)::text AS source_name,
      'entra_application'::text AS asset_kind,
      format('00000000-0000-4000-8000-%s', lpad(i::text, 12, '0')) AS external_id,
      ''::text AS parent_external_id,
      format('Demo Entra Application %s', to_char(i, 'FM000')) AS display_name,
      ''::text AS status,
      (SELECT now_ts FROM ctx) - make_interval(days => (i % 900) + 30) AS created_at_source,
      (SELECT now_ts FROM ctx) - make_interval(days => (i % 45)) AS updated_at_source,
      jsonb_build_object(
        'kind', 'entra_application',
        'app_id', format('10000000-0000-4000-8000-%s', lpad(i::text, 12, '0')),
        'display_name', format('Demo Entra Application %s', to_char(i, 'FM000')),
        'expansion', true
      ) AS raw_json
    FROM generate_series(1, 240) AS s(i)
  ),
  entra_service_principals AS (
    SELECT
      'entra'::text AS source_kind,
      (SELECT entra_tenant_id FROM ctx)::text AS source_name,
      'entra_service_principal'::text AS asset_kind,
      format('11111111-1111-4111-8111-%s', lpad(i::text, 12, '0')) AS external_id,
      format('10000000-0000-4000-8000-%s', lpad((((i - 1) % 240) + 1)::text, 12, '0')) AS parent_external_id,
      format('Demo Service Principal %s', to_char(i, 'FM000')) AS display_name,
      CASE WHEN (i % 13) = 0 THEN 'inactive' ELSE 'active' END AS status,
      (SELECT now_ts FROM ctx) - make_interval(days => (i % 800) + 10) AS created_at_source,
      (SELECT now_ts FROM ctx) - make_interval(days => (i % 35)) AS updated_at_source,
      jsonb_build_object(
        'kind', 'entra_service_principal',
        'app_id', format('10000000-0000-4000-8000-%s', lpad((((i - 1) % 240) + 1)::text, 12, '0')),
        'display_name', format('Demo Service Principal %s', to_char(i, 'FM000')),
        'account_enabled', CASE WHEN (i % 13) = 0 THEN false ELSE true END,
        'expansion', true
      ) AS raw_json
    FROM generate_series(1, 360) AS s(i)
  ),
  all_assets AS (
    SELECT * FROM github_assets
    UNION ALL
    SELECT * FROM entra_apps
    UNION ALL
    SELECT * FROM entra_service_principals
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
  all_assets.source_kind,
  all_assets.source_name,
  all_assets.asset_kind,
  all_assets.external_id,
  all_assets.parent_external_id,
  all_assets.display_name,
  all_assets.status,
  all_assets.created_at_source,
  all_assets.updated_at_source,
  all_assets.raw_json,
  ctx.run_id,
  ctx.now_ts,
  ctx.run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint,
  ctx.now_ts
FROM all_assets
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

-- Programmatic access owners.
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v2),
  gh_assets AS (
    SELECT external_id, row_number() OVER (ORDER BY external_id)::int AS rn
    FROM app_assets
    WHERE source_kind = 'github'
      AND source_name = (SELECT github_org FROM ctx)
      AND asset_kind = 'github_app_installation'
      AND expired_at IS NULL
  ),
  entra_apps AS (
    SELECT external_id, row_number() OVER (ORDER BY external_id)::int AS rn
    FROM app_assets
    WHERE source_kind = 'entra'
      AND source_name = (SELECT entra_tenant_id FROM ctx)
      AND asset_kind = 'entra_application'
      AND expired_at IS NULL
  ),
  entra_sps AS (
    SELECT external_id, row_number() OVER (ORDER BY external_id)::int AS rn
    FROM app_assets
    WHERE source_kind = 'entra'
      AND source_name = (SELECT entra_tenant_id FROM ctx)
      AND asset_kind = 'entra_service_principal'
      AND expired_at IS NULL
  ),
  owner_rows AS (
    SELECT
      'github'::text AS source_kind,
      (SELECT github_org FROM ctx)::text AS source_name,
      'github_app_installation'::text AS asset_kind,
      gh_assets.external_id AS asset_external_id,
      'github_user'::text AS owner_kind,
      format('demo-user%s', lpad((((gh_assets.rn + offs - 1) % 220) + 1)::text, 3, '0')) AS owner_external_id,
      format('Demo User %s', lpad((((gh_assets.rn + offs - 1) % 220) + 1)::text, 3, '0')) AS owner_display_name,
      CASE
        WHEN (((gh_assets.rn + offs - 1) % 220) + 1) <= 180
          THEN format('demo.user%s@example.com', lpad((((gh_assets.rn + offs - 1) % 220) + 1)::text, 3, '0'))
        ELSE ''
      END AS owner_email,
      jsonb_build_object('role', CASE WHEN offs = 1 THEN 'maintainer' ELSE 'security-reviewer' END, 'expansion', true) AS raw_json
    FROM gh_assets
    CROSS JOIN generate_series(1, 2) AS offs
    WHERE NOT ((gh_assets.rn % 3) = 0 AND offs = 2)

    UNION ALL

    SELECT
      'entra',
      (SELECT entra_tenant_id FROM ctx),
      'entra_application',
      entra_apps.external_id,
      'entra_user',
      format('demo.user%s@example.com', lpad((((entra_apps.rn + offs * 7 - 1) % 220) + 1)::text, 3, '0')),
      format('Demo Owner %s', lpad((((entra_apps.rn + offs * 7 - 1) % 220) + 1)::text, 3, '0')),
      format('demo.user%s@example.com', lpad((((entra_apps.rn + offs * 7 - 1) % 220) + 1)::text, 3, '0')),
      jsonb_build_object('role', CASE WHEN offs = 1 THEN 'owner' ELSE 'backup-owner' END, 'expansion', true)
    FROM entra_apps
    CROSS JOIN generate_series(1, 2) AS offs
    WHERE NOT ((entra_apps.rn % 4) = 0 AND offs = 2)

    UNION ALL

    SELECT
      'entra',
      (SELECT entra_tenant_id FROM ctx),
      'entra_service_principal',
      entra_sps.external_id,
      'entra_user',
      format('demo.user%s@example.com', lpad((((entra_sps.rn + 29) % 220) + 1)::text, 3, '0')),
      format('Demo Owner %s', lpad((((entra_sps.rn + 29) % 220) + 1)::text, 3, '0')),
      format('demo.user%s@example.com', lpad((((entra_sps.rn + 29) % 220) + 1)::text, 3, '0')),
      jsonb_build_object('role', 'owner', 'expansion', true)
    FROM entra_sps
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
  app_assets.id,
  owner_rows.owner_kind,
  owner_rows.owner_external_id,
  owner_rows.owner_display_name,
  owner_rows.owner_email,
  owner_rows.raw_json,
  ctx.run_id,
  ctx.now_ts,
  ctx.run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint,
  ctx.now_ts
FROM owner_rows
JOIN app_assets
  ON app_assets.source_kind = owner_rows.source_kind
  AND app_assets.source_name = owner_rows.source_name
  AND app_assets.asset_kind = owner_rows.asset_kind
  AND app_assets.external_id = owner_rows.asset_external_id
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

-- Programmatic access credentials.
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v2),
  gh_assets AS (
    SELECT external_id, row_number() OVER (ORDER BY external_id)::int AS rn
    FROM app_assets
    WHERE source_kind = 'github'
      AND source_name = (SELECT github_org FROM ctx)
      AND asset_kind = 'github_app_installation'
      AND expired_at IS NULL
  ),
  entra_apps AS (
    SELECT external_id, row_number() OVER (ORDER BY external_id)::int AS rn
    FROM app_assets
    WHERE source_kind = 'entra'
      AND source_name = (SELECT entra_tenant_id FROM ctx)
      AND asset_kind = 'entra_application'
      AND expired_at IS NULL
  ),
  entra_sps AS (
    SELECT external_id, row_number() OVER (ORDER BY external_id)::int AS rn
    FROM app_assets
    WHERE source_kind = 'entra'
      AND source_name = (SELECT entra_tenant_id FROM ctx)
      AND asset_kind = 'entra_service_principal'
      AND expired_at IS NULL
  ),
  rows AS (
    SELECT
      'github'::text AS source_kind,
      (SELECT github_org FROM ctx)::text AS source_name,
      'app_asset'::text AS asset_ref_kind,
      gh_assets.external_id AS asset_ref_external_id,
      'github_deploy_key'::text AS credential_kind,
      format('gdk-%s', lpad(gh_assets.rn::text, 5, '0')) AS external_id,
      format('Deploy key %s', lpad(gh_assets.rn::text, 4, '0')) AS display_name,
      substr(md5('gdk:' || gh_assets.external_id), 1, 32) AS fingerprint,
      jsonb_build_object(
        'organization', (SELECT github_org FROM ctx),
        'installation_id', gh_assets.external_id,
        'read_only', ((gh_assets.rn % 2) = 0)
      ) AS scope_json,
      CASE
        WHEN (gh_assets.rn % 17) = 0 THEN 'inactive'
        WHEN (gh_assets.rn % 13) = 0 THEN 'revoked'
        ELSE 'active'
      END AS status,
      (SELECT now_ts FROM ctx) - make_interval(days => (gh_assets.rn % 365) + 5) AS created_at_source,
      CASE
        WHEN (gh_assets.rn % 9) = 0 THEN (SELECT now_ts FROM ctx) - make_interval(days => (gh_assets.rn % 20) + 1)
        WHEN (gh_assets.rn % 7) = 0 THEN (SELECT now_ts FROM ctx) + make_interval(days => (gh_assets.rn % 6) + 1)
        WHEN (gh_assets.rn % 5) = 0 THEN (SELECT now_ts FROM ctx) + make_interval(days => (gh_assets.rn % 20) + 10)
        ELSE NULL::timestamptz
      END AS expires_at_source,
      CASE
        WHEN (gh_assets.rn % 4) = 0 THEN (SELECT now_ts FROM ctx) - make_interval(days => (gh_assets.rn % 160) + 95)
        ELSE (SELECT now_ts FROM ctx) - make_interval(days => (gh_assets.rn % 45) + 1)
      END AS last_used_at_source,
      CASE WHEN (gh_assets.rn % 10) = 0 THEN '' ELSE 'github_user' END AS created_by_kind,
      CASE WHEN (gh_assets.rn % 10) = 0 THEN '' ELSE format('demo-user%s', lpad((((gh_assets.rn - 1) % 220) + 1)::text, 3, '0')) END AS created_by_external_id,
      CASE WHEN (gh_assets.rn % 10) = 0 THEN '' ELSE format('Demo User %s', lpad((((gh_assets.rn - 1) % 220) + 1)::text, 3, '0')) END AS created_by_display_name,
      CASE WHEN (gh_assets.rn % 6) = 0 THEN '' ELSE 'github_user' END AS approved_by_kind,
      CASE WHEN (gh_assets.rn % 6) = 0 THEN '' ELSE format('demo-user%s', lpad((((gh_assets.rn + 17 - 1) % 220) + 1)::text, 3, '0')) END AS approved_by_external_id,
      CASE WHEN (gh_assets.rn % 6) = 0 THEN '' ELSE format('Demo User %s', lpad((((gh_assets.rn + 17 - 1) % 220) + 1)::text, 3, '0')) END AS approved_by_display_name,
      jsonb_build_object('dataset', 'expanded_demo', 'category', 'deploy_key') AS raw_json
    FROM gh_assets

    UNION ALL

    SELECT
      'github',
      (SELECT github_org FROM ctx),
      'app_asset',
      gh_assets.external_id,
      'github_pat_request',
      format('gpr-%s', lpad(gh_assets.rn::text, 5, '0')),
      format('PAT Request %s', lpad(gh_assets.rn::text, 4, '0')),
      substr(md5('gpr:' || gh_assets.external_id), 1, 32),
      jsonb_build_object('organization', (SELECT github_org FROM ctx), 'repository_selection', 'selected'),
      CASE WHEN (gh_assets.rn % 8) = 0 THEN 'approved' WHEN (gh_assets.rn % 5) = 0 THEN 'rejected' ELSE 'pending_approval' END,
      (SELECT now_ts FROM ctx) - make_interval(days => (gh_assets.rn % 120) + 2),
      CASE
        WHEN (gh_assets.rn % 10) = 0 THEN (SELECT now_ts FROM ctx) - make_interval(days => (gh_assets.rn % 12) + 1)
        ELSE (SELECT now_ts FROM ctx) + make_interval(days => (gh_assets.rn % 40) + 3)
      END,
      (SELECT now_ts FROM ctx) - make_interval(days => (gh_assets.rn % 110) + 1),
      'github_user',
      format('demo-user%s', lpad((((gh_assets.rn + 5 - 1) % 220) + 1)::text, 3, '0')),
      format('Demo User %s', lpad((((gh_assets.rn + 5 - 1) % 220) + 1)::text, 3, '0')),
      CASE WHEN (gh_assets.rn % 4) = 0 THEN '' ELSE 'github_user' END,
      CASE WHEN (gh_assets.rn % 4) = 0 THEN '' ELSE format('demo-user%s', lpad((((gh_assets.rn + 39 - 1) % 220) + 1)::text, 3, '0')) END,
      CASE WHEN (gh_assets.rn % 4) = 0 THEN '' ELSE format('Demo User %s', lpad((((gh_assets.rn + 39 - 1) % 220) + 1)::text, 3, '0')) END,
      jsonb_build_object('dataset', 'expanded_demo', 'category', 'pat_request')
    FROM gh_assets
    WHERE (gh_assets.rn % 3) = 0

    UNION ALL

    SELECT
      'github',
      (SELECT github_org FROM ctx),
      'app_asset',
      gh_assets.external_id,
      'github_pat_fine_grained',
      format('gpat-%s', lpad(gh_assets.rn::text, 5, '0')),
      format('Fine-grained PAT %s', lpad(gh_assets.rn::text, 4, '0')),
      substr(md5('gpat:' || gh_assets.external_id), 1, 32),
      jsonb_build_object('organization', (SELECT github_org FROM ctx), 'repository_selection', 'all'),
      CASE WHEN (gh_assets.rn % 11) = 0 THEN 'revoked' WHEN (gh_assets.rn % 9) = 0 THEN 'expired' ELSE 'active' END,
      (SELECT now_ts FROM ctx) - make_interval(days => (gh_assets.rn % 190) + 10),
      CASE
        WHEN (gh_assets.rn % 11) = 0 THEN (SELECT now_ts FROM ctx) - make_interval(days => (gh_assets.rn % 20) + 2)
        WHEN (gh_assets.rn % 7) = 0 THEN (SELECT now_ts FROM ctx) + make_interval(days => (gh_assets.rn % 5) + 1)
        ELSE (SELECT now_ts FROM ctx) + make_interval(days => (gh_assets.rn % 120) + 30)
      END,
      CASE
        WHEN (gh_assets.rn % 6) = 0 THEN (SELECT now_ts FROM ctx) - make_interval(days => (gh_assets.rn % 220) + 120)
        ELSE (SELECT now_ts FROM ctx) - make_interval(days => (gh_assets.rn % 30) + 1)
      END,
      CASE WHEN (gh_assets.rn % 14) = 0 THEN '' ELSE 'github_user' END,
      CASE WHEN (gh_assets.rn % 14) = 0 THEN '' ELSE format('demo-user%s', lpad((((gh_assets.rn + 11 - 1) % 220) + 1)::text, 3, '0')) END,
      CASE WHEN (gh_assets.rn % 14) = 0 THEN '' ELSE format('Demo User %s', lpad((((gh_assets.rn + 11 - 1) % 220) + 1)::text, 3, '0')) END,
      CASE WHEN (gh_assets.rn % 9) = 0 THEN '' ELSE 'github_user' END,
      CASE WHEN (gh_assets.rn % 9) = 0 THEN '' ELSE format('demo-user%s', lpad((((gh_assets.rn + 51 - 1) % 220) + 1)::text, 3, '0')) END,
      CASE WHEN (gh_assets.rn % 9) = 0 THEN '' ELSE format('Demo User %s', lpad((((gh_assets.rn + 51 - 1) % 220) + 1)::text, 3, '0')) END,
      jsonb_build_object('dataset', 'expanded_demo', 'category', 'pat_fine_grained')
    FROM gh_assets
    WHERE (gh_assets.rn % 2) = 0

    UNION ALL

    SELECT
      'entra',
      (SELECT entra_tenant_id FROM ctx),
      'app_asset',
      format('entra_application:%s', entra_apps.external_id),
      'entra_client_secret',
      format('ecs-app-%s', lpad(entra_apps.rn::text, 5, '0')),
      format('Client secret (app) %s', lpad(entra_apps.rn::text, 4, '0')),
      substr(md5('ecs-app:' || entra_apps.external_id), 1, 32),
      jsonb_build_object('asset_kind', 'entra_application', 'asset_external_id', entra_apps.external_id),
      CASE WHEN (entra_apps.rn % 12) = 0 THEN 'expired' WHEN (entra_apps.rn % 7) = 0 THEN 'inactive' ELSE 'active' END,
      (SELECT now_ts FROM ctx) - make_interval(days => (entra_apps.rn % 730) + 20),
      CASE
        WHEN (entra_apps.rn % 12) = 0 THEN (SELECT now_ts FROM ctx) - make_interval(days => (entra_apps.rn % 40) + 1)
        WHEN (entra_apps.rn % 8) = 0 THEN (SELECT now_ts FROM ctx) + make_interval(days => (entra_apps.rn % 6) + 1)
        ELSE (SELECT now_ts FROM ctx) + make_interval(days => (entra_apps.rn % 180) + 20)
      END,
      CASE
        WHEN (entra_apps.rn % 5) = 0 THEN (SELECT now_ts FROM ctx) - make_interval(days => (entra_apps.rn % 180) + 100)
        ELSE (SELECT now_ts FROM ctx) - make_interval(days => (entra_apps.rn % 50) + 1)
      END,
      CASE WHEN (entra_apps.rn % 9) = 0 THEN '' ELSE 'entra_user' END,
      CASE WHEN (entra_apps.rn % 9) = 0 THEN '' ELSE format('demo.user%s@example.com', lpad((((entra_apps.rn + 3 - 1) % 220) + 1)::text, 3, '0')) END,
      CASE WHEN (entra_apps.rn % 9) = 0 THEN '' ELSE format('Demo User %s', lpad((((entra_apps.rn + 3 - 1) % 220) + 1)::text, 3, '0')) END,
      CASE WHEN (entra_apps.rn % 4) = 0 THEN '' ELSE 'entra_user' END,
      CASE WHEN (entra_apps.rn % 4) = 0 THEN '' ELSE format('demo.user%s@example.com', lpad((((entra_apps.rn + 19 - 1) % 220) + 1)::text, 3, '0')) END,
      CASE WHEN (entra_apps.rn % 4) = 0 THEN '' ELSE format('Demo User %s', lpad((((entra_apps.rn + 19 - 1) % 220) + 1)::text, 3, '0')) END,
      jsonb_build_object('dataset', 'expanded_demo', 'category', 'entra_client_secret_application')
    FROM entra_apps

    UNION ALL

    SELECT
      'entra',
      (SELECT entra_tenant_id FROM ctx),
      'app_asset',
      format('entra_application:%s', entra_apps.external_id),
      'entra_certificate',
      format('ecc-app-%s', lpad(entra_apps.rn::text, 5, '0')),
      format('Certificate (app) %s', lpad(entra_apps.rn::text, 4, '0')),
      substr(md5('ecc-app:' || entra_apps.external_id), 1, 32),
      jsonb_build_object('asset_kind', 'entra_application', 'asset_external_id', entra_apps.external_id, 'type', 'certificate'),
      CASE WHEN (entra_apps.rn % 10) = 0 THEN 'expired' ELSE 'active' END,
      (SELECT now_ts FROM ctx) - make_interval(days => (entra_apps.rn % 820) + 25),
      CASE
        WHEN (entra_apps.rn % 10) = 0 THEN (SELECT now_ts FROM ctx) - make_interval(days => (entra_apps.rn % 35) + 1)
        WHEN (entra_apps.rn % 6) = 0 THEN (SELECT now_ts FROM ctx) + make_interval(days => (entra_apps.rn % 7) + 1)
        ELSE (SELECT now_ts FROM ctx) + make_interval(days => (entra_apps.rn % 365) + 60)
      END,
      (SELECT now_ts FROM ctx) - make_interval(days => (entra_apps.rn % 70) + 1),
      'entra_user',
      format('demo.user%s@example.com', lpad((((entra_apps.rn + 7 - 1) % 220) + 1)::text, 3, '0')),
      format('Demo User %s', lpad((((entra_apps.rn + 7 - 1) % 220) + 1)::text, 3, '0')),
      'entra_user',
      format('demo.user%s@example.com', lpad((((entra_apps.rn + 31 - 1) % 220) + 1)::text, 3, '0')),
      format('Demo User %s', lpad((((entra_apps.rn + 31 - 1) % 220) + 1)::text, 3, '0')),
      jsonb_build_object('dataset', 'expanded_demo', 'category', 'entra_certificate_application')
    FROM entra_apps
    WHERE (entra_apps.rn % 2) = 0

    UNION ALL

    SELECT
      'entra',
      (SELECT entra_tenant_id FROM ctx),
      'app_asset',
      format('entra_service_principal:%s', entra_sps.external_id),
      'entra_client_secret',
      format('ecs-sp-%s', lpad(entra_sps.rn::text, 5, '0')),
      format('Client secret (sp) %s', lpad(entra_sps.rn::text, 4, '0')),
      substr(md5('ecs-sp:' || entra_sps.external_id), 1, 32),
      jsonb_build_object('asset_kind', 'entra_service_principal', 'asset_external_id', entra_sps.external_id),
      CASE WHEN (entra_sps.rn % 14) = 0 THEN 'expired' WHEN (entra_sps.rn % 8) = 0 THEN 'inactive' ELSE 'active' END,
      (SELECT now_ts FROM ctx) - make_interval(days => (entra_sps.rn % 540) + 10),
      CASE
        WHEN (entra_sps.rn % 14) = 0 THEN (SELECT now_ts FROM ctx) - make_interval(days => (entra_sps.rn % 30) + 1)
        WHEN (entra_sps.rn % 9) = 0 THEN (SELECT now_ts FROM ctx) + make_interval(days => (entra_sps.rn % 5) + 1)
        ELSE (SELECT now_ts FROM ctx) + make_interval(days => (entra_sps.rn % 240) + 15)
      END,
      CASE
        WHEN (entra_sps.rn % 6) = 0 THEN (SELECT now_ts FROM ctx) - make_interval(days => (entra_sps.rn % 230) + 120)
        ELSE (SELECT now_ts FROM ctx) - make_interval(days => (entra_sps.rn % 55) + 1)
      END,
      CASE WHEN (entra_sps.rn % 11) = 0 THEN '' ELSE 'entra_user' END,
      CASE WHEN (entra_sps.rn % 11) = 0 THEN '' ELSE format('demo.user%s@example.com', lpad((((entra_sps.rn + 13 - 1) % 220) + 1)::text, 3, '0')) END,
      CASE WHEN (entra_sps.rn % 11) = 0 THEN '' ELSE format('Demo User %s', lpad((((entra_sps.rn + 13 - 1) % 220) + 1)::text, 3, '0')) END,
      CASE WHEN (entra_sps.rn % 5) = 0 THEN '' ELSE 'entra_user' END,
      CASE WHEN (entra_sps.rn % 5) = 0 THEN '' ELSE format('demo.user%s@example.com', lpad((((entra_sps.rn + 41 - 1) % 220) + 1)::text, 3, '0')) END,
      CASE WHEN (entra_sps.rn % 5) = 0 THEN '' ELSE format('Demo User %s', lpad((((entra_sps.rn + 41 - 1) % 220) + 1)::text, 3, '0')) END,
      jsonb_build_object('dataset', 'expanded_demo', 'category', 'entra_client_secret_service_principal')
    FROM entra_sps

    UNION ALL

    SELECT
      'entra',
      (SELECT entra_tenant_id FROM ctx),
      'app_asset',
      format('entra_service_principal:%s', entra_sps.external_id),
      'entra_certificate',
      format('ecc-sp-%s', lpad(entra_sps.rn::text, 5, '0')),
      format('Certificate (sp) %s', lpad(entra_sps.rn::text, 4, '0')),
      substr(md5('ecc-sp:' || entra_sps.external_id), 1, 32),
      jsonb_build_object('asset_kind', 'entra_service_principal', 'asset_external_id', entra_sps.external_id, 'type', 'certificate'),
      CASE WHEN (entra_sps.rn % 15) = 0 THEN 'expired' ELSE 'active' END,
      (SELECT now_ts FROM ctx) - make_interval(days => (entra_sps.rn % 620) + 15),
      CASE
        WHEN (entra_sps.rn % 15) = 0 THEN (SELECT now_ts FROM ctx) - make_interval(days => (entra_sps.rn % 20) + 1)
        WHEN (entra_sps.rn % 7) = 0 THEN (SELECT now_ts FROM ctx) + make_interval(days => (entra_sps.rn % 8) + 1)
        ELSE (SELECT now_ts FROM ctx) + make_interval(days => (entra_sps.rn % 320) + 40)
      END,
      (SELECT now_ts FROM ctx) - make_interval(days => (entra_sps.rn % 80) + 1),
      'entra_user',
      format('demo.user%s@example.com', lpad((((entra_sps.rn + 23 - 1) % 220) + 1)::text, 3, '0')),
      format('Demo User %s', lpad((((entra_sps.rn + 23 - 1) % 220) + 1)::text, 3, '0')),
      'entra_user',
      format('demo.user%s@example.com', lpad((((entra_sps.rn + 57 - 1) % 220) + 1)::text, 3, '0')),
      format('Demo User %s', lpad((((entra_sps.rn + 57 - 1) % 220) + 1)::text, 3, '0')),
      jsonb_build_object('dataset', 'expanded_demo', 'category', 'entra_certificate_service_principal')
    FROM entra_sps
    WHERE (entra_sps.rn % 3) = 0
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
  rows.source_kind,
  rows.source_name,
  rows.asset_ref_kind,
  rows.asset_ref_external_id,
  rows.credential_kind,
  rows.external_id,
  rows.display_name,
  rows.fingerprint,
  rows.scope_json,
  rows.status,
  rows.created_at_source,
  rows.expires_at_source,
  rows.last_used_at_source,
  rows.created_by_kind,
  rows.created_by_external_id,
  rows.created_by_display_name,
  rows.approved_by_kind,
  rows.approved_by_external_id,
  rows.approved_by_display_name,
  rows.raw_json,
  ctx.run_id,
  ctx.now_ts,
  ctx.run_id,
  ctx.now_ts,
  NULL::timestamptz,
  NULL::bigint,
  ctx.now_ts
FROM rows
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

-- Programmatic access credential audit events.
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v2),
  seeded_credentials AS (
    SELECT
      credential_artifacts.source_kind,
      credential_artifacts.source_name,
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
      CASE
        WHEN credential_artifacts.source_kind = 'entra' AND position(':' IN credential_artifacts.asset_ref_external_id) > 0
          THEN split_part(credential_artifacts.asset_ref_external_id, ':', 1)
        WHEN credential_artifacts.source_kind = 'github'
          THEN 'github_app_installation'
        ELSE 'app_asset'
      END AS target_kind,
      CASE
        WHEN credential_artifacts.source_kind = 'entra' AND position(':' IN credential_artifacts.asset_ref_external_id) > 0
          THEN split_part(credential_artifacts.asset_ref_external_id, ':', 2)
        ELSE credential_artifacts.asset_ref_external_id
      END AS target_external_id
    FROM credential_artifacts
    WHERE
      (
        credential_artifacts.source_kind = 'github'
        AND credential_artifacts.source_name = (SELECT github_org FROM ctx)
      )
      OR (
        credential_artifacts.source_kind = 'entra'
        AND credential_artifacts.source_name = (SELECT entra_tenant_id FROM ctx)
      )
  ),
  credential_targets AS (
    SELECT
      seeded_credentials.*,
      COALESCE(app_assets.display_name, seeded_credentials.target_external_id) AS target_display_name
    FROM seeded_credentials
    LEFT JOIN app_assets
      ON app_assets.source_kind = seeded_credentials.source_kind
      AND app_assets.source_name = seeded_credentials.source_name
      AND app_assets.asset_kind = seeded_credentials.target_kind
      AND app_assets.external_id = seeded_credentials.target_external_id
      AND app_assets.expired_at IS NULL
  ),
  events AS (
    SELECT
      credential_targets.source_kind,
      credential_targets.source_name,
      format('demo2:create:%s:%s:%s', credential_targets.source_kind, credential_targets.credential_kind, credential_targets.external_id) AS event_external_id,
      'credential.created'::text AS event_type,
      COALESCE(credential_targets.created_at_source, (SELECT now_ts FROM ctx) - interval '30 days') AS event_time,
      COALESCE(NULLIF(credential_targets.created_by_kind, ''), 'system') AS actor_kind,
      COALESCE(NULLIF(credential_targets.created_by_external_id, ''), 'seed-bot@demo.local') AS actor_external_id,
      COALESCE(NULLIF(credential_targets.created_by_display_name, ''), NULLIF(credential_targets.created_by_external_id, ''), 'Demo Seed Bot') AS actor_display_name,
      credential_targets.target_kind,
      credential_targets.target_external_id,
      credential_targets.target_display_name,
      credential_targets.credential_kind,
      credential_targets.external_id AS credential_external_id,
      jsonb_build_object('dataset', 'expanded_demo', 'phase', 'created') AS raw_json
    FROM credential_targets

    UNION ALL

    SELECT
      credential_targets.source_kind,
      credential_targets.source_name,
      format('demo2:approved:%s:%s:%s', credential_targets.source_kind, credential_targets.credential_kind, credential_targets.external_id),
      'credential.approved',
      COALESCE(credential_targets.created_at_source, (SELECT now_ts FROM ctx) - interval '30 days') + interval '1 day',
      COALESCE(NULLIF(credential_targets.approved_by_kind, ''), 'system'),
      COALESCE(NULLIF(credential_targets.approved_by_external_id, ''), 'change-review@demo.local'),
      COALESCE(NULLIF(credential_targets.approved_by_display_name, ''), NULLIF(credential_targets.approved_by_external_id, ''), 'Demo Change Review'),
      credential_targets.target_kind,
      credential_targets.target_external_id,
      credential_targets.target_display_name,
      credential_targets.credential_kind,
      credential_targets.external_id,
      jsonb_build_object('dataset', 'expanded_demo', 'phase', 'approved')
    FROM credential_targets
    WHERE credential_targets.approved_by_external_id <> ''

    UNION ALL

    SELECT
      credential_targets.source_kind,
      credential_targets.source_name,
      format('demo2:state:%s:%s:%s', credential_targets.source_kind, credential_targets.credential_kind, credential_targets.external_id),
      CASE
        WHEN lower(credential_targets.status) = 'revoked' THEN 'credential.revoked'
        WHEN credential_targets.expires_at_source IS NOT NULL AND credential_targets.expires_at_source < (SELECT now_ts FROM ctx) THEN 'credential.expired'
        ELSE 'credential.used'
      END,
      COALESCE(
        CASE
          WHEN lower(credential_targets.status) = 'revoked' THEN credential_targets.expires_at_source
          WHEN credential_targets.expires_at_source IS NOT NULL AND credential_targets.expires_at_source < (SELECT now_ts FROM ctx) THEN credential_targets.expires_at_source
          ELSE credential_targets.last_used_at_source
        END,
        (SELECT now_ts FROM ctx) - interval '5 days'
      ),
      COALESCE(NULLIF(credential_targets.created_by_kind, ''), 'system'),
      COALESCE(NULLIF(credential_targets.created_by_external_id, ''), 'seed-bot@demo.local'),
      COALESCE(NULLIF(credential_targets.created_by_display_name, ''), NULLIF(credential_targets.created_by_external_id, ''), 'Demo Seed Bot'),
      credential_targets.target_kind,
      credential_targets.target_external_id,
      credential_targets.target_display_name,
      credential_targets.credential_kind,
      credential_targets.external_id,
      jsonb_build_object('dataset', 'expanded_demo', 'phase', 'state-change', 'status', credential_targets.status)
    FROM credential_targets
    WHERE
      lower(credential_targets.status) = 'revoked'
      OR (credential_targets.expires_at_source IS NOT NULL AND credential_targets.expires_at_source < (SELECT now_ts FROM ctx))
      OR credential_targets.last_used_at_source IS NOT NULL
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
  events.source_kind,
  events.source_name,
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

-- ------------------------------------------------------------
-- Expand seeded findings volume so demo has denser controls coverage.
-- ------------------------------------------------------------
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v2),
  rs AS (
    SELECT id
    FROM rulesets
    WHERE key = 'cis.okta.idaas_stig.v2'
    LIMIT 1
  ),
  picked AS (
    SELECT
      rules.id AS rule_id,
      rules.key AS rule_key,
      row_number() OVER (ORDER BY rules.key) AS rn
    FROM rules
    JOIN rs ON rs.id = rules.ruleset_id
    WHERE rules.is_active = true
    ORDER BY rules.key
    LIMIT 80
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
  picked.rule_id,
  'connector_instance',
  'okta',
  ctx.okta_domain,
  CASE
    WHEN (picked.rn % 7) = 0 THEN 'error'
    WHEN (picked.rn % 2) = 0 THEN 'fail'
    ELSE 'pass'
  END,
  ctx.now_ts,
  ctx.run_id,
  CASE
    WHEN (picked.rn % 7) = 0 THEN 'Demo seeded: evaluation error (expanded)'
    WHEN (picked.rn % 2) = 0 THEN 'Demo seeded: failing control (expanded)'
    ELSE 'Demo seeded: passing control (expanded)'
  END,
  jsonb_build_object(
    'schema_version', 1,
    'rule', jsonb_build_object(
      'ruleset_key', 'cis.okta.idaas_stig.v2',
      'rule_key', picked.rule_key
    ),
    'check', jsonb_build_object('type', 'demo.seed.expanded'),
    'result', jsonb_build_object(
      'status', CASE
        WHEN (picked.rn % 7) = 0 THEN 'error'
        WHEN (picked.rn % 2) = 0 THEN 'fail'
        ELSE 'pass'
      END
    ),
    'selection', jsonb_build_object(
      'total', 220,
      'selected', CASE WHEN (picked.rn % 2) = 0 THEN 35 ELSE 220 END
    ),
    'violations', CASE
      WHEN (picked.rn % 2) = 0 THEN jsonb_build_array(
        jsonb_build_object('resource_id', format('demo:resource/%s', picked.rn), 'display', format('Expanded demo violation #%s', picked.rn)),
        jsonb_build_object('resource_id', format('demo:resource/%s', picked.rn + 1000), 'display', format('Expanded demo violation #%s', picked.rn + 1000))
      )
      ELSE '[]'::jsonb
    END,
    'violations_truncated', false,
    'demo', true,
    'expanded', true,
    'seeded_at', ctx.now_ts
  ),
  CASE WHEN (picked.rn % 2) = 0 THEN ARRAY[format('demo:resource/%s', picked.rn)]::text[] ELSE '{}'::text[] END,
  CASE WHEN (picked.rn % 7) = 0 THEN 'upstream_api' ELSE '' END
FROM picked
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

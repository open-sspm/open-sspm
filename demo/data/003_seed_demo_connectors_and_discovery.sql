-- Demo breadth seed: connector coverage, source-specific sync runs, and SaaS discovery.

BEGIN;

CREATE TEMP TABLE demo_seed_ctx_v3 (
  now_ts TIMESTAMPTZ NOT NULL,
  okta_domain TEXT NOT NULL,
  github_org TEXT NOT NULL,
  datadog_site TEXT NOT NULL,
  entra_tenant_id TEXT NOT NULL,
  google_customer_id TEXT NOT NULL,
  google_primary_domain TEXT NOT NULL,
  google_admin_email TEXT NOT NULL,
  aws_region TEXT NOT NULL,
  vault_source_name TEXT NOT NULL,
  vault_address TEXT NOT NULL,
  okta_run_id BIGINT NOT NULL,
  okta_discovery_run_id BIGINT NOT NULL,
  entra_run_id BIGINT NOT NULL,
  entra_discovery_run_id BIGINT NOT NULL,
  google_run_id BIGINT NOT NULL,
  google_discovery_run_id BIGINT NOT NULL,
  github_run_id BIGINT NOT NULL,
  datadog_run_id BIGINT NOT NULL,
  aws_run_id BIGINT NOT NULL,
  vault_run_id BIGINT NOT NULL
) ON COMMIT DROP;

TRUNCATE demo_seed_ctx_v3;

WITH
  now_ctx AS (
    SELECT now() AS now_ts
  ),
  okta_run AS (
    INSERT INTO sync_runs (source_kind, source_name, status, started_at, finished_at, message, stats, error_kind)
    SELECT
      'okta',
      'demo.okta.example.com',
      'success',
      now_ts - interval '10 minutes',
      now_ts - interval '5 minutes',
      'seeded demo Okta sync',
      '{}'::jsonb,
      ''
    FROM now_ctx
    RETURNING id
  ),
  okta_discovery_run AS (
    INSERT INTO sync_runs (source_kind, source_name, status, started_at, finished_at, message, stats, error_kind)
    SELECT
      'okta_discovery',
      'demo.okta.example.com',
      'success',
      now_ts - interval '8 minutes',
      now_ts - interval '3 minutes',
      'seeded demo Okta discovery sync',
      '{}'::jsonb,
      ''
    FROM now_ctx
    RETURNING id
  ),
  entra_run AS (
    INSERT INTO sync_runs (source_kind, source_name, status, started_at, finished_at, message, stats, error_kind)
    SELECT
      'entra',
      '00000000-0000-4000-8000-000000000001',
      'success',
      now_ts - interval '3 days 10 minutes',
      now_ts - interval '3 days',
      'seeded demo Entra sync',
      '{}'::jsonb,
      ''
    FROM now_ctx
    RETURNING id
  ),
  entra_discovery_run AS (
    INSERT INTO sync_runs (source_kind, source_name, status, started_at, finished_at, message, stats, error_kind)
    SELECT
      'entra_discovery',
      '00000000-0000-4000-8000-000000000001',
      'success',
      now_ts - interval '3 days 8 minutes',
      now_ts - interval '3 days 2 minutes',
      'seeded demo Entra discovery sync',
      '{}'::jsonb,
      ''
    FROM now_ctx
    RETURNING id
  ),
  google_run AS (
    INSERT INTO sync_runs (source_kind, source_name, status, started_at, finished_at, message, stats, error_kind)
    SELECT
      'google_workspace',
      'C0123',
      'success',
      now_ts - interval '9 minutes',
      now_ts - interval '4 minutes',
      'seeded demo Google Workspace sync',
      '{}'::jsonb,
      ''
    FROM now_ctx
    RETURNING id
  ),
  google_discovery_run AS (
    INSERT INTO sync_runs (source_kind, source_name, status, started_at, finished_at, message, stats, error_kind)
    SELECT
      'google_workspace_discovery',
      'C0123',
      'success',
      now_ts - interval '7 minutes',
      now_ts - interval '2 minutes',
      'seeded demo Google Workspace discovery sync',
      '{}'::jsonb,
      ''
    FROM now_ctx
    RETURNING id
  ),
  github_run AS (
    INSERT INTO sync_runs (source_kind, source_name, status, started_at, finished_at, message, stats, error_kind)
    SELECT
      'github',
      'open-sspm-demo',
      'success',
      now_ts - interval '11 minutes',
      now_ts - interval '6 minutes',
      'seeded demo GitHub sync',
      '{}'::jsonb,
      ''
    FROM now_ctx
    RETURNING id
  ),
  datadog_run AS (
    INSERT INTO sync_runs (source_kind, source_name, status, started_at, finished_at, message, stats, error_kind)
    SELECT
      'datadog',
      'datadoghq.com',
      'success',
      now_ts - interval '12 minutes',
      now_ts - interval '7 minutes',
      'seeded demo Datadog sync',
      '{}'::jsonb,
      ''
    FROM now_ctx
    RETURNING id
  ),
  aws_run AS (
    INSERT INTO sync_runs (source_kind, source_name, status, started_at, finished_at, message, stats, error_kind)
    SELECT
      'aws',
      'eu-west-1',
      'success',
      now_ts - interval '13 minutes',
      now_ts - interval '8 minutes',
      'seeded demo AWS Identity Center sync',
      '{}'::jsonb,
      ''
    FROM now_ctx
    RETURNING id
  ),
  vault_run AS (
    INSERT INTO sync_runs (source_kind, source_name, status, started_at, finished_at, message, stats, error_kind)
    SELECT
      'vault',
      'demo-vault',
      'success',
      now_ts - interval '14 minutes',
      now_ts - interval '9 minutes',
      'seeded demo Vault sync',
      '{}'::jsonb,
      ''
    FROM now_ctx
    RETURNING id
  )
INSERT INTO demo_seed_ctx_v3 (
  now_ts,
  okta_domain,
  github_org,
  datadog_site,
  entra_tenant_id,
  google_customer_id,
  google_primary_domain,
  google_admin_email,
  aws_region,
  vault_source_name,
  vault_address,
  okta_run_id,
  okta_discovery_run_id,
  entra_run_id,
  entra_discovery_run_id,
  google_run_id,
  google_discovery_run_id,
  github_run_id,
  datadog_run_id,
  aws_run_id,
  vault_run_id
)
SELECT
  now_ts,
  'demo.okta.example.com',
  'open-sspm-demo',
  'datadoghq.com',
  '00000000-0000-4000-8000-000000000001',
  'C0123',
  'workspace.demo.example.com',
  'it-admin@workspace.demo.example.com',
  'eu-west-1',
  'demo-vault',
  'https://demo-vault.example.com',
  (SELECT id FROM okta_run),
  (SELECT id FROM okta_discovery_run),
  (SELECT id FROM entra_run),
  (SELECT id FROM entra_discovery_run),
  (SELECT id FROM google_run),
  (SELECT id FROM google_discovery_run),
  (SELECT id FROM github_run),
  (SELECT id FROM datadog_run),
  (SELECT id FROM aws_run),
  (SELECT id FROM vault_run)
FROM now_ctx;

-- Connector coverage for the full demo surface.
WITH ctx AS (SELECT * FROM demo_seed_ctx_v3)
INSERT INTO connector_configs (kind, enabled, config, updated_at)
VALUES
  (
    'okta',
    true,
    jsonb_build_object(
      'domain', (SELECT okta_domain FROM ctx),
      'discovery_enabled', true
    ),
    (SELECT now_ts FROM ctx)
  ),
  (
    'entra',
    true,
    jsonb_build_object(
      'tenant_id', (SELECT entra_tenant_id FROM ctx),
      'client_id', '00000000-0000-4000-8000-000000000002',
      'discovery_enabled', true
    ),
    (SELECT now_ts FROM ctx)
  ),
  (
    'google_workspace',
    true,
    jsonb_build_object(
      'customer_id', (SELECT google_customer_id FROM ctx),
      'primary_domain', (SELECT google_primary_domain FROM ctx),
      'delegated_admin_email', (SELECT google_admin_email FROM ctx),
      'auth_type', 'service_account_json',
      'discovery_enabled', true
    ),
    (SELECT now_ts FROM ctx)
  ),
  (
    'aws_identity_center',
    true,
    jsonb_build_object(
      'region', (SELECT aws_region FROM ctx),
      'name', '',
      'instance_arn', 'arn:aws:sso:::instance/ssoins-demo0001',
      'identity_store_id', 'd-demo0001',
      'auth_type', 'default_chain'
    ),
    (SELECT now_ts FROM ctx)
  ),
  (
    'vault',
    true,
    jsonb_build_object(
      'address', (SELECT vault_address FROM ctx),
      'name', (SELECT vault_source_name FROM ctx),
      'auth_type', 'token',
      'scan_auth_roles', true,
      'tls_skip_verify', false
    ),
    (SELECT now_ts FROM ctx)
  )
ON CONFLICT (kind) DO UPDATE SET
  enabled = EXCLUDED.enabled,
  config = EXCLUDED.config,
  updated_at = EXCLUDED.updated_at
;

WITH ctx AS (SELECT * FROM demo_seed_ctx_v3)
INSERT INTO connector_secrets (kind, secret_name, ciphertext, nonce, version, updated_at)
VALUES
  (
    'google_workspace',
    'service_account_json',
    decode('e72f5a41a23ee105dc318ca8f8c6350fd40146c46d3ab4bb58b7a710170482ba5f573c084ebfc620c921c94c45427fb5ce374e3cd406c434d5a55d9b89efdaf6202071b67c96a5d597d45ab8a4419c9a44e4378f924b3a29910a5a2350f296ae6f7da5229361a2f78fbcbc52641d2e61cedc64bc6c772e7730612072375d704656e8b70ce326adaba7ed8d2366895b94590711ff3a03254d329ba7f1020a3c74342f995abf4278f04a76569cade1641e3d8f13a6e479f16abeed6349d3052bfda585f095bf66036c55702b1f3a3d075d40ff464f', 'hex'),
    decode('112233445566778899aabbcc', 'hex'),
    1,
    (SELECT now_ts FROM ctx)
  ),
  (
    'vault',
    'token',
    decode('756d54eb88f043c62cd6bd913c77a656c50fffa408269b237b4bb6e2f09f5a13', 'hex'),
    decode('0d1e2f30415263748596a7b8', 'hex'),
    1,
    (SELECT now_ts FROM ctx)
  )
ON CONFLICT (kind, secret_name) DO UPDATE SET
  ciphertext = EXCLUDED.ciphertext,
  nonce = EXCLUDED.nonce,
  version = EXCLUDED.version,
  updated_at = EXCLUDED.updated_at
;

WITH ctx AS (SELECT * FROM demo_seed_ctx_v3)
INSERT INTO identity_source_settings (source_kind, source_name, is_authoritative, updated_at)
VALUES
  ('google_workspace', (SELECT google_customer_id FROM ctx), false, (SELECT now_ts FROM ctx)),
  ('aws', (SELECT aws_region FROM ctx), false, (SELECT now_ts FROM ctx)),
  ('vault', (SELECT vault_source_name FROM ctx), false, (SELECT now_ts FROM ctx))
ON CONFLICT (source_kind, source_name) DO UPDATE SET
  is_authoritative = EXCLUDED.is_authoritative,
  updated_at = EXCLUDED.updated_at
;

-- Discovery breadth: 24 apps with managed, no-binding, stale-binding, and hotspot scenarios.
WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v3),
  app_defs (
    app_ord,
    canonical_key,
    display_name,
    primary_domain,
    vendor_name,
    discovery_source_kind,
    source_app_id,
    source_app_name,
    source_app_domain,
    category,
    binding_connector_kind,
    signal_kind,
    actor_count,
    privileged_scope,
    governance_owner_email,
    governance_state,
    ticket_ref,
    business_criticality,
    data_classification,
    notes
  ) AS (
    VALUES
      ( 1, 'github-actions-control-plane', 'GitHub Actions Control Plane', 'actions.demo.example.com', 'GitHub', 'okta', 'okta-app-github-actions-control-plane', 'GitHub Actions Control Plane', 'actions.demo.example.com', 'managed', 'github', 'idp_sso', 6, false, '', '', '', 'unknown', 'unknown', ''),
      ( 2, 'datadog-ci-monitoring', 'Datadog CI Monitoring', 'ci-observability.demo.example.com', 'Datadog', 'okta', 'okta-app-datadog-ci-monitoring', 'Datadog CI Monitoring', 'ci-observability.demo.example.com', 'managed', 'datadog', 'idp_sso', 5, false, '', '', '', 'unknown', 'unknown', ''),
      ( 3, 'github-copilot-enterprise', 'GitHub Copilot Enterprise', 'copilot.demo.example.com', 'GitHub', 'google_workspace', 'gw-discovery-github-copilot-enterprise', 'GitHub Copilot Enterprise', 'copilot.demo.example.com', 'managed', 'github', 'oauth_grant', 7, false, '', '', '', 'unknown', 'unknown', ''),
      ( 4, 'datadog-incident-bridge', 'Datadog Incident Bridge', 'incident-bridge.demo.example.com', 'Datadog', 'google_workspace', 'gw-discovery-datadog-incident-bridge', 'Datadog Incident Bridge', 'incident-bridge.demo.example.com', 'managed', 'datadog', 'oauth_grant', 6, false, '', '', '', 'unknown', 'unknown', ''),
      ( 5, 'github-security-review-bot', 'GitHub Security Review Bot', 'security-review.demo.example.com', 'GitHub', 'okta', 'okta-app-github-security-review-bot', 'GitHub Security Review Bot', 'security-review.demo.example.com', 'managed', 'github', 'idp_sso', 5, false, '', '', '', 'unknown', 'unknown', ''),
      ( 6, 'datadog-synthetic-control', 'Datadog Synthetic Control', 'synthetics.demo.example.com', 'Datadog', 'google_workspace', 'gw-discovery-datadog-synthetic-control', 'Datadog Synthetic Control', 'synthetics.demo.example.com', 'managed', 'datadog', 'oauth_grant', 6, false, '', '', '', 'unknown', 'unknown', ''),
      ( 7, 'aws-console-federation', 'AWS Console Federation', 'aws-console.demo.example.com', 'Amazon', 'okta', 'okta-app-aws-console-federation', 'AWS Console Federation', 'aws-console.demo.example.com', 'managed', 'aws_identity_center', 'idp_sso', 6, false, '', '', '', 'unknown', 'unknown', ''),
      ( 8, 'aws-billing-workspace', 'AWS Billing Workspace', 'billing.demo.example.com', 'Amazon', 'google_workspace', 'gw-discovery-aws-billing-workspace', 'AWS Billing Workspace', 'billing.demo.example.com', 'managed', 'aws_identity_center', 'oauth_grant', 5, false, '', '', '', 'unknown', 'unknown', ''),
      ( 9, 'azure-reference-ops-portal', 'Azure Reference Ops Portal', 'ops-portal.demo.example.com', 'Microsoft', 'entra', 'entra-app-azure-reference-ops-portal', 'Azure Reference Ops Portal', 'ops-portal.demo.example.com', 'stale_binding', 'entra', 'idp_sso', 8, false, 'demo.user009@example.com', 'in_review', '', 'low', 'internal', 'Previous Entra mapping kept for the demo stale-sync scenario.'),
      (10, 'azure-contractors-workbench', 'Azure Contractors Workbench', 'contractors.demo.example.com', 'Microsoft', 'okta', 'okta-app-azure-contractors-workbench', 'Azure Contractors Workbench', 'contractors.demo.example.com', 'stale_binding', 'entra', 'idp_sso', 7, false, 'demo.user010@example.com', 'approved', '', 'low', 'public', 'Authorized but intentionally stale to show aged bindings.'),
      (11, 'azure-data-exchange', 'Azure Data Exchange', 'data-exchange.demo.example.com', 'Microsoft', 'google_workspace', 'gw-discovery-azure-data-exchange', 'Azure Data Exchange', 'data-exchange.demo.example.com', 'stale_binding', 'entra', 'oauth_grant', 7, false, 'demo.user011@example.com', 'in_review', '', 'medium', 'internal', 'App remains linked to a stale Entra sync for demo posture coverage.'),
      (12, 'azure-identity-admin-lite', 'Azure Identity Admin Lite', 'identity-admin.demo.example.com', 'Microsoft', 'entra', 'entra-app-azure-identity-admin-lite', 'Azure Identity Admin Lite', 'identity-admin.demo.example.com', 'stale_binding', 'entra', 'idp_sso', 6, false, 'demo.user012@example.com', 'approved', '', 'low', 'internal', 'Seeded as a stale Entra-bound discovery row.'),
      (13, 'notion-shadow-wiki', 'Notion Shadow Wiki', 'notion.demo.example.com', 'Notion', 'okta', 'okta-app-notion-shadow-wiki', 'Notion Shadow Wiki', 'notion.demo.example.com', 'no_binding', '', 'idp_sso', 4, false, 'demo.user013@example.com', 'approved', '', 'low', 'internal', 'Known but intentionally left unmanaged for the discovery story.'),
      (14, 'miro-brainstorm-lab', 'Miro Brainstorm Lab', 'miro.demo.example.com', 'Miro', 'google_workspace', 'gw-discovery-miro-brainstorm-lab', 'Miro Brainstorm Lab', 'miro.demo.example.com', 'no_binding', '', 'oauth_grant', 5, false, 'demo.user014@example.com', 'in_review', '', 'medium', 'confidential', 'OAuth-based app without a managed connector.'),
      (15, 'figma-freelance-studio', 'Figma Freelance Studio', 'figma.demo.example.com', 'Figma', 'okta', 'okta-app-figma-freelance-studio', 'Figma Freelance Studio', 'figma.demo.example.com', 'no_binding', '', 'idp_sso', 3, false, 'demo.user015@example.com', 'approved', '', 'low', 'internal', 'Seeded as unmanaged but owned.'),
      (16, 'linear-skunkworks', 'Linear Skunkworks', 'linear.demo.example.com', 'Linear', 'entra', 'entra-app-linear-skunkworks', 'Linear Skunkworks', 'linear.demo.example.com', 'no_binding', '', 'idp_sso', 6, false, 'demo.user016@example.com', 'approved', '', 'medium', 'internal', 'No binding on purpose to populate unmanaged discovery states.'),
      (17, 'slack-reference-grid', 'Slack Reference Grid', 'slack.demo.example.com', 'Slack', 'okta', 'okta-app-slack-reference-grid', 'Slack Reference Grid', 'slack.demo.example.com', 'no_binding', '', 'idp_sso', 4, false, 'demo.user017@example.com', 'in_review', '', 'medium', 'internal', 'Older org-level workspace left unmanaged.'),
      (18, 'atlassian-rogue-jira', 'Atlassian Rogue Jira', 'jira.demo.example.com', 'Atlassian', 'google_workspace', 'gw-discovery-atlassian-rogue-jira', 'Atlassian Rogue Jira', 'jira.demo.example.com', 'no_binding', '', 'idp_sso', 5, false, 'demo.user018@example.com', 'ticketed', 'DISC-218', 'medium', 'confidential', 'Under ticket while connector mapping is reviewed.'),
      (19, 'canva-design-club', 'Canva Design Club', 'canva.demo.example.com', 'Canva', 'entra', 'entra-app-canva-design-club', 'Canva Design Club', 'canva.demo.example.com', 'no_binding', '', 'oauth_grant', 3, false, 'demo.user019@example.com', 'approved', '', 'low', 'internal', 'Managed manually today.'),
      (20, '1password-personal-vaults', '1Password Personal Vaults', '1password.demo.example.com', '1Password', 'okta', 'okta-app-1password-personal-vaults', '1Password Personal Vaults', '1password.demo.example.com', 'no_binding', '', 'idp_sso', 2, false, 'demo.user020@example.com', 'approved', '', 'low', 'restricted', 'High-sensitivity data but covered by owner governance.'),
      (21, 'finance-sync-audit-bot', 'Finance Sync Audit Bot', 'finance-sync.demo.example.com', 'Unknown', 'google_workspace', 'gw-hotspot-finance-sync.apps.googleusercontent.com', 'Finance Sync Audit Bot', 'finance-sync.demo.example.com', 'hotspot', '', 'oauth_grant', 72, true, '', 'action_required', '', 'critical', 'restricted', 'Privileged Google OAuth client with no owner.'),
      (22, 'drive-mirror-exporter', 'Drive Mirror Exporter', 'drive-mirror.demo.example.com', 'Unknown', 'google_workspace', 'gw-hotspot-drive-mirror.apps.googleusercontent.com', 'Drive Mirror Exporter', 'drive-mirror.demo.example.com', 'hotspot', '', 'oauth_grant', 68, true, '', 'ticketed', 'SEC-241', 'critical', 'restricted', 'Large privileged footprint routed to security triage.'),
      (23, 'hr-ops-directory-sync', 'HR Ops Directory Sync', 'hr-ops.demo.example.com', 'Unknown', 'google_workspace', 'gw-hotspot-hr-ops.apps.googleusercontent.com', 'HR Ops Directory Sync', 'hr-ops.demo.example.com', 'hotspot', '', 'oauth_grant', 61, true, '', 'action_required', '', 'critical', 'restricted', 'Privileged HR sync client without governance owner.'),
      (24, 'mail-ops-automation', 'Mail Ops Automation', 'mail-ops.demo.example.com', 'Unknown', 'google_workspace', 'gw-hotspot-mail-ops.apps.googleusercontent.com', 'Mail Ops Automation', 'mail-ops.demo.example.com', 'hotspot', '', 'oauth_grant', 55, true, '', 'in_review', '', 'high', 'restricted', 'Review started after mailbox-wide access was detected.')
  )
INSERT INTO saas_apps (
  canonical_key,
  display_name,
  primary_domain,
  vendor_name,
  first_seen_at,
  last_seen_at,
  updated_at
)
SELECT
  app_defs.canonical_key,
  app_defs.display_name,
  app_defs.primary_domain,
  app_defs.vendor_name,
  ctx.now_ts - make_interval(days => 90 + app_defs.app_ord),
  ctx.now_ts - make_interval(hours => (app_defs.app_ord % 12) + 1),
  ctx.now_ts
FROM app_defs
CROSS JOIN ctx
ON CONFLICT (canonical_key) DO UPDATE SET
  display_name = EXCLUDED.display_name,
  primary_domain = EXCLUDED.primary_domain,
  vendor_name = EXCLUDED.vendor_name,
  first_seen_at = LEAST(saas_apps.first_seen_at, EXCLUDED.first_seen_at),
  last_seen_at = GREATEST(saas_apps.last_seen_at, EXCLUDED.last_seen_at),
  updated_at = EXCLUDED.updated_at
;

WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v3),
  app_defs (
    app_ord,
    canonical_key,
    display_name,
    primary_domain,
    vendor_name,
    discovery_source_kind,
    source_app_id,
    source_app_name,
    source_app_domain,
    category,
    binding_connector_kind,
    signal_kind,
    actor_count,
    privileged_scope,
    governance_owner_email,
    governance_state,
    ticket_ref,
    business_criticality,
    data_classification,
    notes
  ) AS (
    VALUES
      ( 1, 'github-actions-control-plane', 'GitHub Actions Control Plane', 'actions.demo.example.com', 'GitHub', 'okta', 'okta-app-github-actions-control-plane', 'GitHub Actions Control Plane', 'actions.demo.example.com', 'managed', 'github', 'idp_sso', 6, false, '', '', '', 'unknown', 'unknown', ''),
      ( 2, 'datadog-ci-monitoring', 'Datadog CI Monitoring', 'ci-observability.demo.example.com', 'Datadog', 'okta', 'okta-app-datadog-ci-monitoring', 'Datadog CI Monitoring', 'ci-observability.demo.example.com', 'managed', 'datadog', 'idp_sso', 5, false, '', '', '', 'unknown', 'unknown', ''),
      ( 3, 'github-copilot-enterprise', 'GitHub Copilot Enterprise', 'copilot.demo.example.com', 'GitHub', 'google_workspace', 'gw-discovery-github-copilot-enterprise', 'GitHub Copilot Enterprise', 'copilot.demo.example.com', 'managed', 'github', 'oauth_grant', 7, false, '', '', '', 'unknown', 'unknown', ''),
      ( 4, 'datadog-incident-bridge', 'Datadog Incident Bridge', 'incident-bridge.demo.example.com', 'Datadog', 'google_workspace', 'gw-discovery-datadog-incident-bridge', 'Datadog Incident Bridge', 'incident-bridge.demo.example.com', 'managed', 'datadog', 'oauth_grant', 6, false, '', '', '', 'unknown', 'unknown', ''),
      ( 5, 'github-security-review-bot', 'GitHub Security Review Bot', 'security-review.demo.example.com', 'GitHub', 'okta', 'okta-app-github-security-review-bot', 'GitHub Security Review Bot', 'security-review.demo.example.com', 'managed', 'github', 'idp_sso', 5, false, '', '', '', 'unknown', 'unknown', ''),
      ( 6, 'datadog-synthetic-control', 'Datadog Synthetic Control', 'synthetics.demo.example.com', 'Datadog', 'google_workspace', 'gw-discovery-datadog-synthetic-control', 'Datadog Synthetic Control', 'synthetics.demo.example.com', 'managed', 'datadog', 'oauth_grant', 6, false, '', '', '', 'unknown', 'unknown', ''),
      ( 7, 'aws-console-federation', 'AWS Console Federation', 'aws-console.demo.example.com', 'Amazon', 'okta', 'okta-app-aws-console-federation', 'AWS Console Federation', 'aws-console.demo.example.com', 'managed', 'aws_identity_center', 'idp_sso', 6, false, '', '', '', 'unknown', 'unknown', ''),
      ( 8, 'aws-billing-workspace', 'AWS Billing Workspace', 'billing.demo.example.com', 'Amazon', 'google_workspace', 'gw-discovery-aws-billing-workspace', 'AWS Billing Workspace', 'billing.demo.example.com', 'managed', 'aws_identity_center', 'oauth_grant', 5, false, '', '', '', 'unknown', 'unknown', ''),
      ( 9, 'azure-reference-ops-portal', 'Azure Reference Ops Portal', 'ops-portal.demo.example.com', 'Microsoft', 'entra', 'entra-app-azure-reference-ops-portal', 'Azure Reference Ops Portal', 'ops-portal.demo.example.com', 'stale_binding', 'entra', 'idp_sso', 8, false, 'demo.user009@example.com', 'in_review', '', 'low', 'internal', 'Previous Entra mapping kept for the demo stale-sync scenario.'),
      (10, 'azure-contractors-workbench', 'Azure Contractors Workbench', 'contractors.demo.example.com', 'Microsoft', 'okta', 'okta-app-azure-contractors-workbench', 'Azure Contractors Workbench', 'contractors.demo.example.com', 'stale_binding', 'entra', 'idp_sso', 7, false, 'demo.user010@example.com', 'approved', '', 'low', 'public', 'Authorized but intentionally stale to show aged bindings.'),
      (11, 'azure-data-exchange', 'Azure Data Exchange', 'data-exchange.demo.example.com', 'Microsoft', 'google_workspace', 'gw-discovery-azure-data-exchange', 'Azure Data Exchange', 'data-exchange.demo.example.com', 'stale_binding', 'entra', 'oauth_grant', 7, false, 'demo.user011@example.com', 'in_review', '', 'medium', 'internal', 'App remains linked to a stale Entra sync for demo posture coverage.'),
      (12, 'azure-identity-admin-lite', 'Azure Identity Admin Lite', 'identity-admin.demo.example.com', 'Microsoft', 'entra', 'entra-app-azure-identity-admin-lite', 'Azure Identity Admin Lite', 'identity-admin.demo.example.com', 'stale_binding', 'entra', 'idp_sso', 6, false, 'demo.user012@example.com', 'approved', '', 'low', 'internal', 'Seeded as a stale Entra-bound discovery row.'),
      (13, 'notion-shadow-wiki', 'Notion Shadow Wiki', 'notion.demo.example.com', 'Notion', 'okta', 'okta-app-notion-shadow-wiki', 'Notion Shadow Wiki', 'notion.demo.example.com', 'no_binding', '', 'idp_sso', 4, false, 'demo.user013@example.com', 'approved', '', 'low', 'internal', 'Known but intentionally left unmanaged for the discovery story.'),
      (14, 'miro-brainstorm-lab', 'Miro Brainstorm Lab', 'miro.demo.example.com', 'Miro', 'google_workspace', 'gw-discovery-miro-brainstorm-lab', 'Miro Brainstorm Lab', 'miro.demo.example.com', 'no_binding', '', 'oauth_grant', 5, false, 'demo.user014@example.com', 'in_review', '', 'medium', 'confidential', 'OAuth-based app without a managed connector.'),
      (15, 'figma-freelance-studio', 'Figma Freelance Studio', 'figma.demo.example.com', 'Figma', 'okta', 'okta-app-figma-freelance-studio', 'Figma Freelance Studio', 'figma.demo.example.com', 'no_binding', '', 'idp_sso', 3, false, 'demo.user015@example.com', 'approved', '', 'low', 'internal', 'Seeded as unmanaged but owned.'),
      (16, 'linear-skunkworks', 'Linear Skunkworks', 'linear.demo.example.com', 'Linear', 'entra', 'entra-app-linear-skunkworks', 'Linear Skunkworks', 'linear.demo.example.com', 'no_binding', '', 'idp_sso', 6, false, 'demo.user016@example.com', 'approved', '', 'medium', 'internal', 'No binding on purpose to populate unmanaged discovery states.'),
      (17, 'slack-reference-grid', 'Slack Reference Grid', 'slack.demo.example.com', 'Slack', 'okta', 'okta-app-slack-reference-grid', 'Slack Reference Grid', 'slack.demo.example.com', 'no_binding', '', 'idp_sso', 4, false, 'demo.user017@example.com', 'in_review', '', 'medium', 'internal', 'Older org-level workspace left unmanaged.'),
      (18, 'atlassian-rogue-jira', 'Atlassian Rogue Jira', 'jira.demo.example.com', 'Atlassian', 'google_workspace', 'gw-discovery-atlassian-rogue-jira', 'Atlassian Rogue Jira', 'jira.demo.example.com', 'no_binding', '', 'idp_sso', 5, false, 'demo.user018@example.com', 'ticketed', 'DISC-218', 'medium', 'confidential', 'Under ticket while connector mapping is reviewed.'),
      (19, 'canva-design-club', 'Canva Design Club', 'canva.demo.example.com', 'Canva', 'entra', 'entra-app-canva-design-club', 'Canva Design Club', 'canva.demo.example.com', 'no_binding', '', 'oauth_grant', 3, false, 'demo.user019@example.com', 'approved', '', 'low', 'internal', 'Managed manually today.'),
      (20, '1password-personal-vaults', '1Password Personal Vaults', '1password.demo.example.com', '1Password', 'okta', 'okta-app-1password-personal-vaults', '1Password Personal Vaults', '1password.demo.example.com', 'no_binding', '', 'idp_sso', 2, false, 'demo.user020@example.com', 'approved', '', 'low', 'restricted', 'High-sensitivity data but covered by owner governance.'),
      (21, 'finance-sync-audit-bot', 'Finance Sync Audit Bot', 'finance-sync.demo.example.com', 'Unknown', 'google_workspace', 'gw-hotspot-finance-sync.apps.googleusercontent.com', 'Finance Sync Audit Bot', 'finance-sync.demo.example.com', 'hotspot', '', 'oauth_grant', 72, true, '', 'action_required', '', 'critical', 'restricted', 'Privileged Google OAuth client with no owner.'),
      (22, 'drive-mirror-exporter', 'Drive Mirror Exporter', 'drive-mirror.demo.example.com', 'Unknown', 'google_workspace', 'gw-hotspot-drive-mirror.apps.googleusercontent.com', 'Drive Mirror Exporter', 'drive-mirror.demo.example.com', 'hotspot', '', 'oauth_grant', 68, true, '', 'ticketed', 'SEC-241', 'critical', 'restricted', 'Large privileged footprint routed to security triage.'),
      (23, 'hr-ops-directory-sync', 'HR Ops Directory Sync', 'hr-ops.demo.example.com', 'Unknown', 'google_workspace', 'gw-hotspot-hr-ops.apps.googleusercontent.com', 'HR Ops Directory Sync', 'hr-ops.demo.example.com', 'hotspot', '', 'oauth_grant', 61, true, '', 'action_required', '', 'critical', 'restricted', 'Privileged HR sync client without governance owner.'),
      (24, 'mail-ops-automation', 'Mail Ops Automation', 'mail-ops.demo.example.com', 'Unknown', 'google_workspace', 'gw-hotspot-mail-ops.apps.googleusercontent.com', 'Mail Ops Automation', 'mail-ops.demo.example.com', 'hotspot', '', 'oauth_grant', 55, true, '', 'in_review', '', 'high', 'restricted', 'Review started after mailbox-wide access was detected.')
  ),
  sources AS (
    SELECT
      sa.id AS saas_app_id,
      app_defs.app_ord,
      app_defs.canonical_key,
      app_defs.discovery_source_kind AS source_kind,
      CASE app_defs.discovery_source_kind
        WHEN 'okta' THEN ctx.okta_domain
        WHEN 'entra' THEN ctx.entra_tenant_id
        WHEN 'google_workspace' THEN ctx.google_customer_id
        ELSE ''
      END AS source_name,
      app_defs.source_app_id,
      app_defs.source_app_name,
      app_defs.source_app_domain,
      CASE app_defs.discovery_source_kind
        WHEN 'okta' THEN ctx.okta_discovery_run_id
        WHEN 'entra' THEN ctx.entra_discovery_run_id
        WHEN 'google_workspace' THEN ctx.google_discovery_run_id
        ELSE ctx.okta_discovery_run_id
      END AS run_id,
      CASE app_defs.discovery_source_kind
        WHEN 'entra' THEN ctx.now_ts - interval '3 days'
        ELSE ctx.now_ts - interval '2 minutes'
      END AS observed_at
    FROM app_defs
    JOIN saas_apps sa ON sa.canonical_key = app_defs.canonical_key
    CROSS JOIN ctx
  )
INSERT INTO saas_app_sources (
  saas_app_id,
  source_kind,
  source_name,
  source_app_id,
  source_app_name,
  source_app_domain,
  seen_in_run_id,
  seen_at,
  last_observed_run_id,
  last_observed_at,
  expired_at,
  expired_run_id,
  updated_at
)
SELECT
  sources.saas_app_id,
  sources.source_kind,
  sources.source_name,
  sources.source_app_id,
  sources.source_app_name,
  sources.source_app_domain,
  sources.run_id,
  sources.observed_at,
  sources.run_id,
  sources.observed_at,
  NULL::timestamptz,
  NULL::bigint,
  (SELECT now_ts FROM ctx)
FROM sources
CROSS JOIN ctx
ON CONFLICT (source_kind, source_name, source_app_id) DO UPDATE SET
  saas_app_id = EXCLUDED.saas_app_id,
  source_app_name = EXCLUDED.source_app_name,
  source_app_domain = EXCLUDED.source_app_domain,
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  last_observed_run_id = EXCLUDED.last_observed_run_id,
  last_observed_at = EXCLUDED.last_observed_at,
  expired_at = NULL,
  expired_run_id = NULL,
  updated_at = EXCLUDED.updated_at
;

WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v3),
  app_defs (
    app_ord,
    canonical_key,
    display_name,
    primary_domain,
    vendor_name,
    discovery_source_kind,
    source_app_id,
    source_app_name,
    source_app_domain,
    category,
    binding_connector_kind,
    signal_kind,
    actor_count,
    privileged_scope,
    governance_owner_email,
    governance_state,
    ticket_ref,
    business_criticality,
    data_classification,
    notes
  ) AS (
    VALUES
      ( 1, 'github-actions-control-plane', 'GitHub Actions Control Plane', 'actions.demo.example.com', 'GitHub', 'okta', 'okta-app-github-actions-control-plane', 'GitHub Actions Control Plane', 'actions.demo.example.com', 'managed', 'github', 'idp_sso', 6, false, '', '', '', 'unknown', 'unknown', ''),
      ( 2, 'datadog-ci-monitoring', 'Datadog CI Monitoring', 'ci-observability.demo.example.com', 'Datadog', 'okta', 'okta-app-datadog-ci-monitoring', 'Datadog CI Monitoring', 'ci-observability.demo.example.com', 'managed', 'datadog', 'idp_sso', 5, false, '', '', '', 'unknown', 'unknown', ''),
      ( 3, 'github-copilot-enterprise', 'GitHub Copilot Enterprise', 'copilot.demo.example.com', 'GitHub', 'google_workspace', 'gw-discovery-github-copilot-enterprise', 'GitHub Copilot Enterprise', 'copilot.demo.example.com', 'managed', 'github', 'oauth_grant', 7, false, '', '', '', 'unknown', 'unknown', ''),
      ( 4, 'datadog-incident-bridge', 'Datadog Incident Bridge', 'incident-bridge.demo.example.com', 'Datadog', 'google_workspace', 'gw-discovery-datadog-incident-bridge', 'Datadog Incident Bridge', 'incident-bridge.demo.example.com', 'managed', 'datadog', 'oauth_grant', 6, false, '', '', '', 'unknown', 'unknown', ''),
      ( 5, 'github-security-review-bot', 'GitHub Security Review Bot', 'security-review.demo.example.com', 'GitHub', 'okta', 'okta-app-github-security-review-bot', 'GitHub Security Review Bot', 'security-review.demo.example.com', 'managed', 'github', 'idp_sso', 5, false, '', '', '', 'unknown', 'unknown', ''),
      ( 6, 'datadog-synthetic-control', 'Datadog Synthetic Control', 'synthetics.demo.example.com', 'Datadog', 'google_workspace', 'gw-discovery-datadog-synthetic-control', 'Datadog Synthetic Control', 'synthetics.demo.example.com', 'managed', 'datadog', 'oauth_grant', 6, false, '', '', '', 'unknown', 'unknown', ''),
      ( 7, 'aws-console-federation', 'AWS Console Federation', 'aws-console.demo.example.com', 'Amazon', 'okta', 'okta-app-aws-console-federation', 'AWS Console Federation', 'aws-console.demo.example.com', 'managed', 'aws_identity_center', 'idp_sso', 6, false, '', '', '', 'unknown', 'unknown', ''),
      ( 8, 'aws-billing-workspace', 'AWS Billing Workspace', 'billing.demo.example.com', 'Amazon', 'google_workspace', 'gw-discovery-aws-billing-workspace', 'AWS Billing Workspace', 'billing.demo.example.com', 'managed', 'aws_identity_center', 'oauth_grant', 5, false, '', '', '', 'unknown', 'unknown', ''),
      ( 9, 'azure-reference-ops-portal', 'Azure Reference Ops Portal', 'ops-portal.demo.example.com', 'Microsoft', 'entra', 'entra-app-azure-reference-ops-portal', 'Azure Reference Ops Portal', 'ops-portal.demo.example.com', 'stale_binding', 'entra', 'idp_sso', 8, false, 'demo.user009@example.com', 'in_review', '', 'low', 'internal', 'Previous Entra mapping kept for the demo stale-sync scenario.'),
      (10, 'azure-contractors-workbench', 'Azure Contractors Workbench', 'contractors.demo.example.com', 'Microsoft', 'okta', 'okta-app-azure-contractors-workbench', 'Azure Contractors Workbench', 'contractors.demo.example.com', 'stale_binding', 'entra', 'idp_sso', 7, false, 'demo.user010@example.com', 'approved', '', 'low', 'public', 'Authorized but intentionally stale to show aged bindings.'),
      (11, 'azure-data-exchange', 'Azure Data Exchange', 'data-exchange.demo.example.com', 'Microsoft', 'google_workspace', 'gw-discovery-azure-data-exchange', 'Azure Data Exchange', 'data-exchange.demo.example.com', 'stale_binding', 'entra', 'oauth_grant', 7, false, 'demo.user011@example.com', 'in_review', '', 'medium', 'internal', 'App remains linked to a stale Entra sync for demo posture coverage.'),
      (12, 'azure-identity-admin-lite', 'Azure Identity Admin Lite', 'identity-admin.demo.example.com', 'Microsoft', 'entra', 'entra-app-azure-identity-admin-lite', 'Azure Identity Admin Lite', 'identity-admin.demo.example.com', 'stale_binding', 'entra', 'idp_sso', 6, false, 'demo.user012@example.com', 'approved', '', 'low', 'internal', 'Seeded as a stale Entra-bound discovery row.'),
      (13, 'notion-shadow-wiki', 'Notion Shadow Wiki', 'notion.demo.example.com', 'Notion', 'okta', 'okta-app-notion-shadow-wiki', 'Notion Shadow Wiki', 'notion.demo.example.com', 'no_binding', '', 'idp_sso', 4, false, 'demo.user013@example.com', 'approved', '', 'low', 'internal', 'Known but intentionally left unmanaged for the discovery story.'),
      (14, 'miro-brainstorm-lab', 'Miro Brainstorm Lab', 'miro.demo.example.com', 'Miro', 'google_workspace', 'gw-discovery-miro-brainstorm-lab', 'Miro Brainstorm Lab', 'miro.demo.example.com', 'no_binding', '', 'oauth_grant', 5, false, 'demo.user014@example.com', 'in_review', '', 'medium', 'confidential', 'OAuth-based app without a managed connector.'),
      (15, 'figma-freelance-studio', 'Figma Freelance Studio', 'figma.demo.example.com', 'Figma', 'okta', 'okta-app-figma-freelance-studio', 'Figma Freelance Studio', 'figma.demo.example.com', 'no_binding', '', 'idp_sso', 3, false, 'demo.user015@example.com', 'approved', '', 'low', 'internal', 'Seeded as unmanaged but owned.'),
      (16, 'linear-skunkworks', 'Linear Skunkworks', 'linear.demo.example.com', 'Linear', 'entra', 'entra-app-linear-skunkworks', 'Linear Skunkworks', 'linear.demo.example.com', 'no_binding', '', 'idp_sso', 6, false, 'demo.user016@example.com', 'approved', '', 'medium', 'internal', 'No binding on purpose to populate unmanaged discovery states.'),
      (17, 'slack-reference-grid', 'Slack Reference Grid', 'slack.demo.example.com', 'Slack', 'okta', 'okta-app-slack-reference-grid', 'Slack Reference Grid', 'slack.demo.example.com', 'no_binding', '', 'idp_sso', 4, false, 'demo.user017@example.com', 'in_review', '', 'medium', 'internal', 'Older org-level workspace left unmanaged.'),
      (18, 'atlassian-rogue-jira', 'Atlassian Rogue Jira', 'jira.demo.example.com', 'Atlassian', 'google_workspace', 'gw-discovery-atlassian-rogue-jira', 'Atlassian Rogue Jira', 'jira.demo.example.com', 'no_binding', '', 'idp_sso', 5, false, 'demo.user018@example.com', 'ticketed', 'DISC-218', 'medium', 'confidential', 'Under ticket while connector mapping is reviewed.'),
      (19, 'canva-design-club', 'Canva Design Club', 'canva.demo.example.com', 'Canva', 'entra', 'entra-app-canva-design-club', 'Canva Design Club', 'canva.demo.example.com', 'no_binding', '', 'oauth_grant', 3, false, 'demo.user019@example.com', 'approved', '', 'low', 'internal', 'Managed manually today.'),
      (20, '1password-personal-vaults', '1Password Personal Vaults', '1password.demo.example.com', '1Password', 'okta', 'okta-app-1password-personal-vaults', '1Password Personal Vaults', '1password.demo.example.com', 'no_binding', '', 'idp_sso', 2, false, 'demo.user020@example.com', 'approved', '', 'low', 'restricted', 'High-sensitivity data but covered by owner governance.'),
      (21, 'finance-sync-audit-bot', 'Finance Sync Audit Bot', 'finance-sync.demo.example.com', 'Unknown', 'google_workspace', 'gw-hotspot-finance-sync.apps.googleusercontent.com', 'Finance Sync Audit Bot', 'finance-sync.demo.example.com', 'hotspot', '', 'oauth_grant', 72, true, '', 'action_required', '', 'critical', 'restricted', 'Privileged Google OAuth client with no owner.'),
      (22, 'drive-mirror-exporter', 'Drive Mirror Exporter', 'drive-mirror.demo.example.com', 'Unknown', 'google_workspace', 'gw-hotspot-drive-mirror.apps.googleusercontent.com', 'Drive Mirror Exporter', 'drive-mirror.demo.example.com', 'hotspot', '', 'oauth_grant', 68, true, '', 'ticketed', 'SEC-241', 'critical', 'restricted', 'Large privileged footprint routed to security triage.'),
      (23, 'hr-ops-directory-sync', 'HR Ops Directory Sync', 'hr-ops.demo.example.com', 'Unknown', 'google_workspace', 'gw-hotspot-hr-ops.apps.googleusercontent.com', 'HR Ops Directory Sync', 'hr-ops.demo.example.com', 'hotspot', '', 'oauth_grant', 61, true, '', 'action_required', '', 'critical', 'restricted', 'Privileged HR sync client without governance owner.'),
      (24, 'mail-ops-automation', 'Mail Ops Automation', 'mail-ops.demo.example.com', 'Unknown', 'google_workspace', 'gw-hotspot-mail-ops.apps.googleusercontent.com', 'Mail Ops Automation', 'mail-ops.demo.example.com', 'hotspot', '', 'oauth_grant', 55, true, '', 'in_review', '', 'high', 'restricted', 'Review started after mailbox-wide access was detected.')
  ),
  seeded AS (
    SELECT
      sa.id AS saas_app_id,
      app_defs.app_ord,
      app_defs.canonical_key,
      app_defs.discovery_source_kind AS source_kind,
      CASE app_defs.discovery_source_kind
        WHEN 'okta' THEN ctx.okta_domain
        WHEN 'entra' THEN ctx.entra_tenant_id
        WHEN 'google_workspace' THEN ctx.google_customer_id
        ELSE ''
      END AS source_name,
      app_defs.source_app_id,
      app_defs.source_app_name,
      app_defs.source_app_domain,
      app_defs.signal_kind,
      app_defs.actor_count,
      app_defs.privileged_scope,
      CASE app_defs.discovery_source_kind
        WHEN 'okta' THEN ctx.okta_discovery_run_id
        WHEN 'entra' THEN ctx.entra_discovery_run_id
        WHEN 'google_workspace' THEN ctx.google_discovery_run_id
        ELSE ctx.okta_discovery_run_id
      END AS run_id
    FROM app_defs
    JOIN saas_apps sa ON sa.canonical_key = app_defs.canonical_key
    CROSS JOIN ctx
  ),
  events AS (
    SELECT
      seeded.saas_app_id,
      seeded.source_kind,
      seeded.source_name,
      seeded.signal_kind,
      format('%s-event-%s', seeded.canonical_key, lpad(actor_idx::text, 3, '0')) AS event_external_id,
      seeded.source_app_id,
      seeded.source_app_name,
      seeded.source_app_domain,
      format('%s-actor-%s', replace(seeded.canonical_key, '-', ''), lpad(actor_idx::text, 3, '0')) AS actor_external_id,
      format('%s.actor%s@demo.example.com', replace(seeded.canonical_key, '-', '.'), lpad(actor_idx::text, 3, '0')) AS actor_email,
      format('Demo Actor %s-%s', seeded.app_ord, lpad(actor_idx::text, 3, '0')) AS actor_display_name,
      CASE seeded.source_kind
        WHEN 'entra' THEN (SELECT now_ts FROM ctx) - interval '3 days' + make_interval(hours => (actor_idx % 8))
        ELSE (SELECT now_ts FROM ctx) - make_interval(hours => (actor_idx % 48))
      END AS observed_at,
      CASE
        WHEN seeded.signal_kind = 'oauth_grant' AND seeded.privileged_scope THEN jsonb_build_array('directory.readwrite.all', 'offline_access', 'mail.read')
        WHEN seeded.signal_kind = 'oauth_grant' THEN jsonb_build_array('mail.read', 'drive.file')
        ELSE '[]'::jsonb
      END AS scopes_json,
      seeded.run_id,
      jsonb_build_object(
        'dataset', 'demo_breadth',
        'source_app_id', seeded.source_app_id,
        'signal_kind', seeded.signal_kind
      ) AS raw_json
    FROM seeded
    CROSS JOIN generate_series(1, 72) AS actor_idx
    WHERE actor_idx <= seeded.actor_count
  )
INSERT INTO saas_app_events (
  saas_app_id,
  source_kind,
  source_name,
  signal_kind,
  event_external_id,
  source_app_id,
  source_app_name,
  source_app_domain,
  actor_external_id,
  actor_email,
  actor_display_name,
  observed_at,
  scopes_json,
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
  events.saas_app_id,
  events.source_kind,
  events.source_name,
  events.signal_kind,
  events.event_external_id,
  events.source_app_id,
  events.source_app_name,
  events.source_app_domain,
  events.actor_external_id,
  events.actor_email,
  events.actor_display_name,
  events.observed_at,
  events.scopes_json,
  events.raw_json,
  events.run_id,
  events.observed_at,
  events.run_id,
  events.observed_at,
  NULL::timestamptz,
  NULL::bigint,
  (SELECT now_ts FROM ctx)
FROM events
CROSS JOIN ctx
ON CONFLICT (source_kind, source_name, signal_kind, event_external_id) DO UPDATE SET
  saas_app_id = EXCLUDED.saas_app_id,
  source_app_id = EXCLUDED.source_app_id,
  source_app_name = EXCLUDED.source_app_name,
  source_app_domain = EXCLUDED.source_app_domain,
  actor_external_id = EXCLUDED.actor_external_id,
  actor_email = EXCLUDED.actor_email,
  actor_display_name = EXCLUDED.actor_display_name,
  observed_at = EXCLUDED.observed_at,
  scopes_json = EXCLUDED.scopes_json,
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
  ctx AS (SELECT * FROM demo_seed_ctx_v3),
  app_defs (
    app_ord,
    canonical_key,
    binding_connector_kind
  ) AS (
    VALUES
      ( 1, 'github-actions-control-plane', 'github'),
      ( 2, 'datadog-ci-monitoring', 'datadog'),
      ( 3, 'github-copilot-enterprise', 'github'),
      ( 4, 'datadog-incident-bridge', 'datadog'),
      ( 5, 'github-security-review-bot', 'github'),
      ( 6, 'datadog-synthetic-control', 'datadog'),
      ( 7, 'aws-console-federation', 'aws_identity_center'),
      ( 8, 'aws-billing-workspace', 'aws_identity_center'),
      ( 9, 'azure-reference-ops-portal', 'entra'),
      (10, 'azure-contractors-workbench', 'entra'),
      (11, 'azure-data-exchange', 'entra'),
      (12, 'azure-identity-admin-lite', 'entra')
  )
INSERT INTO saas_app_bindings (
  saas_app_id,
  connector_kind,
  connector_source_name,
  binding_source,
  confidence,
  is_primary,
  created_by_auth_user_id,
  created_at,
  updated_at
)
SELECT
  sa.id,
  app_defs.binding_connector_kind,
  CASE app_defs.binding_connector_kind
    WHEN 'github' THEN ctx.github_org
    WHEN 'datadog' THEN ctx.datadog_site
    WHEN 'entra' THEN ctx.entra_tenant_id
    WHEN 'aws_identity_center' THEN ctx.aws_region
    ELSE ''
  END,
  'manual',
  CASE
    WHEN app_defs.binding_connector_kind = 'entra' THEN 0.82
    ELSE 0.97
  END,
  true,
  NULL::bigint,
  ctx.now_ts,
  ctx.now_ts
FROM app_defs
JOIN saas_apps sa ON sa.canonical_key = app_defs.canonical_key
CROSS JOIN ctx
ON CONFLICT (saas_app_id, connector_kind, connector_source_name) DO UPDATE SET
  binding_source = EXCLUDED.binding_source,
  confidence = EXCLUDED.confidence,
  is_primary = EXCLUDED.is_primary,
  updated_at = EXCLUDED.updated_at
;

WITH
  ctx AS (SELECT * FROM demo_seed_ctx_v3),
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
  app_defs (
    canonical_key,
    governance_owner_email,
    governance_state,
    ticket_ref,
    business_criticality,
    data_classification,
    notes
  ) AS (
    VALUES
      ('azure-reference-ops-portal', 'demo.user009@example.com', 'in_review', '', 'low', 'internal', 'Previous Entra mapping kept for the demo stale-sync scenario.'),
      ('azure-contractors-workbench', 'demo.user010@example.com', 'approved', '', 'low', 'public', 'Authorized but intentionally stale to show aged bindings.'),
      ('azure-data-exchange', 'demo.user011@example.com', 'in_review', '', 'medium', 'internal', 'App remains linked to a stale Entra sync for demo posture coverage.'),
      ('azure-identity-admin-lite', 'demo.user012@example.com', 'approved', '', 'low', 'internal', 'Seeded as a stale Entra-bound discovery row.'),
      ('notion-shadow-wiki', 'demo.user013@example.com', 'approved', '', 'low', 'internal', 'Known but intentionally left unmanaged for the discovery story.'),
      ('miro-brainstorm-lab', 'demo.user014@example.com', 'in_review', '', 'medium', 'confidential', 'OAuth-based app without a managed connector.'),
      ('figma-freelance-studio', 'demo.user015@example.com', 'approved', '', 'low', 'internal', 'Seeded as unmanaged but owned.'),
      ('linear-skunkworks', 'demo.user016@example.com', 'approved', '', 'medium', 'internal', 'No binding on purpose to populate unmanaged discovery states.'),
      ('slack-reference-grid', 'demo.user017@example.com', 'in_review', '', 'medium', 'internal', 'Older org-level workspace left unmanaged.'),
      ('atlassian-rogue-jira', 'demo.user018@example.com', 'ticketed', 'DISC-218', 'medium', 'confidential', 'Under ticket while connector mapping is reviewed.'),
      ('canva-design-club', 'demo.user019@example.com', 'approved', '', 'low', 'internal', 'Managed manually today.'),
      ('1password-personal-vaults', 'demo.user020@example.com', 'approved', '', 'low', 'restricted', 'High-sensitivity data but covered by owner governance.'),
      ('finance-sync-audit-bot', '', 'action_required', '', 'critical', 'restricted', 'Privileged Google OAuth client with no owner.'),
      ('drive-mirror-exporter', '', 'ticketed', 'SEC-241', 'critical', 'restricted', 'Large privileged footprint routed to security triage.'),
      ('hr-ops-directory-sync', '', 'action_required', '', 'critical', 'restricted', 'Privileged HR sync client without governance owner.'),
      ('mail-ops-automation', '', 'in_review', '', 'high', 'restricted', 'Review started after mailbox-wide access was detected.')
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
  'saas_app',
  sa.id,
  app_defs.governance_state,
  owner.id,
  app_defs.business_criticality,
  app_defs.data_classification,
  app_defs.ticket_ref,
  app_defs.notes,
  NULL::bigint,
  ctx.now_ts
FROM app_defs
JOIN saas_apps sa ON sa.canonical_key = app_defs.canonical_key
LEFT JOIN LATERAL (
  SELECT i.id
  FROM identities i
  LEFT JOIN authoritative_identities ai ON ai.identity_id = i.id
  WHERE lower(i.primary_email) = lower(app_defs.governance_owner_email)
  ORDER BY (ai.identity_id IS NOT NULL) DESC, i.id ASC
  LIMIT 1
) owner ON TRUE
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

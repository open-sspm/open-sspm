-- Backfill Okta users into accounts as first-class source rows.

WITH okta_source AS (
  SELECT COALESCE(NULLIF(lower(trim(config ->> 'domain')), ''), 'okta') AS source_name
  FROM connector_configs
  WHERE kind = 'okta'
  LIMIT 1
), resolved_okta_source AS (
  SELECT COALESCE((SELECT source_name FROM okta_source), 'okta') AS source_name
)
INSERT INTO accounts (
  source_kind,
  source_name,
  external_id,
  email,
  display_name,
  status,
  raw_json,
  created_at,
  updated_at,
  last_login_at,
  last_login_ip,
  last_login_region,
  seen_in_run_id,
  seen_at,
  last_observed_run_id,
  last_observed_at,
  expired_at,
  expired_run_id
)
SELECT
  'okta',
  ros.source_name,
  iu.external_id,
  lower(trim(iu.email)),
  iu.display_name,
  iu.status,
  iu.raw_json,
  iu.created_at,
  iu.updated_at,
  iu.last_login_at,
  iu.last_login_ip,
  iu.last_login_region,
  iu.seen_in_run_id,
  iu.seen_at,
  iu.last_observed_run_id,
  iu.last_observed_at,
  iu.expired_at,
  iu.expired_run_id
FROM idp_users iu
CROSS JOIN resolved_okta_source ros
ON CONFLICT (source_kind, source_name, external_id) DO UPDATE SET
  email = EXCLUDED.email,
  display_name = EXCLUDED.display_name,
  status = EXCLUDED.status,
  raw_json = EXCLUDED.raw_json,
  updated_at = EXCLUDED.updated_at,
  last_login_at = COALESCE(EXCLUDED.last_login_at, accounts.last_login_at),
  last_login_ip = COALESCE(NULLIF(EXCLUDED.last_login_ip, ''), accounts.last_login_ip),
  last_login_region = COALESCE(NULLIF(EXCLUDED.last_login_region, ''), accounts.last_login_region),
  seen_in_run_id = EXCLUDED.seen_in_run_id,
  seen_at = EXCLUDED.seen_at,
  last_observed_run_id = EXCLUDED.last_observed_run_id,
  last_observed_at = EXCLUDED.last_observed_at,
  expired_at = EXCLUDED.expired_at,
  expired_run_id = EXCLUDED.expired_run_id;

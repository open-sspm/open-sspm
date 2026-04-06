DROP FUNCTION IF EXISTS app_asset_posture_rows(timestamptz);
DROP VIEW IF EXISTS app_asset_posture_inputs_v;

DROP FUNCTION IF EXISTS saas_app_posture_rows(
  timestamptz,
  timestamptz,
  timestamptz,
  timestamptz,
  timestamptz,
  timestamptz,
  timestamptz
);
DROP VIEW IF EXISTS saas_app_posture_inputs_v;

DROP VIEW IF EXISTS connected_app_summaries_v;

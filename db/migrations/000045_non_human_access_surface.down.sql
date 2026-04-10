DROP VIEW IF EXISTS non_human_principal_read_models_v;
DROP VIEW IF EXISTS non_human_principal_projection_v;
DROP VIEW IF EXISTS non_human_app_asset_credential_refs_v;
DROP VIEW IF EXISTS non_human_principal_asset_links_v;

DROP TABLE IF EXISTS non_human_principals;

DROP FUNCTION IF EXISTS non_human_account_asset_external_id(
  text,
  text,
  text
);

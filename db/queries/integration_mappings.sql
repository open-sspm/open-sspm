-- name: UpsertIntegrationOktaAppMap :exec
INSERT INTO integration_okta_app_map (
  integration_kind,
  okta_source_kind,
  okta_source_name,
  okta_app_external_id,
  updated_at
)
VALUES ($1, $2, $3, $4, now())
ON CONFLICT (integration_kind) DO UPDATE SET
  okta_source_kind = EXCLUDED.okta_source_kind,
  okta_source_name = EXCLUDED.okta_source_name,
  okta_app_external_id = EXCLUDED.okta_app_external_id,
  updated_at = now();

-- name: DeleteIntegrationOktaAppMap :exec
DELETE FROM integration_okta_app_map
WHERE integration_kind = $1;

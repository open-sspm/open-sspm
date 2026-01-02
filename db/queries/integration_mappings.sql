-- name: UpsertIntegrationOktaAppMap :exec
INSERT INTO integration_okta_app_map (integration_kind, okta_app_external_id, updated_at)
VALUES ($1, $2, now())
ON CONFLICT (integration_kind) DO UPDATE SET
  okta_app_external_id = EXCLUDED.okta_app_external_id,
  updated_at = now();

-- name: DeleteIntegrationOktaAppMap :exec
DELETE FROM integration_okta_app_map
WHERE integration_kind = $1;

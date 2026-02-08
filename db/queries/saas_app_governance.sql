-- name: UpsertSaaSAppGovernanceOverride :exec
INSERT INTO saas_app_governance_overrides (
  saas_app_id,
  owner_identity_id,
  business_criticality,
  data_classification,
  notes,
  updated_by_auth_user_id,
  updated_at
)
VALUES (
  sqlc.arg(saas_app_id)::bigint,
  sqlc.narg(owner_identity_id)::bigint,
  sqlc.arg(business_criticality)::text,
  sqlc.arg(data_classification)::text,
  sqlc.arg(notes)::text,
  sqlc.narg(updated_by_auth_user_id)::bigint,
  now()
)
ON CONFLICT (saas_app_id) DO UPDATE SET
  owner_identity_id = EXCLUDED.owner_identity_id,
  business_criticality = EXCLUDED.business_criticality,
  data_classification = EXCLUDED.data_classification,
  notes = EXCLUDED.notes,
  updated_by_auth_user_id = EXCLUDED.updated_by_auth_user_id,
  updated_at = now();

-- name: GetSaaSAppGovernanceViewBySaaSAppID :one
SELECT
  sa.id AS saas_app_id,
  go.owner_identity_id,
  COALESCE(go.business_criticality, 'unknown') AS business_criticality,
  COALESCE(go.data_classification, 'unknown') AS data_classification,
  COALESCE(go.notes, '') AS notes,
  go.updated_by_auth_user_id,
  go.updated_at,
  COALESCE(owner.display_name, '') AS owner_display_name,
  COALESCE(owner.primary_email, '') AS owner_primary_email
FROM saas_apps sa
LEFT JOIN saas_app_governance_overrides go ON go.saas_app_id = sa.id
LEFT JOIN identities owner ON owner.id = go.owner_identity_id
WHERE sa.id = $1;

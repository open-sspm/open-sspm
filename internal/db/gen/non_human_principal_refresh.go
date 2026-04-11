package gen

import "context"

const deleteAllNonHumanPrincipals = `
DELETE FROM non_human_principals
`

const insertAllNonHumanPrincipalReadModels = `
INSERT INTO non_human_principals (
  principal_ref,
  identity_id,
  app_asset_id,
  principal_type,
  source_kind,
  source_name,
  display_name,
  secondary_name,
  linked_assets_count,
  linked_credentials_count,
  last_seen_at,
  activity_state,
  freshness_state,
  governance_state,
  accountable_owner_identity_id,
  accountable_owner_display_name,
  accountable_owner_primary_email,
  owner_presence,
  has_critical_credential,
  has_high_risk_credential,
  has_expired_credential,
  has_expiring_credential,
  has_unused_credential,
  has_stale_evidence,
  risk_reason_count,
  risk_level,
  projection_refreshed_at
)
SELECT
  pr.principal_ref,
  pr.identity_id,
  pr.app_asset_id,
  pr.principal_type,
  pr.source_kind,
  pr.source_name,
  pr.display_name,
  pr.secondary_name,
  pr.linked_assets_count,
  pr.linked_credentials_count,
  pr.last_seen_at,
  pr.activity_state,
  pr.freshness_state,
  pr.governance_state,
  pr.accountable_owner_identity_id,
  pr.accountable_owner_display_name,
  pr.accountable_owner_primary_email,
  pr.owner_presence,
  pr.has_critical_credential,
  pr.has_high_risk_credential,
  pr.has_expired_credential,
  pr.has_expiring_credential,
  pr.has_unused_credential,
  pr.has_stale_evidence,
  pr.risk_reason_count,
  pr.risk_level,
  now()
FROM non_human_principal_projection_v pr
`

const deleteNonHumanPrincipalReadModelsBySource = `
WITH requested_source AS (
  SELECT
    lower(trim($1::text)) AS source_kind,
    lower(trim($2::text)) AS source_name
),
affected_identity_ids AS (
  SELECT DISTINCT i.id AS identity_id
  FROM identities i
  JOIN identity_accounts ia ON ia.identity_id = i.id
  JOIN accounts a ON a.id = ia.account_id
  JOIN requested_source rs
    ON rs.source_kind <> ''
   AND rs.source_name <> ''
   AND lower(trim(a.source_kind)) = rs.source_kind
   AND lower(trim(a.source_name)) = rs.source_name
  WHERE i.kind IN ('service', 'bot')
  UNION
  SELECT DISTINCT nhp.identity_id
  FROM non_human_principals nhp
  JOIN requested_source rs
    ON rs.source_kind <> ''
   AND rs.source_name <> ''
   AND lower(trim(nhp.source_kind)) = rs.source_kind
   AND lower(trim(nhp.source_name)) = rs.source_name
  WHERE nhp.identity_id > 0
),
affected_asset_ids AS (
  SELECT DISTINCT aa.id AS app_asset_id
  FROM app_assets aa
  JOIN requested_source rs
    ON rs.source_kind <> ''
   AND rs.source_name <> ''
   AND lower(trim(aa.source_kind)) = rs.source_kind
   AND lower(trim(aa.source_name)) = rs.source_name
  UNION
  SELECT DISTINCT nhp.app_asset_id
  FROM non_human_principals nhp
  JOIN requested_source rs
    ON rs.source_kind <> ''
   AND rs.source_name <> ''
   AND lower(trim(nhp.source_kind)) = rs.source_kind
   AND lower(trim(nhp.source_name)) = rs.source_name
  WHERE nhp.app_asset_id > 0
),
affected_principal_refs AS (
  SELECT 'identity-' || ai.identity_id::text AS principal_ref
  FROM affected_identity_ids ai
  UNION
  SELECT 'app-asset-' || aa.app_asset_id::text AS principal_ref
  FROM affected_asset_ids aa
)
DELETE FROM non_human_principals nhp
USING affected_principal_refs apr
WHERE nhp.principal_ref = apr.principal_ref
`

const insertNonHumanPrincipalReadModelsBySource = `
WITH requested_source AS (
  SELECT
    lower(trim($1::text)) AS source_kind,
    lower(trim($2::text)) AS source_name
),
affected_identity_ids AS (
  SELECT DISTINCT i.id AS identity_id
  FROM identities i
  JOIN identity_accounts ia ON ia.identity_id = i.id
  JOIN accounts a ON a.id = ia.account_id
  JOIN requested_source rs
    ON rs.source_kind <> ''
   AND rs.source_name <> ''
   AND lower(trim(a.source_kind)) = rs.source_kind
   AND lower(trim(a.source_name)) = rs.source_name
  WHERE i.kind IN ('service', 'bot')
  UNION
  SELECT DISTINCT nhp.identity_id
  FROM non_human_principals nhp
  JOIN requested_source rs
    ON rs.source_kind <> ''
   AND rs.source_name <> ''
   AND lower(trim(nhp.source_kind)) = rs.source_kind
   AND lower(trim(nhp.source_name)) = rs.source_name
  WHERE nhp.identity_id > 0
),
affected_asset_ids AS (
  SELECT DISTINCT aa.id AS app_asset_id
  FROM app_assets aa
  JOIN requested_source rs
    ON rs.source_kind <> ''
   AND rs.source_name <> ''
   AND lower(trim(aa.source_kind)) = rs.source_kind
   AND lower(trim(aa.source_name)) = rs.source_name
  UNION
  SELECT DISTINCT nhp.app_asset_id
  FROM non_human_principals nhp
  JOIN requested_source rs
    ON rs.source_kind <> ''
   AND rs.source_name <> ''
   AND lower(trim(nhp.source_kind)) = rs.source_kind
   AND lower(trim(nhp.source_name)) = rs.source_name
  WHERE nhp.app_asset_id > 0
),
affected_principal_refs AS (
  SELECT 'identity-' || ai.identity_id::text AS principal_ref
  FROM affected_identity_ids ai
  UNION
  SELECT 'app-asset-' || aa.app_asset_id::text AS principal_ref
  FROM affected_asset_ids aa
)
INSERT INTO non_human_principals (
  principal_ref,
  identity_id,
  app_asset_id,
  principal_type,
  source_kind,
  source_name,
  display_name,
  secondary_name,
  linked_assets_count,
  linked_credentials_count,
  last_seen_at,
  activity_state,
  freshness_state,
  governance_state,
  accountable_owner_identity_id,
  accountable_owner_display_name,
  accountable_owner_primary_email,
  owner_presence,
  has_critical_credential,
  has_high_risk_credential,
  has_expired_credential,
  has_expiring_credential,
  has_unused_credential,
  has_stale_evidence,
  risk_reason_count,
  risk_level,
  projection_refreshed_at
)
SELECT
  pr.principal_ref,
  pr.identity_id,
  pr.app_asset_id,
  pr.principal_type,
  pr.source_kind,
  pr.source_name,
  pr.display_name,
  pr.secondary_name,
  pr.linked_assets_count,
  pr.linked_credentials_count,
  pr.last_seen_at,
  pr.activity_state,
  pr.freshness_state,
  pr.governance_state,
  pr.accountable_owner_identity_id,
  pr.accountable_owner_display_name,
  pr.accountable_owner_primary_email,
  pr.owner_presence,
  pr.has_critical_credential,
  pr.has_high_risk_credential,
  pr.has_expired_credential,
  pr.has_expiring_credential,
  pr.has_unused_credential,
  pr.has_stale_evidence,
  pr.risk_reason_count,
  pr.risk_level,
  now()
FROM non_human_principal_projection_v pr
JOIN affected_principal_refs apr
  ON apr.principal_ref = pr.principal_ref
`

func (q *Queries) RefreshAllNonHumanPrincipalReadModelsSafely(ctx context.Context) (int64, error) {
	if _, err := q.db.Exec(ctx, deleteAllNonHumanPrincipals); err != nil {
		return 0, err
	}
	tag, err := q.db.Exec(ctx, insertAllNonHumanPrincipalReadModels)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (q *Queries) RefreshNonHumanPrincipalReadModelsBySourceSafely(ctx context.Context, arg RefreshNonHumanPrincipalReadModelsBySourceParams) (int64, error) {
	if _, err := q.db.Exec(ctx, deleteNonHumanPrincipalReadModelsBySource, arg.SourceKind, arg.SourceName); err != nil {
		return 0, err
	}
	tag, err := q.db.Exec(ctx, insertNonHumanPrincipalReadModelsBySource, arg.SourceKind, arg.SourceName)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

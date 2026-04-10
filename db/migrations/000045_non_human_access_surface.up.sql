CREATE OR REPLACE FUNCTION non_human_account_asset_external_id(
  source_kind text,
  entity_category text,
  external_id text
)
RETURNS text
LANGUAGE sql
STABLE
AS $$
SELECT CASE
  WHEN lower(trim(COALESCE(source_kind, ''))) = 'entra'
    AND lower(trim(COALESCE(entity_category, ''))) = 'service_principal'
    AND lower(trim(COALESCE(external_id, ''))) LIKE 'sp:%'
    THEN substr(trim(COALESCE(external_id, '')), 4)
  ELSE trim(COALESCE(external_id, ''))
END;
$$;

CREATE OR REPLACE VIEW non_human_principal_asset_links_v AS
WITH active_accounts AS (
  SELECT
    a.id AS account_id,
    a.source_kind,
    a.source_name,
    a.external_id,
    a.entity_category,
    non_human_account_asset_external_id(a.source_kind, a.entity_category, a.external_id) AS asset_match_external_id
  FROM accounts a
  WHERE a.expired_at IS NULL
    AND a.last_observed_run_id IS NOT NULL
),
non_human_identity_accounts AS (
  SELECT
    i.id AS identity_id,
    aa.account_id,
    aa.source_kind,
    aa.source_name,
    aa.asset_match_external_id
  FROM identities i
  JOIN identity_accounts ia ON ia.identity_id = i.id
  JOIN active_accounts aa ON aa.account_id = ia.account_id
  WHERE i.kind IN ('service', 'bot')
),
active_assets AS (
  SELECT
    aa.id AS app_asset_id,
    aa.source_kind,
    aa.source_name,
    aa.external_id
  FROM app_assets aa
  WHERE aa.expired_at IS NULL
    AND aa.last_observed_run_id IS NOT NULL
),
identity_asset_links AS (
  SELECT DISTINCT
    'identity-' || nia.identity_id::text AS principal_ref,
    nia.identity_id::bigint AS identity_id,
    aa.app_asset_id::bigint AS app_asset_id,
    aa.source_kind::text AS source_kind,
    aa.source_name::text AS source_name
  FROM non_human_identity_accounts nia
  JOIN active_assets aa
    ON aa.source_kind = nia.source_kind
   AND aa.source_name = nia.source_name
   AND lower(trim(aa.external_id)) = lower(trim(nia.asset_match_external_id))
)
SELECT
  ial.principal_ref::text AS principal_ref,
  ial.identity_id::bigint AS identity_id,
  ial.app_asset_id::bigint AS app_asset_id,
  ial.source_kind::text AS source_kind,
  ial.source_name::text AS source_name
FROM identity_asset_links ial
UNION ALL
SELECT
  'app-asset-' || aa.app_asset_id::text AS principal_ref,
  0::bigint AS identity_id,
  aa.app_asset_id::bigint AS app_asset_id,
  aa.source_kind::text AS source_kind,
  aa.source_name::text AS source_name
FROM active_assets aa;

CREATE OR REPLACE VIEW non_human_app_asset_credential_refs_v AS
WITH active_assets AS (
  SELECT
    aa.id AS app_asset_id,
    aa.source_kind,
    aa.source_name,
    aa.asset_kind,
    aa.external_id
  FROM app_assets aa
  WHERE aa.expired_at IS NULL
    AND aa.last_observed_run_id IS NOT NULL
    AND trim(COALESCE(aa.external_id, '')) <> ''
),
base_refs AS (
  SELECT
    aa.app_asset_id,
    aa.source_kind,
    aa.source_name,
    'app_asset'::text AS asset_ref_kind,
    CASE
      WHEN trim(COALESCE(aa.asset_kind, '')) = '' THEN trim(aa.external_id)
      ELSE trim(aa.asset_kind) || ':' || trim(aa.external_id)
    END::text AS asset_ref_external_id
  FROM active_assets aa
  UNION ALL
  SELECT
    aa.app_asset_id,
    aa.source_kind,
    aa.source_name,
    'app_asset'::text AS asset_ref_kind,
    trim(aa.external_id)::text AS asset_ref_external_id
  FROM active_assets aa
  UNION ALL
  SELECT
    aa.app_asset_id,
    aa.source_kind,
    aa.source_name,
    trim(aa.asset_kind)::text AS asset_ref_kind,
    CASE
      WHEN trim(COALESCE(aa.asset_kind, '')) = '' THEN trim(aa.external_id)
      ELSE trim(aa.asset_kind) || ':' || trim(aa.external_id)
    END::text AS asset_ref_external_id
  FROM active_assets aa
  WHERE lower(trim(COALESCE(aa.source_kind, ''))) = 'google_workspace'
    AND trim(COALESCE(aa.asset_kind, '')) <> ''
)
SELECT DISTINCT
  br.app_asset_id::bigint AS app_asset_id,
  br.source_kind::text AS source_kind,
  br.source_name::text AS source_name,
  br.asset_ref_kind::text AS asset_ref_kind,
  br.asset_ref_external_id::text AS asset_ref_external_id
FROM base_refs br
WHERE trim(COALESCE(br.asset_ref_kind, '')) <> ''
  AND trim(COALESCE(br.asset_ref_external_id, '')) <> '';

CREATE TABLE IF NOT EXISTS non_human_principals (
  principal_ref TEXT PRIMARY KEY,
  identity_id BIGINT NOT NULL DEFAULT 0,
  app_asset_id BIGINT NOT NULL DEFAULT 0,
  principal_type TEXT NOT NULL,
  source_kind TEXT NOT NULL,
  source_name TEXT NOT NULL,
  display_name TEXT NOT NULL,
  secondary_name TEXT NOT NULL DEFAULT '',
  linked_assets_count BIGINT NOT NULL DEFAULT 0,
  linked_credentials_count BIGINT NOT NULL DEFAULT 0,
  last_seen_at TIMESTAMPTZ,
  activity_state TEXT NOT NULL DEFAULT 'never_seen',
  freshness_state TEXT NOT NULL DEFAULT 'unknown',
  governance_state TEXT NOT NULL DEFAULT 'unreviewed',
  accountable_owner_identity_id BIGINT NOT NULL DEFAULT 0,
  accountable_owner_display_name TEXT NOT NULL DEFAULT '',
  accountable_owner_primary_email TEXT NOT NULL DEFAULT '',
  owner_presence TEXT NOT NULL DEFAULT 'unknown',
  has_critical_credential BOOLEAN NOT NULL DEFAULT false,
  has_high_risk_credential BOOLEAN NOT NULL DEFAULT false,
  has_expired_credential BOOLEAN NOT NULL DEFAULT false,
  has_expiring_credential BOOLEAN NOT NULL DEFAULT false,
  has_unused_credential BOOLEAN NOT NULL DEFAULT false,
  has_stale_evidence BOOLEAN NOT NULL DEFAULT false,
  risk_reason_count INTEGER NOT NULL DEFAULT 0,
  risk_level TEXT NOT NULL DEFAULT 'low',
  projection_refreshed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_non_human_principals_source_scope
  ON non_human_principals (source_kind, source_name, principal_type);

CREATE INDEX IF NOT EXISTS idx_non_human_principals_priority
  ON non_human_principals (risk_level, owner_presence, governance_state, freshness_state);

CREATE INDEX IF NOT EXISTS idx_non_human_principals_last_seen
  ON non_human_principals (last_seen_at DESC NULLS LAST);

CREATE OR REPLACE VIEW non_human_principal_projection_v AS
WITH active_accounts AS (
  SELECT
    a.id AS account_id,
    a.source_kind,
    a.source_name,
    a.external_id,
    a.display_name AS account_display_name,
    a.email,
    a.entity_category,
    a.created_at,
    a.last_observed_at,
    non_human_account_asset_external_id(a.source_kind, a.entity_category, a.external_id) AS asset_match_external_id
  FROM accounts a
  WHERE a.expired_at IS NULL
    AND a.last_observed_run_id IS NOT NULL
),
non_human_identity_accounts AS (
  SELECT
    i.id AS identity_id,
    i.kind AS principal_type,
    COALESCE(NULLIF(trim(i.display_name), ''), NULLIF(trim(i.primary_email), ''), 'Identity ' || i.id::text)::text AS display_name,
    COALESCE(i.primary_email, '')::text AS primary_email,
    aa.account_id,
    aa.source_kind,
    aa.source_name,
    aa.external_id,
    aa.created_at,
    aa.last_observed_at
  FROM identities i
  JOIN identity_accounts ia ON ia.identity_id = i.id
  JOIN active_accounts aa ON aa.account_id = ia.account_id
  WHERE i.kind IN ('service', 'bot')
),
identity_primary_source AS (
  SELECT DISTINCT ON (nia.identity_id)
    nia.identity_id,
    nia.source_kind,
    nia.source_name,
    nia.external_id
  FROM non_human_identity_accounts nia
  LEFT JOIN identity_source_settings iss
    ON iss.source_kind = nia.source_kind
   AND iss.source_name = nia.source_name
   AND iss.is_authoritative
  ORDER BY nia.identity_id, (iss.is_authoritative IS NOT TRUE), nia.account_id
),
identity_summary AS (
  SELECT
    nia.identity_id,
    min(nia.principal_type)::text AS principal_type,
    min(nia.display_name)::text AS display_name,
    min(nia.primary_email)::text AS primary_email,
    max(nia.last_observed_at)::timestamptz AS account_last_seen_at,
    min(nia.created_at)::timestamptz AS first_seen_at
  FROM non_human_identity_accounts nia
  GROUP BY nia.identity_id
),
active_assets AS (
  SELECT
    pr.id::bigint AS app_asset_id,
    pr.source_kind::text AS source_kind,
    pr.source_name::text AS source_name,
    pr.asset_kind::text AS asset_kind,
    pr.external_id::text AS external_id,
    pr.parent_external_id::text AS parent_external_id,
    COALESCE(NULLIF(trim(pr.display_name), ''), pr.external_id)::text AS display_name,
    pr.status::text AS status,
    pr.governance_state::text AS governance_state,
    pr.governance_owner_identity_id::bigint AS governance_owner_identity_id,
    pr.governance_owner_display_name::text AS governance_owner_display_name,
    pr.governance_owner_primary_email::text AS governance_owner_primary_email,
    pr.evidence_last_seen_at::timestamptz AS evidence_last_seen_at,
    pr.evidence_freshness::text AS evidence_freshness
  FROM connected_app_read_models_v pr
),
connector_state AS (
  SELECT
    css.source_kind,
    css.source_name,
    css.fresh_until_at
  FROM connector_source_state css
),
identity_connector_state AS (
  SELECT
    ips.identity_id,
    css.fresh_until_at
  FROM identity_primary_source ips
  LEFT JOIN connector_state css
    ON css.source_kind = ips.source_kind
   AND lower(trim(css.source_name)) = lower(trim(ips.source_name))
),
asset_owner_candidates AS (
  SELECT
    aa.app_asset_id,
    COALESCE(owner_by_external.id, owner_by_email.id, 0)::bigint AS owner_identity_id,
    COALESCE(
      NULLIF(trim(owner_by_external.display_name), ''),
      NULLIF(trim(owner_by_email.display_name), ''),
      NULLIF(trim(aao.owner_display_name), ''),
      NULLIF(trim(aao.owner_email), ''),
      trim(aao.owner_external_id)
    )::text AS owner_display_name,
    COALESCE(
      NULLIF(trim(owner_by_external.primary_email), ''),
      NULLIF(trim(owner_by_email.primary_email), ''),
      NULLIF(lower(trim(aao.owner_email)), ''),
      ''
    )::text AS owner_primary_email,
    count(*) OVER (PARTITION BY aa.app_asset_id)::bigint AS owner_count,
    row_number() OVER (
      PARTITION BY aa.app_asset_id
      ORDER BY lower(
        COALESCE(
          NULLIF(trim(aao.owner_display_name), ''),
          NULLIF(trim(aao.owner_email), ''),
          trim(aao.owner_external_id)
        )
      ) ASC,
      aao.id ASC
    ) AS row_num
  FROM app_asset_owners aao
  JOIN active_assets aa ON aa.app_asset_id = aao.app_asset_id
  LEFT JOIN LATERAL (
    SELECT
      i.id,
      i.display_name,
      i.primary_email
    FROM active_accounts oa
    JOIN identity_accounts ia ON ia.account_id = oa.account_id
    JOIN identities i ON i.id = ia.identity_id
    WHERE oa.source_kind = aa.source_kind
      AND oa.source_name = aa.source_name
      AND lower(trim(oa.external_id)) = lower(trim(aao.owner_external_id))
    ORDER BY i.id ASC
    LIMIT 1
  ) owner_by_external ON TRUE
  LEFT JOIN LATERAL (
    SELECT
      i.id,
      i.display_name,
      i.primary_email
    FROM identities i
    WHERE lower(trim(i.primary_email)) = lower(trim(aao.owner_email))
    ORDER BY i.id ASC
    LIMIT 1
  ) owner_by_email ON TRUE
  WHERE aao.expired_at IS NULL
    AND aao.last_observed_run_id IS NOT NULL
),
asset_owner_choice AS (
  SELECT
    aoc.app_asset_id,
    aoc.owner_identity_id,
    CASE
      WHEN aoc.owner_count > 1 THEN aoc.owner_display_name || ' +' || (aoc.owner_count - 1)::text
      ELSE aoc.owner_display_name
    END::text AS owner_display_name,
    aoc.owner_primary_email::text AS owner_primary_email,
    aoc.owner_count::bigint AS owner_count
  FROM asset_owner_candidates aoc
  WHERE aoc.row_num = 1
),
asset_credential_stats AS (
  SELECT
    aa.app_asset_id,
    count(DISTINCT ca.id)::bigint AS linked_credentials_count,
    max(ca.last_used_at_source)::timestamptz AS credential_last_used_at,
    bool_or(
      credential_artifact_risk_level(
        ca.status,
        ca.credential_kind,
        ca.expires_at_source,
        ca.last_used_at_source,
        ca.created_by_external_id,
        ca.approved_by_external_id,
        now()
      ) = 'critical'
    )::boolean AS has_critical_credential,
    bool_or(
      credential_artifact_risk_level(
        ca.status,
        ca.credential_kind,
        ca.expires_at_source,
        ca.last_used_at_source,
        ca.created_by_external_id,
        ca.approved_by_external_id,
        now()
      ) IN ('critical', 'high')
    )::boolean AS has_high_risk_credential,
    bool_or(
      ca.expires_at_source IS NOT NULL
      AND ca.expires_at_source < now()
    )::boolean AS has_expired_credential,
    bool_or(
      ca.expires_at_source IS NOT NULL
      AND ca.expires_at_source >= now()
      AND ca.expires_at_source <= now() + interval '30 days'
    )::boolean AS has_expiring_credential,
    bool_or(
      ca.last_used_at_source IS NOT NULL
      AND ca.last_used_at_source <= now() - interval '90 days'
    )::boolean AS has_unused_credential
  FROM active_assets aa
  LEFT JOIN non_human_app_asset_credential_refs_v nhac
    ON nhac.app_asset_id = aa.app_asset_id
  LEFT JOIN credential_artifacts ca
    ON ca.source_kind = nhac.source_kind
   AND ca.source_name = nhac.source_name
   AND ca.asset_ref_kind = nhac.asset_ref_kind
   AND ca.asset_ref_external_id = nhac.asset_ref_external_id
   AND ca.expired_at IS NULL
   AND ca.last_observed_run_id IS NOT NULL
  GROUP BY aa.app_asset_id
),
identity_asset_stats AS (
  SELECT
    nhpal.principal_ref,
    nhpal.identity_id,
    count(DISTINCT nhpal.app_asset_id)::bigint AS linked_assets_count,
    max(aa.evidence_last_seen_at)::timestamptz AS asset_last_seen_at,
    max(acs.credential_last_used_at)::timestamptz AS credential_last_used_at,
    COALESCE(sum(acs.linked_credentials_count), 0)::bigint AS linked_credentials_count,
    bool_or(COALESCE(acs.has_critical_credential, false))::boolean AS has_critical_credential,
    bool_or(COALESCE(acs.has_high_risk_credential, false))::boolean AS has_high_risk_credential,
    bool_or(COALESCE(acs.has_expired_credential, false))::boolean AS has_expired_credential,
    bool_or(COALESCE(acs.has_expiring_credential, false))::boolean AS has_expiring_credential,
    bool_or(COALESCE(acs.has_unused_credential, false))::boolean AS has_unused_credential,
    bool_or(
      aa.evidence_freshness = 'stale'
      OR css.fresh_until_at IS NULL
      OR css.fresh_until_at < now()
    )::boolean AS has_stale_evidence
  FROM non_human_principal_asset_links_v nhpal
  JOIN active_assets aa ON aa.app_asset_id = nhpal.app_asset_id
  LEFT JOIN asset_credential_stats acs ON acs.app_asset_id = nhpal.app_asset_id
  LEFT JOIN connector_state css
    ON css.source_kind = aa.source_kind
   AND lower(trim(css.source_name)) = lower(trim(aa.source_name))
  WHERE nhpal.identity_id > 0
  GROUP BY nhpal.principal_ref, nhpal.identity_id
),
identity_governance_choice AS (
  SELECT DISTINCT ON (nhpal.identity_id)
    nhpal.identity_id,
    aa.governance_state,
    aa.governance_owner_identity_id,
    aa.governance_owner_display_name,
    aa.governance_owner_primary_email
  FROM non_human_principal_asset_links_v nhpal
  JOIN active_assets aa ON aa.app_asset_id = nhpal.app_asset_id
  WHERE nhpal.identity_id > 0
  ORDER BY
    nhpal.identity_id,
    CASE aa.governance_state
      WHEN 'action_required' THEN 0
      WHEN 'in_review' THEN 1
      WHEN 'ticketed' THEN 2
      WHEN 'approved' THEN 3
      ELSE 4
    END ASC,
    (aa.governance_owner_identity_id = 0) ASC,
    aa.app_asset_id ASC
),
identity_source_owner_choice AS (
  SELECT DISTINCT ON (nhpal.identity_id)
    nhpal.identity_id,
    aoc.owner_identity_id,
    aoc.owner_display_name,
    aoc.owner_primary_email
  FROM non_human_principal_asset_links_v nhpal
  JOIN asset_owner_choice aoc ON aoc.app_asset_id = nhpal.app_asset_id
  WHERE nhpal.identity_id > 0
  ORDER BY
    nhpal.identity_id,
    (NULLIF(trim(aoc.owner_display_name), '') IS NULL) ASC,
    aoc.app_asset_id ASC
),
identity_rows AS (
  SELECT
    'identity-' || isy.identity_id::text AS principal_ref,
    isy.identity_id::bigint AS identity_id,
    0::bigint AS app_asset_id,
    isy.principal_type::text AS principal_type,
    COALESCE(ips.source_kind, '')::text AS source_kind,
    COALESCE(ips.source_name, '')::text AS source_name,
    isy.display_name::text AS display_name,
    COALESCE(NULLIF(trim(isy.primary_email), ''), NULLIF(trim(ips.external_id), ''), '')::text AS secondary_name,
    COALESCE(ias.linked_assets_count, 0)::bigint AS linked_assets_count,
    COALESCE(ias.linked_credentials_count, 0)::bigint AS linked_credentials_count,
    NULLIF(
      GREATEST(
        COALESCE(isy.account_last_seen_at, '-infinity'::timestamptz),
        COALESCE(ias.asset_last_seen_at, '-infinity'::timestamptz),
        COALESCE(ias.credential_last_used_at, '-infinity'::timestamptz)
      ),
      '-infinity'::timestamptz
    )::timestamptz AS last_seen_at,
    COALESCE(igc.governance_state, 'unreviewed')::text AS governance_state,
    CASE
      WHEN COALESCE(igc.governance_owner_identity_id, 0) > 0 THEN igc.governance_owner_identity_id
      WHEN COALESCE(NULLIF(trim(isoc.owner_display_name), ''), NULLIF(trim(isoc.owner_primary_email), '')) IS NOT NULL THEN isoc.owner_identity_id
      ELSE 0
    END::bigint AS accountable_owner_identity_id,
    CASE
      WHEN COALESCE(igc.governance_owner_identity_id, 0) > 0 THEN COALESCE(
        NULLIF(trim(igc.governance_owner_display_name), ''),
        NULLIF(trim(igc.governance_owner_primary_email), ''),
        ''
      )
      ELSE COALESCE(
        NULLIF(trim(isoc.owner_display_name), ''),
        NULLIF(trim(isoc.owner_primary_email), ''),
        ''
      )
    END::text AS accountable_owner_display_name,
    CASE
      WHEN COALESCE(igc.governance_owner_identity_id, 0) > 0 THEN COALESCE(NULLIF(trim(igc.governance_owner_primary_email), ''), '')
      ELSE COALESCE(NULLIF(trim(isoc.owner_primary_email), ''), '')
    END::text AS accountable_owner_primary_email,
    CASE
      WHEN COALESCE(igc.governance_owner_identity_id, 0) > 0
        OR COALESCE(NULLIF(trim(isoc.owner_display_name), ''), NULLIF(trim(isoc.owner_primary_email), '')) IS NOT NULL
        THEN 'owned'
      ELSE 'unknown'
    END::text AS owner_presence,
    COALESCE(ias.has_critical_credential, false)::boolean AS has_critical_credential,
    COALESCE(ias.has_high_risk_credential, false)::boolean AS has_high_risk_credential,
    COALESCE(ias.has_expired_credential, false)::boolean AS has_expired_credential,
    COALESCE(ias.has_expiring_credential, false)::boolean AS has_expiring_credential,
    COALESCE(ias.has_unused_credential, false)::boolean AS has_unused_credential,
    (
      COALESCE(ias.has_stale_evidence, false)
      OR ics.fresh_until_at IS NULL
      OR ics.fresh_until_at < now()
    )::boolean AS has_stale_evidence
  FROM identity_summary isy
  LEFT JOIN identity_primary_source ips ON ips.identity_id = isy.identity_id
  LEFT JOIN identity_asset_stats ias ON ias.identity_id = isy.identity_id
  LEFT JOIN identity_governance_choice igc ON igc.identity_id = isy.identity_id
  LEFT JOIN identity_source_owner_choice isoc ON isoc.identity_id = isy.identity_id
  LEFT JOIN identity_connector_state ics ON ics.identity_id = isy.identity_id
),
asset_linked_to_identity AS (
  SELECT DISTINCT nhpal.app_asset_id
  FROM non_human_principal_asset_links_v nhpal
  WHERE nhpal.identity_id > 0
),
asset_rows AS (
  SELECT
    'app-asset-' || aa.app_asset_id::text AS principal_ref,
    0::bigint AS identity_id,
    aa.app_asset_id::bigint AS app_asset_id,
    CASE
      WHEN aa.asset_kind = 'entra_service_principal' THEN 'service'
      ELSE 'app'
    END::text AS principal_type,
    aa.source_kind::text AS source_kind,
    aa.source_name::text AS source_name,
    aa.display_name::text AS display_name,
    aa.external_id::text AS secondary_name,
    1::bigint AS linked_assets_count,
    COALESCE(acs.linked_credentials_count, 0)::bigint AS linked_credentials_count,
    NULLIF(
      GREATEST(
        COALESCE(aa.evidence_last_seen_at, '-infinity'::timestamptz),
        COALESCE(acs.credential_last_used_at, '-infinity'::timestamptz)
      ),
      '-infinity'::timestamptz
    )::timestamptz AS last_seen_at,
    aa.governance_state::text AS governance_state,
    CASE
      WHEN COALESCE(aa.governance_owner_identity_id, 0) > 0 THEN aa.governance_owner_identity_id
      WHEN COALESCE(NULLIF(trim(aoc.owner_display_name), ''), NULLIF(trim(aoc.owner_primary_email), '')) IS NOT NULL THEN aoc.owner_identity_id
      ELSE 0
    END::bigint AS accountable_owner_identity_id,
    CASE
      WHEN COALESCE(aa.governance_owner_identity_id, 0) > 0 THEN COALESCE(
        NULLIF(trim(aa.governance_owner_display_name), ''),
        NULLIF(trim(aa.governance_owner_primary_email), ''),
        ''
      )
      ELSE COALESCE(
        NULLIF(trim(aoc.owner_display_name), ''),
        NULLIF(trim(aoc.owner_primary_email), ''),
        ''
      )
    END::text AS accountable_owner_display_name,
    CASE
      WHEN COALESCE(aa.governance_owner_identity_id, 0) > 0 THEN COALESCE(NULLIF(trim(aa.governance_owner_primary_email), ''), '')
      ELSE COALESCE(NULLIF(trim(aoc.owner_primary_email), ''), '')
    END::text AS accountable_owner_primary_email,
    CASE
      WHEN COALESCE(aa.governance_owner_identity_id, 0) > 0
        OR COALESCE(NULLIF(trim(aoc.owner_display_name), ''), NULLIF(trim(aoc.owner_primary_email), '')) IS NOT NULL
        THEN 'owned'
      ELSE 'unknown'
    END::text AS owner_presence,
    COALESCE(acs.has_critical_credential, false)::boolean AS has_critical_credential,
    COALESCE(acs.has_high_risk_credential, false)::boolean AS has_high_risk_credential,
    COALESCE(acs.has_expired_credential, false)::boolean AS has_expired_credential,
    COALESCE(acs.has_expiring_credential, false)::boolean AS has_expiring_credential,
    COALESCE(acs.has_unused_credential, false)::boolean AS has_unused_credential,
    (
      aa.evidence_freshness = 'stale'
      OR css.fresh_until_at IS NULL
      OR css.fresh_until_at < now()
    )::boolean AS has_stale_evidence
  FROM active_assets aa
  LEFT JOIN asset_linked_to_identity ali ON ali.app_asset_id = aa.app_asset_id
  LEFT JOIN asset_owner_choice aoc ON aoc.app_asset_id = aa.app_asset_id
  LEFT JOIN asset_credential_stats acs ON acs.app_asset_id = aa.app_asset_id
  LEFT JOIN connector_state css
    ON css.source_kind = aa.source_kind
   AND lower(trim(css.source_name)) = lower(trim(aa.source_name))
  WHERE ali.app_asset_id IS NULL
),
base AS (
  SELECT *
  FROM identity_rows
  UNION ALL
  SELECT *
  FROM asset_rows
),
scored AS (
  SELECT
    b.*,
    CASE
      WHEN b.last_seen_at IS NULL THEN 'never_seen'
      WHEN b.last_seen_at >= now() - interval '30 days' THEN 'recent'
      WHEN b.last_seen_at >= now() - interval '90 days' THEN 'aging'
      ELSE 'stale'
    END::text AS activity_state,
    CASE
      WHEN b.has_stale_evidence THEN 'stale'
      WHEN b.last_seen_at IS NULL THEN 'unknown'
      ELSE 'current'
    END::text AS freshness_state,
    (
      CASE WHEN b.owner_presence = 'unknown' THEN 1 ELSE 0 END
      + CASE WHEN b.has_high_risk_credential THEN 1 ELSE 0 END
      + CASE WHEN b.has_expired_credential THEN 1 ELSE 0 END
      + CASE WHEN b.has_expiring_credential THEN 1 ELSE 0 END
      + CASE WHEN b.has_unused_credential THEN 1 ELSE 0 END
      + CASE WHEN b.has_stale_evidence THEN 1 ELSE 0 END
      + CASE
          WHEN b.governance_state = 'unreviewed'
            AND (
              b.has_high_risk_credential
              OR b.has_expired_credential
              OR b.has_expiring_credential
              OR b.has_unused_credential
              OR b.has_stale_evidence
            )
            THEN 1
          ELSE 0
        END
    )::int AS risk_reason_count,
    CASE
      WHEN b.has_critical_credential THEN 'critical'
      WHEN b.owner_presence = 'unknown'
        OR b.has_high_risk_credential
        OR b.has_expired_credential
        OR (
          b.governance_state = 'unreviewed'
          AND (
            b.has_high_risk_credential
            OR b.has_expired_credential
            OR b.has_expiring_credential
            OR b.has_unused_credential
            OR b.has_stale_evidence
          )
        )
        THEN 'high'
      WHEN b.has_expiring_credential
        OR b.has_unused_credential
        OR b.has_stale_evidence
        THEN 'medium'
      ELSE 'low'
    END::text AS risk_level
  FROM base b
)
SELECT
  s.principal_ref::text AS principal_ref,
  s.identity_id,
  s.app_asset_id,
  s.principal_type,
  s.source_kind,
  s.source_name,
  s.display_name,
  s.secondary_name,
  s.linked_assets_count,
  s.linked_credentials_count,
  s.last_seen_at,
  s.activity_state,
  s.freshness_state,
  s.governance_state,
  s.accountable_owner_identity_id,
  s.accountable_owner_display_name,
  s.accountable_owner_primary_email,
  s.owner_presence,
  s.has_critical_credential,
  s.has_high_risk_credential,
  s.has_expired_credential,
  s.has_expiring_credential,
  s.has_unused_credential,
  s.has_stale_evidence,
  s.risk_reason_count,
  s.risk_level
FROM scored s;

CREATE OR REPLACE VIEW non_human_principal_read_models_v AS
SELECT
  nhp.principal_ref,
  nhp.identity_id,
  nhp.app_asset_id,
  nhp.principal_type,
  nhp.source_kind,
  nhp.source_name,
  nhp.display_name,
  nhp.secondary_name,
  nhp.linked_assets_count,
  nhp.linked_credentials_count,
  nhp.last_seen_at,
  nhp.activity_state,
  nhp.freshness_state,
  nhp.governance_state,
  nhp.accountable_owner_identity_id,
  nhp.accountable_owner_display_name,
  nhp.accountable_owner_primary_email,
  nhp.owner_presence,
  nhp.has_critical_credential,
  nhp.has_high_risk_credential,
  nhp.has_expired_credential,
  nhp.has_expiring_credential,
  nhp.has_unused_credential,
  nhp.has_stale_evidence,
  nhp.risk_reason_count,
  nhp.risk_level
FROM non_human_principals nhp;

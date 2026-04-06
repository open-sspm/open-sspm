CREATE OR REPLACE VIEW connected_app_summaries_v AS
SELECT
  aa.id,
  aa.source_kind,
  aa.source_name,
  aa.asset_kind,
  aa.external_id,
  aa.parent_external_id,
  aa.display_name,
  aa.status,
  aa.created_at_source,
  aa.updated_at_source,
  aa.raw_json,
  aa.seen_in_run_id,
  aa.seen_at,
  aa.last_observed_run_id,
  aa.last_observed_at,
  aa.expired_at,
  aa.expired_run_id,
  aa.created_at,
  aa.updated_at,
  COALESCE(cag.review_state, 'unreviewed')::text AS review_state,
  COALESCE(cag.ticket_ref, '')::text AS ticket_ref,
  COALESCE(cag.notes, '')::text AS notes,
  COALESCE(cag.owner_identity_id, 0)::bigint AS review_owner_identity_id,
  COALESCE(owner.display_name, '')::text AS review_owner_display_name,
  COALESCE(owner.primary_email, '')::text AS review_owner_primary_email,
  COALESCE(owner.kind, '')::text AS review_owner_kind,
  COALESCE(owner_counts.owner_count, 0)::bigint AS owner_count,
  COALESCE(grant_counts.grant_count, 0)::bigint AS grant_count,
  COALESCE(grant_counts.actor_count, 0)::bigint AS actor_count,
  COALESCE(discovery_counts.discovery_source_count, 0)::bigint AS discovery_source_count,
  COALESCE(discovery_counts.discovery_event_count_30d, 0)::bigint AS discovery_event_count_30d,
  COALESCE(discovery_counts.last_evidence_at, aa.last_observed_at)::timestamptz AS evidence_last_seen_at
FROM app_assets aa
LEFT JOIN connected_app_governance cag ON cag.app_asset_id = aa.id
LEFT JOIN identities owner ON owner.id = cag.owner_identity_id
LEFT JOIN LATERAL (
  SELECT count(*)::bigint AS owner_count
  FROM app_asset_owners aao
  WHERE aao.app_asset_id = aa.id
    AND aao.expired_at IS NULL
    AND aao.last_observed_run_id IS NOT NULL
) owner_counts ON TRUE
LEFT JOIN LATERAL (
  SELECT
    count(*)::bigint AS grant_count,
    count(
      DISTINCT COALESCE(
        NULLIF(trim(ca.created_by_external_id), ''),
        NULLIF(lower(trim(ca.created_by_display_name)), '')
      )
    )::bigint AS actor_count
  FROM credential_artifacts ca
  WHERE ca.source_kind = aa.source_kind
    AND ca.source_name = aa.source_name
    AND ca.asset_ref_kind = aa.asset_kind
    AND ca.asset_ref_external_id = (aa.asset_kind || ':' || aa.external_id)
    AND ca.expired_at IS NULL
    AND ca.last_observed_run_id IS NOT NULL
) grant_counts ON TRUE
LEFT JOIN LATERAL (
  SELECT
    source_counts.discovery_source_count,
    event_counts.discovery_event_count_30d,
    NULLIF(
      GREATEST(
        COALESCE(source_counts.last_source_seen_at, '-infinity'::timestamptz),
        COALESCE(event_counts.last_event_at, '-infinity'::timestamptz)
      ),
      '-infinity'::timestamptz
    )::timestamptz AS last_evidence_at
  FROM (
    SELECT
      count(*)::bigint AS discovery_source_count,
      NULLIF(
        GREATEST(
          COALESCE(max(sas.last_observed_at), '-infinity'::timestamptz),
          COALESCE(max(sas.seen_at), '-infinity'::timestamptz)
        ),
        '-infinity'::timestamptz
      )::timestamptz AS last_source_seen_at
    FROM saas_app_sources sas
    WHERE sas.source_kind = aa.source_kind
      AND sas.source_name = aa.source_name
      AND sas.source_app_id = aa.external_id
      AND sas.expired_at IS NULL
      AND sas.last_observed_run_id IS NOT NULL
  ) source_counts
  CROSS JOIN (
    SELECT
      count(*) FILTER (
        WHERE e.observed_at >= now() - interval '30 days'
      )::bigint AS discovery_event_count_30d,
      max(e.observed_at)::timestamptz AS last_event_at
    FROM saas_app_events e
    WHERE e.source_kind = aa.source_kind
      AND e.source_name = aa.source_name
      AND e.source_app_id = aa.external_id
      AND e.expired_at IS NULL
      AND e.last_observed_run_id IS NOT NULL
  ) event_counts
) discovery_counts ON TRUE
WHERE aa.expired_at IS NULL
  AND aa.last_observed_run_id IS NOT NULL;

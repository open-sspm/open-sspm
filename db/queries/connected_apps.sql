-- name: CountConnectedAppsBySourceAndQueryAndReviewState :one
SELECT count(*)
FROM app_assets aa
LEFT JOIN connected_app_governance cag ON cag.app_asset_id = aa.id
WHERE aa.source_kind = sqlc.arg(source_kind)::text
  AND aa.source_name = sqlc.arg(source_name)::text
  AND aa.asset_kind = sqlc.arg(asset_kind)::text
  AND aa.expired_at IS NULL
  AND aa.last_observed_run_id IS NOT NULL
  AND (
    sqlc.arg(review_state)::text = ''
    OR COALESCE(cag.review_state, 'unreviewed') = sqlc.arg(review_state)::text
  )
  AND (
    sqlc.arg(query)::text = ''
    OR aa.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR aa.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
  );

-- name: CountConnectedAppsGroupedByReviewState :many
SELECT
  COALESCE(cag.review_state, 'unreviewed')::text AS review_state,
  count(*)::bigint AS app_count
FROM app_assets aa
LEFT JOIN connected_app_governance cag ON cag.app_asset_id = aa.id
WHERE aa.source_kind = sqlc.arg(source_kind)::text
  AND aa.source_name = sqlc.arg(source_name)::text
  AND aa.asset_kind = sqlc.arg(asset_kind)::text
  AND aa.expired_at IS NULL
  AND aa.last_observed_run_id IS NOT NULL
  AND (
    sqlc.arg(query)::text = ''
    OR aa.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR aa.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
GROUP BY COALESCE(cag.review_state, 'unreviewed')
ORDER BY CASE COALESCE(cag.review_state, 'unreviewed')
  WHEN 'needs_revocation' THEN 0
  WHEN 'under_review' THEN 1
  WHEN 'unreviewed' THEN 2
  WHEN 'ticketed' THEN 3
  WHEN 'sanctioned' THEN 4
  ELSE 5
END;

-- name: ListConnectedAppsPageBySourceAndQueryAndReviewState :many
SELECT
  aa.*,
  COALESCE(cag.review_state, 'unreviewed')::text AS review_state,
  COALESCE(cag.ticket_ref, '')::text AS ticket_ref,
  COALESCE(cag.notes, '')::text AS notes,
  COALESCE(cag.owner_identity_id, 0)::bigint AS review_owner_identity_id,
  COALESCE(owner.display_name, '')::text AS review_owner_display_name,
  COALESCE(owner.primary_email, '')::text AS review_owner_primary_email,
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
WHERE aa.source_kind = sqlc.arg(source_kind)::text
  AND aa.source_name = sqlc.arg(source_name)::text
  AND aa.asset_kind = sqlc.arg(asset_kind)::text
  AND aa.expired_at IS NULL
  AND aa.last_observed_run_id IS NOT NULL
  AND (
    sqlc.arg(review_state)::text = ''
    OR COALESCE(cag.review_state, 'unreviewed') = sqlc.arg(review_state)::text
  )
  AND (
    sqlc.arg(query)::text = ''
    OR aa.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR aa.external_id ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
ORDER BY
  CASE COALESCE(cag.review_state, 'unreviewed')
    WHEN 'needs_revocation' THEN 0
    WHEN 'under_review' THEN 1
    WHEN 'unreviewed' THEN 2
    WHEN 'ticketed' THEN 3
    WHEN 'sanctioned' THEN 4
    ELSE 5
  END ASC,
  COALESCE(discovery_counts.discovery_event_count_30d, 0) DESC,
  COALESCE(discovery_counts.last_evidence_at, aa.last_observed_at) DESC,
  lower(COALESCE(NULLIF(trim(aa.display_name), ''), aa.external_id)) ASC,
  aa.id ASC
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: GetConnectedAppSummaryByID :one
SELECT
  aa.*,
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
WHERE aa.id = sqlc.arg(id)::bigint
  AND aa.expired_at IS NULL
  AND aa.last_observed_run_id IS NOT NULL;

-- name: UpsertConnectedAppGovernance :one
INSERT INTO connected_app_governance (
  app_asset_id,
  review_state,
  owner_identity_id,
  ticket_ref,
  notes,
  updated_by_auth_user_id,
  updated_at
)
VALUES (
  sqlc.arg(app_asset_id)::bigint,
  sqlc.arg(review_state)::text,
  sqlc.narg(owner_identity_id)::bigint,
  sqlc.arg(ticket_ref)::text,
  sqlc.arg(notes)::text,
  sqlc.narg(updated_by_auth_user_id)::bigint,
  now()
)
ON CONFLICT (app_asset_id) DO UPDATE SET
  review_state = EXCLUDED.review_state,
  owner_identity_id = EXCLUDED.owner_identity_id,
  ticket_ref = EXCLUDED.ticket_ref,
  notes = EXCLUDED.notes,
  updated_by_auth_user_id = EXCLUDED.updated_by_auth_user_id,
  updated_at = now()
RETURNING *;

-- name: ListConnectedAppDiscoverySourcesBySourceAppID :many
WITH scoped_sources AS (
  SELECT *
  FROM saas_app_sources sas
  WHERE sas.source_kind = sqlc.arg(source_kind)::text
    AND sas.source_name = sqlc.arg(source_name)::text
    AND sas.source_app_id = sqlc.arg(source_app_id)::text
    AND sas.expired_at IS NULL
    AND sas.last_observed_run_id IS NOT NULL
),
posture_inputs AS (
  SELECT
    spi.*,
    CASE spi.bound_connector_kind
      WHEN 'okta' THEN sqlc.arg(okta_fresh_after)::timestamptz
      WHEN 'entra' THEN sqlc.arg(entra_fresh_after)::timestamptz
      WHEN 'google_workspace' THEN sqlc.arg(google_workspace_fresh_after)::timestamptz
      WHEN 'github' THEN sqlc.arg(github_fresh_after)::timestamptz
      WHEN 'datadog' THEN sqlc.arg(datadog_fresh_after)::timestamptz
      WHEN 'aws_identity_center' THEN sqlc.arg(aws_fresh_after)::timestamptz
      ELSE sqlc.arg(default_fresh_after)::timestamptz
    END AS fresh_after
  FROM saas_app_posture_inputs_v spi
  JOIN (
    SELECT DISTINCT saas_app_id
    FROM scoped_sources
  ) scoped_app_ids ON scoped_app_ids.saas_app_id = spi.id
),
posture_state AS (
  SELECT
    pi.*,
    CASE
      WHEN pi.bound_connector_kind = '' OR pi.bound_connector_source_name = '' THEN 'unmanaged'
      WHEN NOT pi.connector_configured THEN 'unmanaged'
      WHEN NOT pi.connector_enabled THEN 'unmanaged'
      WHEN pi.last_success_at IS NULL THEN 'unmanaged'
      WHEN pi.last_success_at < pi.fresh_after THEN 'unmanaged'
      ELSE 'managed'
    END AS managed_state
  FROM posture_inputs pi
),
posture_with_risk AS (
  SELECT
    ps.*,
    LEAST(100,
      CASE WHEN ps.managed_state <> 'managed' THEN 45 ELSE 0 END
      + CASE WHEN ps.has_privileged_scope THEN 20 ELSE 0 END
      + CASE WHEN ps.owner_identity_id = 0 THEN 15 ELSE 0 END
      + CASE WHEN ps.actors_30d >= 50 THEN 10 ELSE 0 END
      + CASE WHEN ps.managed_state <> 'managed' AND ps.effective_business_criticality IN ('high', 'critical') THEN 10 ELSE 0 END
      + CASE WHEN ps.managed_state <> 'managed' AND ps.effective_data_classification IN ('confidential', 'restricted') THEN 5 ELSE 0 END
    )::int AS risk_score
  FROM posture_state ps
),
posture_rows AS (
  SELECT
    pwr.*,
    CASE
      WHEN pwr.risk_score >= 80 THEN 'critical'
      WHEN pwr.risk_score >= 60 THEN 'high'
      WHEN pwr.risk_score >= 30 THEN 'medium'
      ELSE 'low'
    END AS risk_level
  FROM posture_with_risk pwr
)
SELECT
  sas.*,
  pr.canonical_key,
  pr.display_name AS discovery_display_name,
  pr.primary_domain AS discovery_primary_domain,
  pr.vendor_name AS discovery_vendor_name,
  pr.managed_state AS discovery_managed_state,
  pr.risk_level AS discovery_risk_level
FROM scoped_sources sas
JOIN posture_rows pr ON pr.id = sas.saas_app_id
ORDER BY sas.last_observed_at DESC, sas.id DESC;

-- name: ListConnectedAppDiscoveryEventsBySourceAppID :many
SELECT *
FROM saas_app_events e
WHERE e.source_kind = sqlc.arg(source_kind)::text
  AND e.source_name = sqlc.arg(source_name)::text
  AND e.source_app_id = sqlc.arg(source_app_id)::text
  AND e.expired_at IS NULL
  AND e.last_observed_run_id IS NOT NULL
ORDER BY e.observed_at DESC, e.id DESC
LIMIT sqlc.arg(limit_rows)::int;

CREATE OR REPLACE VIEW discovery_app_read_models_v AS
WITH primary_bindings AS (
  SELECT DISTINCT ON (b.saas_app_id)
    b.saas_app_id,
    lower(trim(b.connector_kind)) AS connector_kind,
    trim(b.connector_source_name) AS connector_source_name
  FROM saas_app_bindings b
  WHERE b.is_primary
  ORDER BY b.saas_app_id, b.id ASC
),
posture_base AS (
  SELECT
    sa.id,
    sa.canonical_key,
    sa.display_name,
    sa.primary_domain,
    sa.vendor_name,
    sa.first_seen_at,
    sa.last_seen_at,
    sa.created_at,
    sa.updated_at,
    COALESCE(go.owner_identity_id, 0)::bigint AS owner_identity_id,
    COALESCE(owner.display_name, '')::text AS owner_display_name,
    COALESCE(owner.primary_email, '')::text AS owner_primary_email,
    COALESCE(go.review_owner_identity_id, 0)::bigint AS review_owner_identity_id,
    COALESCE(review_owner.display_name, '')::text AS review_owner_display_name,
    COALESCE(review_owner.primary_email, '')::text AS review_owner_primary_email,
    sa.actors_30d::bigint AS actors_30d,
    sa.has_privileged_scope::boolean AS has_privileged_scope,
    sa.has_confidential_scope::boolean AS has_confidential_scope,
    COALESCE(pb.connector_kind, '')::text AS bound_connector_kind,
    COALESCE(pb.connector_source_name, '')::text AS bound_connector_source_name,
    COALESCE(css.enabled, false)::boolean AS connector_enabled,
    COALESCE(css.configured, false)::boolean AS connector_configured,
    css.last_success_at::timestamptz AS last_success_at,
    css.fresh_until_at::timestamptz AS fresh_until_at,
    CASE
      WHEN sa.actors_30d >= 200 THEN 'critical'
      WHEN sa.actors_30d >= 50 OR sa.has_privileged_scope THEN 'high'
      WHEN sa.actors_30d >= 10 THEN 'medium'
      ELSE 'low'
    END AS suggested_business_criticality,
    CASE
      WHEN sa.has_privileged_scope THEN 'restricted'
      WHEN sa.has_confidential_scope THEN 'confidential'
      ELSE 'internal'
    END AS suggested_data_classification,
    CASE lower(trim(COALESCE(go.business_criticality, '')))
      WHEN 'low' THEN 'low'
      WHEN 'medium' THEN 'medium'
      WHEN 'high' THEN 'high'
      WHEN 'critical' THEN 'critical'
      ELSE CASE
        WHEN sa.actors_30d >= 200 THEN 'critical'
        WHEN sa.actors_30d >= 50 OR sa.has_privileged_scope THEN 'high'
        WHEN sa.actors_30d >= 10 THEN 'medium'
        ELSE 'low'
      END
    END AS effective_business_criticality,
    CASE lower(trim(COALESCE(go.data_classification, '')))
      WHEN 'public' THEN 'public'
      WHEN 'internal' THEN 'internal'
      WHEN 'confidential' THEN 'confidential'
      WHEN 'restricted' THEN 'restricted'
      ELSE CASE
        WHEN sa.has_privileged_scope THEN 'restricted'
        WHEN sa.has_confidential_scope THEN 'confidential'
        ELSE 'internal'
      END
    END AS effective_data_classification,
    COALESCE(go.governance_state, 'unreviewed')::text AS governance_state,
    COALESCE(go.review_disposition, 'unreviewed')::text AS review_disposition,
    COALESCE(go.ticket_ref, '')::text AS ticket_ref,
    COALESCE(go.notes, '')::text AS notes,
    go.follow_up_due_date::date AS follow_up_due_date,
    COALESCE(go.follow_up_due_date < CURRENT_DATE, false)::boolean AS is_follow_up_overdue,
    COALESCE(go.replacement_saas_app_id, 0)::bigint AS replacement_saas_app_id,
    COALESCE(NULLIF(trim(replacement.display_name), ''), replacement.canonical_key, '')::text AS replacement_display_name,
    COALESCE(replacement.primary_domain, '')::text AS replacement_primary_domain
  FROM saas_apps sa
  LEFT JOIN governance_subject_overrides go
    ON go.subject_kind = 'saas_app'
   AND go.subject_id = sa.id
  LEFT JOIN identities owner
    ON owner.id = go.owner_identity_id
  LEFT JOIN identities review_owner
    ON review_owner.id = go.review_owner_identity_id
  LEFT JOIN saas_apps replacement
    ON replacement.id = go.replacement_saas_app_id
  LEFT JOIN primary_bindings pb
    ON pb.saas_app_id = sa.id
  LEFT JOIN connector_source_state css
    ON css.source_kind = CASE COALESCE(pb.connector_kind, '')
      WHEN 'aws_identity_center' THEN 'aws'
      ELSE COALESCE(pb.connector_kind, '')
    END
   AND lower(trim(css.source_name)) = lower(COALESCE(pb.connector_source_name, ''))
),
posture_state AS (
  SELECT
    pb.*,
    CASE
      WHEN pb.bound_connector_kind = '' OR pb.bound_connector_source_name = '' THEN 'unmanaged'
      WHEN NOT pb.connector_configured THEN 'unmanaged'
      WHEN NOT pb.connector_enabled THEN 'unmanaged'
      WHEN pb.last_success_at IS NULL THEN 'unmanaged'
      WHEN pb.fresh_until_at IS NULL THEN 'unmanaged'
      WHEN pb.fresh_until_at < now() THEN 'unmanaged'
      ELSE 'managed'
    END AS managed_state,
    CASE
      WHEN pb.bound_connector_kind = '' OR pb.bound_connector_source_name = '' THEN 'no_binding'
      WHEN NOT pb.connector_configured THEN 'connector_not_configured'
      WHEN NOT pb.connector_enabled THEN 'connector_disabled'
      WHEN pb.last_success_at IS NULL THEN 'stale_sync'
      WHEN pb.fresh_until_at IS NULL THEN 'stale_sync'
      WHEN pb.fresh_until_at < now() THEN 'stale_sync'
      ELSE 'active_binding_fresh_sync'
    END AS managed_reason
  FROM posture_base pb
),
scored AS (
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
)
SELECT
  scored.*,
  CASE
    WHEN scored.risk_score >= 80 THEN 'critical'
    WHEN scored.risk_score >= 60 THEN 'high'
    WHEN scored.risk_score >= 30 THEN 'medium'
    ELSE 'low'
  END AS risk_level
FROM scored;

DROP TABLE IF EXISTS saas_app_risk_read_models;

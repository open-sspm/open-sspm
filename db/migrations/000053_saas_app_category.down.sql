DROP VIEW IF EXISTS discovery_app_read_models_v;

CREATE VIEW discovery_app_read_models_v AS
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
    COALESCE(risk.suggested_business_criticality, 'low')::text AS suggested_business_criticality,
    COALESCE(risk.suggested_data_classification, 'internal')::text AS suggested_data_classification,
    COALESCE(
      risk.effective_business_criticality,
      CASE lower(trim(COALESCE(go.business_criticality, '')))
        WHEN 'low' THEN 'low'
        WHEN 'medium' THEN 'medium'
        WHEN 'high' THEN 'high'
        WHEN 'critical' THEN 'critical'
        ELSE 'low'
      END
    )::text AS effective_business_criticality,
    COALESCE(
      risk.effective_data_classification,
      CASE lower(trim(COALESCE(go.data_classification, '')))
        WHEN 'public' THEN 'public'
        WHEN 'internal' THEN 'internal'
        WHEN 'confidential' THEN 'confidential'
        WHEN 'restricted' THEN 'restricted'
        ELSE 'internal'
      END
    )::text AS effective_data_classification,
    COALESCE(go.governance_state, 'unreviewed')::text AS governance_state,
    COALESCE(go.review_disposition, 'unreviewed')::text AS review_disposition,
    COALESCE(go.ticket_ref, '')::text AS ticket_ref,
    COALESCE(go.notes, '')::text AS notes,
    go.follow_up_due_date::date AS follow_up_due_date,
    COALESCE(go.follow_up_due_date < CURRENT_DATE, false)::boolean AS is_follow_up_overdue,
    COALESCE(go.replacement_saas_app_id, 0)::bigint AS replacement_saas_app_id,
    COALESCE(NULLIF(trim(replacement.display_name), ''), replacement.canonical_key, '')::text AS replacement_display_name,
    COALESCE(replacement.primary_domain, '')::text AS replacement_primary_domain,
    COALESCE(risk.risk_score, 0)::int AS risk_score,
    COALESCE(risk.risk_level, 'low')::text AS risk_level
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
  LEFT JOIN saas_app_risk_read_models risk
    ON risk.saas_app_id = sa.id
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
)
SELECT
  ps.id,
  ps.canonical_key,
  ps.display_name,
  ps.primary_domain,
  ps.vendor_name,
  ps.first_seen_at,
  ps.last_seen_at,
  ps.created_at,
  ps.updated_at,
  ps.owner_identity_id,
  ps.owner_display_name,
  ps.owner_primary_email,
  ps.review_owner_identity_id,
  ps.review_owner_display_name,
  ps.review_owner_primary_email,
  ps.actors_30d,
  ps.has_privileged_scope,
  ps.has_confidential_scope,
  ps.bound_connector_kind,
  ps.bound_connector_source_name,
  ps.connector_enabled,
  ps.connector_configured,
  ps.last_success_at,
  ps.fresh_until_at,
  ps.suggested_business_criticality,
  ps.suggested_data_classification,
  ps.effective_business_criticality,
  ps.effective_data_classification,
  ps.governance_state,
  ps.review_disposition,
  ps.ticket_ref,
  ps.notes,
  ps.follow_up_due_date,
  ps.is_follow_up_overdue,
  ps.replacement_saas_app_id,
  ps.replacement_display_name,
  ps.replacement_primary_domain,
  ps.managed_state,
  ps.managed_reason,
  ps.risk_score,
  ps.risk_level
FROM posture_state ps;

ALTER TABLE saas_apps
  DROP COLUMN IF EXISTS category;

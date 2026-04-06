CREATE TABLE IF NOT EXISTS connector_source_state (
  source_kind TEXT NOT NULL,
  source_name TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT false,
  configured BOOLEAN NOT NULL DEFAULT false,
  discovery_enabled BOOLEAN NOT NULL DEFAULT false,
  last_success_at TIMESTAMPTZ,
  fresh_until_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (source_kind, source_name)
);

CREATE INDEX IF NOT EXISTS idx_connector_source_state_discovery_scope
  ON connector_source_state (configured, discovery_enabled, source_kind, source_name);

CREATE INDEX IF NOT EXISTS idx_connector_source_state_updated_at
  ON connector_source_state (updated_at);

ALTER TABLE saas_apps
  ADD COLUMN IF NOT EXISTS actors_30d BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS has_privileged_scope BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS has_confidential_scope BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS projection_refreshed_at TIMESTAMPTZ;

ALTER TABLE app_assets
  ADD COLUMN IF NOT EXISTS owner_count BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS grant_count BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS actor_count BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS discovery_source_count BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS discovery_event_count_30d BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS evidence_last_seen_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS projection_refreshed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_saas_app_sources_active_source_app_id
  ON saas_app_sources (source_kind, source_name, source_app_id)
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_saas_app_events_active_source_app_id_observed_at_desc
  ON saas_app_events (source_kind, source_name, source_app_id, observed_at DESC)
  WHERE expired_at IS NULL AND last_observed_run_id IS NOT NULL;

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
    COALESCE(go.ticket_ref, '')::text AS ticket_ref,
    COALESCE(go.notes, '')::text AS notes
  FROM saas_apps sa
  LEFT JOIN governance_subject_overrides go
    ON go.subject_kind = 'saas_app'
   AND go.subject_id = sa.id
  LEFT JOIN identities owner
    ON owner.id = go.owner_identity_id
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

CREATE OR REPLACE VIEW connected_app_read_models_v AS
WITH posture_base AS (
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
    COALESCE(go.governance_state, 'unreviewed')::text AS governance_state,
    COALESCE(go.ticket_ref, '')::text AS ticket_ref,
    COALESCE(go.notes, '')::text AS notes,
    COALESCE(go.owner_identity_id, 0)::bigint AS governance_owner_identity_id,
    COALESCE(owner.display_name, '')::text AS governance_owner_display_name,
    COALESCE(owner.primary_email, '')::text AS governance_owner_primary_email,
    COALESCE(owner.kind, '')::text AS governance_owner_kind,
    aa.owner_count::bigint AS owner_count,
    aa.grant_count::bigint AS grant_count,
    aa.actor_count::bigint AS actor_count,
    aa.discovery_source_count::bigint AS discovery_source_count,
    aa.discovery_event_count_30d::bigint AS discovery_event_count_30d,
    COALESCE(aa.evidence_last_seen_at, aa.last_observed_at)::timestamptz AS evidence_last_seen_at,
    'unknown'::text AS suggested_business_criticality,
    'unknown'::text AS suggested_data_classification,
    CASE lower(trim(COALESCE(go.business_criticality, '')))
      WHEN 'low' THEN 'low'
      WHEN 'medium' THEN 'medium'
      WHEN 'high' THEN 'high'
      WHEN 'critical' THEN 'critical'
      ELSE 'unknown'
    END AS effective_business_criticality,
    CASE lower(trim(COALESCE(go.data_classification, '')))
      WHEN 'public' THEN 'public'
      WHEN 'internal' THEN 'internal'
      WHEN 'confidential' THEN 'confidential'
      WHEN 'restricted' THEN 'restricted'
      ELSE 'unknown'
    END AS effective_data_classification
  FROM app_assets aa
  LEFT JOIN governance_subject_overrides go
    ON go.subject_kind = 'app_asset'
   AND go.subject_id = aa.id
  LEFT JOIN identities owner
    ON owner.id = go.owner_identity_id
  WHERE aa.expired_at IS NULL
    AND aa.last_observed_run_id IS NOT NULL
),
with_signals AS (
  SELECT
    pb.*,
    (
      1
      + CASE WHEN pb.owner_count > 0 OR pb.governance_owner_identity_id > 0 THEN 1 ELSE 0 END
      + CASE WHEN pb.discovery_source_count > 0 THEN 1 ELSE 0 END
    ) AS evidence_signal_count
  FROM posture_base pb
)
SELECT
  ws.*,
  CASE
    WHEN ws.evidence_last_seen_at IS NULL THEN 'unknown'
    WHEN ws.evidence_last_seen_at >= now() - interval '7 days' THEN 'fresh'
    WHEN ws.evidence_last_seen_at >= now() - interval '30 days' THEN 'aging'
    ELSE 'stale'
  END AS evidence_freshness,
  CASE ws.evidence_signal_count
    WHEN 3 THEN 'high'
    WHEN 2 THEN 'medium'
    ELSE 'low'
  END AS evidence_confidence,
  CASE ws.evidence_signal_count
    WHEN 3 THEN 'Inventory, ownership, and discovery evidence all line up.'
    WHEN 2 THEN 'Multiple evidence paths are available, but attribution is still partial.'
    ELSE 'This record currently relies on a single evidence path.'
  END AS evidence_confidence_reason
FROM with_signals ws;

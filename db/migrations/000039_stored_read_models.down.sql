DROP VIEW IF EXISTS connected_app_read_models_v;
DROP VIEW IF EXISTS discovery_app_read_models_v;

DROP INDEX IF EXISTS idx_saas_app_events_active_source_app_id_observed_at_desc;
DROP INDEX IF EXISTS idx_saas_app_sources_active_source_app_id;
DROP INDEX IF EXISTS idx_connector_source_state_discovery_scope;

ALTER TABLE app_assets
  DROP COLUMN IF EXISTS projection_refreshed_at,
  DROP COLUMN IF EXISTS evidence_last_seen_at,
  DROP COLUMN IF EXISTS discovery_event_count_30d,
  DROP COLUMN IF EXISTS discovery_source_count,
  DROP COLUMN IF EXISTS actor_count,
  DROP COLUMN IF EXISTS grant_count,
  DROP COLUMN IF EXISTS owner_count;

ALTER TABLE saas_apps
  DROP COLUMN IF EXISTS projection_refreshed_at,
  DROP COLUMN IF EXISTS has_confidential_scope,
  DROP COLUMN IF EXISTS has_privileged_scope,
  DROP COLUMN IF EXISTS actors_30d;

DROP TABLE IF EXISTS connector_source_state;

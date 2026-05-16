DROP INDEX IF EXISTS idx_event_targets_saas_app;
DROP INDEX IF EXISTS idx_event_targets_identity;
DROP INDEX IF EXISTS idx_event_targets_lookup;
DROP INDEX IF EXISTS idx_events_received_brin;
DROP INDEX IF EXISTS idx_events_envelope_gin;
DROP INDEX IF EXISTS idx_events_identity_occurred;
DROP INDEX IF EXISTS idx_events_target_id_occurred;
DROP INDEX IF EXISTS idx_events_actor_email_occurred;
DROP INDEX IF EXISTS idx_events_actor_id_occurred;
DROP INDEX IF EXISTS idx_events_event_type_occurred;
DROP INDEX IF EXISTS idx_events_source_occurred;
DROP INDEX IF EXISTS idx_events_occurred_at;

DROP TABLE IF EXISTS event_dedupe_keys;
DROP TABLE IF EXISTS event_targets;
DROP TABLE IF EXISTS events;

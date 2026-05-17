DROP INDEX IF EXISTS idx_connector_cursor_state_resync;
DROP INDEX IF EXISTS idx_connector_cursor_state_source;
DROP TABLE IF EXISTS connector_cursor_state;

DROP INDEX IF EXISTS event_inbox_dead_idx;
DROP INDEX IF EXISTS event_inbox_lease_idx;
DROP INDEX IF EXISTS event_inbox_ready_idx;
DROP INDEX IF EXISTS event_inbox_delivery_dedupe;
DROP TABLE IF EXISTS event_inbox;

DROP TYPE IF EXISTS event_inbox_status;

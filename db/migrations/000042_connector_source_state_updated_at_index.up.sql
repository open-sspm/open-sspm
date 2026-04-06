CREATE INDEX IF NOT EXISTS idx_connector_source_state_updated_at
  ON connector_source_state (updated_at);

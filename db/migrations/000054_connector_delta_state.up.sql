CREATE TABLE IF NOT EXISTS connector_delta_state (
  source_kind TEXT NOT NULL,
  source_name TEXT NOT NULL,
  resource TEXT NOT NULL,
  delta_link TEXT NOT NULL DEFAULT '',
  last_success_run_id BIGINT REFERENCES sync_runs(id) ON DELETE SET NULL,
  last_finished_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (source_kind, source_name, resource)
);

CREATE INDEX IF NOT EXISTS idx_connector_delta_state_source
  ON connector_delta_state (source_kind, source_name);

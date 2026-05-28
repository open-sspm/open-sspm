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

INSERT INTO connector_delta_state (
  source_kind,
  source_name,
  resource,
  delta_link,
  last_success_run_id,
  last_finished_at,
  updated_at
)
SELECT
  source_kind,
  source_name,
  resource,
  trim(cursor_json ->> 'delta_link'),
  last_run_id,
  last_success_at,
  updated_at
FROM connector_cursor_state
WHERE cursor_kind = 'graph_delta'
  AND trim(cursor_json ->> 'delta_link') <> ''
ON CONFLICT (source_kind, source_name, resource) DO UPDATE SET
  delta_link = EXCLUDED.delta_link,
  last_success_run_id = EXCLUDED.last_success_run_id,
  last_finished_at = EXCLUDED.last_finished_at,
  updated_at = now();

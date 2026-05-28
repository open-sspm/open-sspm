INSERT INTO connector_cursor_state (
  source_kind,
  source_name,
  resource,
  cursor_kind,
  cursor_json,
  last_success_at,
  last_run_id,
  updated_at
)
SELECT
  source_kind,
  source_name,
  resource,
  'graph_delta',
  jsonb_build_object('delta_link', delta_link),
  last_finished_at,
  last_success_run_id,
  updated_at
FROM connector_delta_state
WHERE trim(delta_link) <> ''
ON CONFLICT (source_kind, source_name, resource) DO UPDATE SET
  cursor_kind = EXCLUDED.cursor_kind,
  cursor_json = EXCLUDED.cursor_json,
  last_success_at = COALESCE(EXCLUDED.last_success_at, connector_cursor_state.last_success_at),
  last_run_id = COALESCE(EXCLUDED.last_run_id, connector_cursor_state.last_run_id),
  updated_at = now();

DROP TABLE IF EXISTS connector_delta_state;

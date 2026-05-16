CREATE TABLE IF NOT EXISTS event_projection_checkpoints (
  projection_name TEXT NOT NULL,
  source_kind TEXT NOT NULL,
  source_name TEXT NOT NULL,
  last_event_received_at TIMESTAMPTZ,
  last_event_id UUID,
  last_projected_at TIMESTAMPTZ,
  stats JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (projection_name, source_kind, source_name),
  CONSTRAINT event_projection_checkpoints_projection_nonempty CHECK (trim(projection_name) <> ''),
  CONSTRAINT event_projection_checkpoints_source_kind_nonempty CHECK (trim(source_kind) <> ''),
  CONSTRAINT event_projection_checkpoints_source_name_nonempty CHECK (trim(source_name) <> '')
);

CREATE TABLE IF NOT EXISTS event_projection_shadow_discovery_events (
  projection_name TEXT NOT NULL,
  source_kind TEXT NOT NULL,
  source_name TEXT NOT NULL,
  signal_kind TEXT NOT NULL,
  event_external_id TEXT NOT NULL,
  source_app_id TEXT NOT NULL DEFAULT '',
  source_app_name TEXT NOT NULL DEFAULT '',
  source_app_domain TEXT NOT NULL DEFAULT '',
  actor_external_id TEXT NOT NULL DEFAULT '',
  actor_email TEXT NOT NULL DEFAULT '',
  actor_display_name TEXT NOT NULL DEFAULT '',
  observed_at TIMESTAMPTZ NOT NULL,
  event_received_at TIMESTAMPTZ NOT NULL,
  event_id UUID NOT NULL,
  projected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (projection_name, source_kind, source_name, signal_kind, event_external_id),
  FOREIGN KEY (event_received_at, event_id)
    REFERENCES events (received_at, id)
    ON DELETE CASCADE,
  CONSTRAINT event_projection_shadow_discovery_projection_nonempty CHECK (trim(projection_name) <> ''),
  CONSTRAINT event_projection_shadow_discovery_source_kind_nonempty CHECK (trim(source_kind) <> ''),
  CONSTRAINT event_projection_shadow_discovery_source_name_nonempty CHECK (trim(source_name) <> ''),
  CONSTRAINT event_projection_shadow_discovery_signal_kind_nonempty CHECK (trim(signal_kind) <> ''),
  CONSTRAINT event_projection_shadow_discovery_event_external_id_nonempty CHECK (trim(event_external_id) <> '')
);

CREATE INDEX IF NOT EXISTS idx_event_projection_shadow_discovery_observed
  ON event_projection_shadow_discovery_events (projection_name, source_kind, source_name, observed_at DESC);

CREATE TABLE IF NOT EXISTS event_projection_diff_runs (
  id BIGSERIAL PRIMARY KEY,
  projection_name TEXT NOT NULL,
  source_kind TEXT NOT NULL,
  source_name TEXT NOT NULL,
  window_start TIMESTAMPTZ,
  window_end TIMESTAMPTZ,
  baseline_count BIGINT NOT NULL DEFAULT 0,
  projected_count BIGINT NOT NULL DEFAULT 0,
  matching_count BIGINT NOT NULL DEFAULT 0,
  missing_in_projection_count BIGINT NOT NULL DEFAULT 0,
  missing_in_baseline_count BIGINT NOT NULL DEFAULT 0,
  sample JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_event_projection_diff_runs_source_created
  ON event_projection_diff_runs (projection_name, source_kind, source_name, created_at DESC);

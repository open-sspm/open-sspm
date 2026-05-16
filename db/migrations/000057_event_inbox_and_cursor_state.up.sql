DO $$
BEGIN
  CREATE TYPE event_inbox_status AS ENUM (
    'queued',
    'processing',
    'ignored',
    'processed',
    'dead'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS event_inbox (
  id BIGSERIAL PRIMARY KEY,
  source_kind TEXT NOT NULL,
  source_id BIGINT,
  source_name TEXT NOT NULL,
  channel TEXT NOT NULL,

  external_event_id TEXT NOT NULL DEFAULT '',
  dedupe_key TEXT NOT NULL,
  dedupe_hash BYTEA NOT NULL,
  payload_hash BYTEA NOT NULL,

  status event_inbox_status NOT NULL DEFAULT 'queued',
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  processing_started_at TIMESTAMPTZ,
  processed_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  attempt_count INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 8,
  lease_owner TEXT,
  lease_until TIMESTAMPTZ,

  headers JSONB NOT NULL DEFAULT '{}'::jsonb,
  query JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw_body BYTEA NOT NULL,
  decoded_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
  ignore_reason TEXT NOT NULL DEFAULT '',
  last_error TEXT NOT NULL DEFAULT '',
  trace_id TEXT NOT NULL DEFAULT '',

  CONSTRAINT event_inbox_source_kind_nonempty CHECK (trim(source_kind) <> ''),
  CONSTRAINT event_inbox_source_name_nonempty CHECK (trim(source_name) <> ''),
  CONSTRAINT event_inbox_channel_nonempty CHECK (trim(channel) <> ''),
  CONSTRAINT event_inbox_dedupe_key_nonempty CHECK (trim(dedupe_key) <> ''),
  CONSTRAINT event_inbox_lease_owner_nonempty CHECK (lease_owner IS NULL OR trim(lease_owner) <> '')
);

CREATE UNIQUE INDEX IF NOT EXISTS event_inbox_delivery_dedupe
  ON event_inbox (source_kind, source_name, channel, dedupe_hash);

CREATE INDEX IF NOT EXISTS event_inbox_ready_idx
  ON event_inbox (available_at, id)
  WHERE status = 'queued';

CREATE INDEX IF NOT EXISTS event_inbox_lease_idx
  ON event_inbox (lease_until)
  WHERE status = 'processing';

CREATE INDEX IF NOT EXISTS event_inbox_dead_idx
  ON event_inbox (received_at DESC)
  WHERE status = 'dead';

CREATE TABLE IF NOT EXISTS connector_cursor_state (
  source_kind TEXT NOT NULL,
  source_id BIGINT,
  source_name TEXT NOT NULL,
  resource TEXT NOT NULL,
  cursor_kind TEXT NOT NULL,
  cursor_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  watermark TIMESTAMPTZ,
  cursor_expires_at TIMESTAMPTZ,
  last_success_at TIMESTAMPTZ,
  last_attempt_at TIMESTAMPTZ,
  last_error_at TIMESTAMPTZ,
  last_error TEXT NOT NULL DEFAULT '',
  last_run_id BIGINT REFERENCES sync_runs(id) ON DELETE SET NULL,
  last_provider_event_id TEXT NOT NULL DEFAULT '',
  needs_full_resync BOOLEAN NOT NULL DEFAULT false,
  version BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (source_kind, source_name, resource),
  CONSTRAINT connector_cursor_state_source_kind_nonempty CHECK (trim(source_kind) <> ''),
  CONSTRAINT connector_cursor_state_source_name_nonempty CHECK (trim(source_name) <> ''),
  CONSTRAINT connector_cursor_state_resource_nonempty CHECK (trim(resource) <> ''),
  CONSTRAINT connector_cursor_state_cursor_kind_nonempty CHECK (trim(cursor_kind) <> '')
);

CREATE INDEX IF NOT EXISTS idx_connector_cursor_state_source
  ON connector_cursor_state (source_kind, source_name);

CREATE INDEX IF NOT EXISTS idx_connector_cursor_state_resync
  ON connector_cursor_state (needs_full_resync, updated_at)
  WHERE needs_full_resync;

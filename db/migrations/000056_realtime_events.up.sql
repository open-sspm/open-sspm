CREATE TABLE IF NOT EXISTS events (
  id UUID NOT NULL,
  received_at TIMESTAMPTZ NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  observed_at TIMESTAMPTZ,
  normalized_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  ingest_id BIGINT,
  source_kind TEXT NOT NULL,
  source_id BIGINT,
  source_name TEXT NOT NULL,
  channel TEXT NOT NULL,

  provider_event_id TEXT NOT NULL DEFAULT '',
  dedupe_key TEXT NOT NULL,
  dedupe_hash BYTEA NOT NULL,

  event_type TEXT NOT NULL,
  category TEXT NOT NULL,
  action TEXT NOT NULL DEFAULT '',
  severity SMALLINT NOT NULL DEFAULT 0,

  actor_kind TEXT NOT NULL DEFAULT '',
  actor_id TEXT NOT NULL DEFAULT '',
  actor_email TEXT NOT NULL DEFAULT '',
  actor_display_name TEXT NOT NULL DEFAULT '',

  target_kind TEXT NOT NULL DEFAULT '',
  target_id TEXT NOT NULL DEFAULT '',
  target_name TEXT NOT NULL DEFAULT '',
  target_ref JSONB NOT NULL DEFAULT '{}'::jsonb,

  outcome TEXT NOT NULL DEFAULT '',
  ip INET,
  user_agent TEXT NOT NULL DEFAULT '',

  identity_id BIGINT,
  saas_app_id BIGINT,

  envelope JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw JSONB NOT NULL,
  trace_id TEXT NOT NULL DEFAULT '',

  PRIMARY KEY (received_at, id)
) PARTITION BY RANGE (received_at);

CREATE TABLE IF NOT EXISTS events_default PARTITION OF events DEFAULT;

CREATE TABLE IF NOT EXISTS event_targets (
  event_received_at TIMESTAMPTZ NOT NULL,
  event_id UUID NOT NULL,
  ordinal INT NOT NULL,
  role TEXT NOT NULL DEFAULT 'target',
  target_kind TEXT NOT NULL,
  target_id TEXT NOT NULL DEFAULT '',
  target_name TEXT NOT NULL DEFAULT '',
  target_email TEXT NOT NULL DEFAULT '',
  identity_id BIGINT,
  saas_app_id BIGINT,
  envelope JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (event_received_at, event_id, ordinal),
  FOREIGN KEY (event_received_at, event_id)
    REFERENCES events (received_at, id)
    ON DELETE CASCADE
) PARTITION BY RANGE (event_received_at);

CREATE TABLE IF NOT EXISTS event_targets_default PARTITION OF event_targets DEFAULT;

CREATE TABLE IF NOT EXISTS event_dedupe_keys (
  source_kind TEXT NOT NULL,
  source_id BIGINT,
  source_name TEXT NOT NULL,
  dedupe_hash BYTEA NOT NULL,
  dedupe_key TEXT NOT NULL,
  event_received_at TIMESTAMPTZ NOT NULL,
  event_id UUID NOT NULL,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  duplicate_count BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (source_kind, source_name, dedupe_hash)
);

CREATE INDEX IF NOT EXISTS idx_events_occurred_at
  ON events (occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_events_source_occurred
  ON events (source_kind, source_name, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_events_event_type_occurred
  ON events (event_type, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_events_actor_id_occurred
  ON events (actor_id, occurred_at DESC)
  WHERE actor_id <> '';

CREATE INDEX IF NOT EXISTS idx_events_actor_email_occurred
  ON events (actor_email, occurred_at DESC)
  WHERE actor_email <> '';

CREATE INDEX IF NOT EXISTS idx_events_target_id_occurred
  ON events (target_id, occurred_at DESC)
  WHERE target_id <> '';

CREATE INDEX IF NOT EXISTS idx_events_identity_occurred
  ON events (identity_id, occurred_at DESC)
  WHERE identity_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_events_envelope_gin
  ON events USING GIN (envelope jsonb_path_ops);

CREATE INDEX IF NOT EXISTS idx_events_received_brin
  ON events USING BRIN (received_at);

CREATE INDEX IF NOT EXISTS idx_event_targets_lookup
  ON event_targets (target_kind, target_id, event_received_at DESC);

CREATE INDEX IF NOT EXISTS idx_event_targets_identity
  ON event_targets (identity_id, event_received_at DESC)
  WHERE identity_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_event_targets_saas_app
  ON event_targets (saas_app_id, event_received_at DESC)
  WHERE saas_app_id IS NOT NULL;


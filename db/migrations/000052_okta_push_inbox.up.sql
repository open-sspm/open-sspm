CREATE TABLE IF NOT EXISTS okta_push_inbox (
  id BIGSERIAL PRIMARY KEY,
  source_name TEXT NOT NULL,
  channel TEXT NOT NULL,
  delivery_external_id TEXT NOT NULL DEFAULT '',
  event_external_id TEXT NOT NULL,
  event_type TEXT NOT NULL DEFAULT '',
  event_index INT NOT NULL DEFAULT 0,
  published_at TIMESTAMPTZ,
  status TEXT NOT NULL DEFAULT 'queued',
  raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  attempts INT NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ,
  processed_run_id BIGINT REFERENCES sync_runs(id) ON DELETE SET NULL,
  processed_at TIMESTAMPTZ,
  last_received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  error_message TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT okta_push_inbox_channel_check
    CHECK (channel IN ('event_hook', 'eventbridge')),
  CONSTRAINT okta_push_inbox_status_check
    CHECK (status IN ('queued', 'processing', 'processed', 'ignored', 'dead_letter')),
  CONSTRAINT okta_push_inbox_event_external_id_check
    CHECK (trim(event_external_id) <> '')
);

CREATE INDEX IF NOT EXISTS idx_okta_push_inbox_queue
  ON okta_push_inbox (status, next_attempt_at, id);

CREATE INDEX IF NOT EXISTS idx_okta_push_inbox_source_published
  ON okta_push_inbox (source_name, published_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_okta_push_inbox_event_dedupe
  ON okta_push_inbox (source_name, event_external_id);

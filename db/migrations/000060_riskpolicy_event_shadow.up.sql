CREATE TABLE IF NOT EXISTS riskpolicy_event_queue (
  id BIGSERIAL PRIMARY KEY,
  event_received_at TIMESTAMPTZ NOT NULL,
  event_id UUID NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued',
  attempts INT NOT NULL DEFAULT 0,
  available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  claimed_by TEXT,
  claimed_at TIMESTAMPTZ,
  lease_until TIMESTAMPTZ,
  last_error TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (event_received_at, event_id),
  FOREIGN KEY (event_received_at, event_id)
    REFERENCES events (received_at, id)
    ON DELETE CASCADE,
  CONSTRAINT riskpolicy_event_queue_status_check
    CHECK (status IN ('queued', 'processing', 'processed', 'dead'))
);

CREATE INDEX IF NOT EXISTS idx_riskpolicy_event_queue_claim
  ON riskpolicy_event_queue (available_at ASC, id ASC)
  WHERE status = 'queued';

CREATE INDEX IF NOT EXISTS idx_riskpolicy_event_queue_lease
  ON riskpolicy_event_queue (lease_until)
  WHERE status = 'processing';

CREATE TABLE IF NOT EXISTS riskpolicy_event_shadow_signals (
  event_received_at TIMESTAMPTZ NOT NULL,
  event_id UUID NOT NULL,
  signal_id TEXT NOT NULL,
  policy_pack_id TEXT NOT NULL,
  policy_pack_version TEXT NOT NULL,
  severity TEXT NOT NULL,
  title TEXT NOT NULL,
  evidence TEXT NOT NULL DEFAULT '',
  output JSONB NOT NULL DEFAULT '{}'::jsonb,
  evaluated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (event_received_at, event_id, signal_id),
  FOREIGN KEY (event_received_at, event_id)
    REFERENCES events (received_at, id)
    ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_riskpolicy_event_shadow_signals_event
  ON riskpolicy_event_shadow_signals (event_received_at, event_id);

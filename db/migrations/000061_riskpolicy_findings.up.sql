CREATE TABLE IF NOT EXISTS riskpolicy_findings (
  finding_key TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  shadow BOOLEAN NOT NULL DEFAULT true,
  status TEXT NOT NULL DEFAULT 'open',
  source_kind TEXT NOT NULL DEFAULT '',
  source_name TEXT NOT NULL DEFAULT '',
  entity_kind TEXT NOT NULL DEFAULT '',
  entity_id TEXT NOT NULL DEFAULT '',
  entity_name TEXT NOT NULL DEFAULT '',
  event_received_at TIMESTAMPTZ,
  event_id UUID,
  signal_id TEXT NOT NULL,
  policy_pack_id TEXT NOT NULL,
  policy_pack_version TEXT NOT NULL,
  severity TEXT NOT NULL,
  title TEXT NOT NULL,
  evidence TEXT NOT NULL DEFAULT '',
  output JSONB NOT NULL DEFAULT '{}'::jsonb,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT riskpolicy_findings_status_check CHECK (status IN ('open', 'resolved', 'suppressed')),
  CONSTRAINT riskpolicy_findings_source_nonempty CHECK (trim(source) <> ''),
  CONSTRAINT riskpolicy_findings_signal_nonempty CHECK (trim(signal_id) <> ''),
  FOREIGN KEY (event_received_at, event_id)
    REFERENCES events (received_at, id)
    ON DELETE SET NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_riskpolicy_findings_event_signal
  ON riskpolicy_findings (event_received_at, event_id, signal_id)
  WHERE event_received_at IS NOT NULL AND event_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_riskpolicy_findings_shadow_status
  ON riskpolicy_findings (shadow, status, severity);

CREATE INDEX IF NOT EXISTS idx_riskpolicy_findings_source
  ON riskpolicy_findings (source_kind, source_name);

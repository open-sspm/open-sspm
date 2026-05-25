CREATE TABLE IF NOT EXISTS findings (
  finding_key TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  base_severity TEXT NOT NULL DEFAULT '',
  effective_severity TEXT NOT NULL DEFAULT '',
  severity_source TEXT NOT NULL DEFAULT 'policy',
  title TEXT NOT NULL DEFAULT '',
  summary TEXT NOT NULL DEFAULT '',
  evidence TEXT NOT NULL DEFAULT '',
  remediation TEXT NOT NULL DEFAULT '',
  source_kind TEXT NOT NULL DEFAULT '',
  source_name TEXT NOT NULL DEFAULT '',
  scope_kind TEXT NOT NULL DEFAULT '',
  scope_source_kind TEXT NOT NULL DEFAULT '',
  scope_source_name TEXT NOT NULL DEFAULT '',
  entity_kind TEXT NOT NULL DEFAULT '',
  entity_id TEXT NOT NULL DEFAULT '',
  entity_name TEXT NOT NULL DEFAULT '',
  resource_kind TEXT NOT NULL DEFAULT '',
  resource_id TEXT NOT NULL DEFAULT '',
  resource_name TEXT NOT NULL DEFAULT '',
  policy_bundle_id TEXT NOT NULL DEFAULT '',
  policy_bundle_version TEXT NOT NULL DEFAULT '',
  policy_id TEXT NOT NULL DEFAULT '',
  policy_title TEXT NOT NULL DEFAULT '',
  rule_id TEXT,
  ruleset_id TEXT,
  event_received_at TIMESTAMPTZ,
  event_id UUID,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved_at TIMESTAMPTZ,
  suppressed_until TIMESTAMPTZ,
  suppression_reason TEXT NOT NULL DEFAULT '',
  suppressed_by TEXT NOT NULL DEFAULT '',
  suppressed_at TIMESTAMPTZ,
  output JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT findings_status_check CHECK (status IN ('open', 'resolved', 'suppressed', 'accepted_risk')),
  CONSTRAINT findings_severity_source_check CHECK (severity_source IN ('policy', 'override', 'manual')),
  CONSTRAINT findings_source_kind_nonempty CHECK (trim(source_kind) <> ''),
  CONSTRAINT findings_source_name_nonempty CHECK (trim(source_name) <> ''),
  CONSTRAINT findings_policy_id_nonempty CHECK (trim(policy_id) <> ''),
  FOREIGN KEY (event_received_at, event_id)
    REFERENCES events (received_at, id)
    ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_findings_status_severity
  ON findings (status, effective_severity);

CREATE INDEX IF NOT EXISTS idx_findings_source
  ON findings (source_kind, source_name);

CREATE INDEX IF NOT EXISTS idx_findings_ruleset_scope
  ON findings (ruleset_id, rule_id, scope_kind, scope_source_kind, scope_source_name);

CREATE INDEX IF NOT EXISTS idx_findings_event_ref
  ON findings (event_received_at, event_id)
  WHERE event_received_at IS NOT NULL AND event_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS finding_events (
  id BIGSERIAL PRIMARY KEY,
  finding_key TEXT NOT NULL REFERENCES findings(finding_key) ON DELETE RESTRICT,
  event_key TEXT NOT NULL,
  event_type TEXT NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  actor_kind TEXT NOT NULL DEFAULT '',
  actor_id TEXT NOT NULL DEFAULT '',
  message TEXT NOT NULL DEFAULT '',
  old_status TEXT NOT NULL DEFAULT '',
  new_status TEXT NOT NULL DEFAULT '',
  old_severity TEXT NOT NULL DEFAULT '',
  new_severity TEXT NOT NULL DEFAULT '',
  source_event_received_at TIMESTAMPTZ,
  source_event_id UUID,
  evaluation_run_id BIGINT REFERENCES sync_runs(id) ON DELETE SET NULL,
  sync_run_id BIGINT REFERENCES sync_runs(id) ON DELETE SET NULL,
  policy_control_id BIGINT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT finding_events_type_nonempty CHECK (trim(event_type) <> ''),
  CONSTRAINT finding_events_key_nonempty CHECK (trim(event_key) <> ''),
  UNIQUE (finding_key, event_key),
  FOREIGN KEY (source_event_received_at, source_event_id)
    REFERENCES events (received_at, id)
    ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_finding_events_finding_occurred
  ON finding_events (finding_key, occurred_at DESC);

INSERT INTO findings (
  finding_key,
  status,
  base_severity,
  effective_severity,
  severity_source,
  title,
  summary,
  evidence,
  source_kind,
  source_name,
  scope_kind,
  scope_source_kind,
  scope_source_name,
  entity_kind,
  entity_id,
  entity_name,
  policy_bundle_id,
  policy_bundle_version,
  policy_id,
  policy_title,
  event_received_at,
  event_id,
  first_seen_at,
  last_seen_at,
  resolved_at,
  output,
  created_at,
  updated_at
)
SELECT
  rf.finding_key,
  rf.status,
  rf.severity,
  rf.severity,
  'policy',
  rf.title,
  rf.evidence,
  rf.evidence,
  rf.source_kind,
  rf.source_name,
  'event',
  rf.source_kind,
  rf.source_name,
  rf.entity_kind,
  rf.entity_id,
  rf.entity_name,
  rf.policy_pack_id,
  rf.policy_pack_version,
  rf.signal_id,
  rf.title,
  rf.event_received_at,
  rf.event_id,
  rf.first_seen_at,
  rf.last_seen_at,
  rf.resolved_at,
  jsonb_build_object(
    'legacy_source', rf.source,
    'legacy_signal_id', rf.signal_id,
    'legacy_shadow', rf.shadow,
    'riskpolicy_output', rf.output
  ),
  rf.first_seen_at,
  rf.updated_at
FROM riskpolicy_findings rf
ON CONFLICT (finding_key) DO NOTHING;

INSERT INTO finding_events (
  finding_key,
  event_key,
  event_type,
  occurred_at,
  new_status,
  new_severity,
  source_event_received_at,
  source_event_id,
  payload
)
SELECT
  f.finding_key,
  'backfill:' || f.finding_key,
  CASE WHEN f.status = 'resolved' THEN 'resolved' ELSE 'opened' END,
  f.first_seen_at,
  f.status,
  f.effective_severity,
  f.event_received_at,
  f.event_id,
  jsonb_build_object('source', 'riskpolicy_findings_backfill')
FROM findings f
WHERE f.output ->> 'legacy_source' = 'riskpolicy_event_shadow'
ON CONFLICT (finding_key, event_key) DO NOTHING;

-- PHASE-TWO-DELETE: riskpolicy_findings remains as a compatibility/parity source until canonical findings own riskpolicy projection.

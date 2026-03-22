-- Durable manual sync dispatch queue.

CREATE TABLE IF NOT EXISTS sync_jobs (
  id UUID PRIMARY KEY,
  lane TEXT NOT NULL,
  connector_kind TEXT,
  source_name TEXT,
  trigger_kind TEXT NOT NULL,
  status TEXT NOT NULL,
  attempt_count INT NOT NULL DEFAULT 0,
  claimed_by TEXT,
  claimed_at TIMESTAMPTZ,
  heartbeat_at TIMESTAMPTZ,
  lease_expires_at TIMESTAMPTZ,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT sync_jobs_lane_valid CHECK (lane IN ('full', 'discovery')),
  CONSTRAINT sync_jobs_trigger_kind_valid CHECK (trigger_kind IN ('manual')),
  CONSTRAINT sync_jobs_status_valid CHECK (status IN ('pending', 'claimed', 'running', 'succeeded', 'failed')),
  CONSTRAINT sync_jobs_claimed_by_nonempty CHECK (claimed_by IS NULL OR claimed_by <> ''),
  CONSTRAINT sync_jobs_connector_kind_nonempty CHECK (connector_kind IS NULL OR connector_kind <> ''),
  CONSTRAINT sync_jobs_source_name_nonempty CHECK (source_name IS NULL OR source_name <> '')
);

CREATE UNIQUE INDEX IF NOT EXISTS sync_jobs_active_manual_scope_idx
  ON sync_jobs (lane, COALESCE(connector_kind, ''), COALESCE(source_name, ''), trigger_kind)
  WHERE status IN ('pending', 'claimed', 'running');

CREATE INDEX IF NOT EXISTS sync_jobs_claim_idx
  ON sync_jobs (lane, trigger_kind, status, created_at, id);

CREATE INDEX IF NOT EXISTS sync_jobs_stale_idx
  ON sync_jobs (lane, trigger_kind, lease_expires_at)
  WHERE status IN ('claimed', 'running');

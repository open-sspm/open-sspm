-- Phase 2 sync job queue: scheduled jobs, delayed availability, and rerun promotion.

ALTER TABLE sync_jobs
  DROP CONSTRAINT IF EXISTS sync_jobs_trigger_kind_valid;

ALTER TABLE sync_jobs
  ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS rerun_requested BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE sync_jobs
  ADD CONSTRAINT sync_jobs_trigger_kind_valid CHECK (trigger_kind IN ('manual', 'scheduled'));

DROP INDEX IF EXISTS sync_jobs_active_manual_scope_idx;

CREATE UNIQUE INDEX IF NOT EXISTS sync_jobs_active_scope_idx
  ON sync_jobs (lane, COALESCE(connector_kind, ''), COALESCE(source_name, ''))
  WHERE status IN ('pending', 'claimed', 'running');

DROP INDEX IF EXISTS sync_jobs_claim_idx;

CREATE INDEX IF NOT EXISTS sync_jobs_claim_idx
  ON sync_jobs (lane, status, available_at, created_at, id);

DROP INDEX IF EXISTS sync_jobs_stale_idx;

CREATE INDEX IF NOT EXISTS sync_jobs_stale_idx
  ON sync_jobs (lane, status, lease_expires_at)
  WHERE status IN ('claimed', 'running');

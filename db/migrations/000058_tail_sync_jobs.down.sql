DROP INDEX IF EXISTS sync_jobs_claim_idx;

CREATE INDEX IF NOT EXISTS sync_jobs_claim_idx
  ON sync_jobs (lane, status, available_at, created_at, id);

DROP INDEX IF EXISTS sync_jobs_active_scope_idx;

CREATE UNIQUE INDEX IF NOT EXISTS sync_jobs_active_scope_idx
  ON sync_jobs (lane, COALESCE(connector_kind, ''), COALESCE(source_name, ''))
  WHERE status IN ('pending', 'claimed', 'running');

ALTER TABLE sync_jobs
  DROP CONSTRAINT IF EXISTS sync_jobs_resource_nonempty,
  DROP CONSTRAINT IF EXISTS sync_jobs_lane_valid;

ALTER TABLE sync_jobs
  ADD CONSTRAINT sync_jobs_lane_valid CHECK (lane IN ('full', 'discovery'));

ALTER TABLE sync_jobs
  DROP COLUMN IF EXISTS created_reason,
  DROP COLUMN IF EXISTS priority,
  DROP COLUMN IF EXISTS payload,
  DROP COLUMN IF EXISTS resource;

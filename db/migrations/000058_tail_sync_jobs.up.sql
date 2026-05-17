ALTER TABLE sync_jobs
  DROP CONSTRAINT IF EXISTS sync_jobs_lane_valid;

ALTER TABLE sync_jobs
  ADD COLUMN IF NOT EXISTS resource TEXT,
  ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS priority INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS created_reason TEXT NOT NULL DEFAULT '';

ALTER TABLE sync_jobs
  ADD CONSTRAINT sync_jobs_lane_valid CHECK (lane IN ('full', 'discovery', 'tail')),
  ADD CONSTRAINT sync_jobs_resource_nonempty CHECK (resource IS NULL OR trim(resource) <> '');

DROP INDEX IF EXISTS sync_jobs_active_scope_idx;

CREATE UNIQUE INDEX IF NOT EXISTS sync_jobs_active_scope_idx
  ON sync_jobs (lane, COALESCE(connector_kind, ''), COALESCE(source_name, ''), COALESCE(resource, ''))
  WHERE status IN ('pending', 'claimed', 'running');

DROP INDEX IF EXISTS sync_jobs_claim_idx;

CREATE INDEX IF NOT EXISTS sync_jobs_claim_idx
  ON sync_jobs (lane, status, priority DESC, available_at, created_at, id);

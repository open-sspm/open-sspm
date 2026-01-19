-- Sync lock leases for cross-worker mutual exclusion (no pinned pool connections).

CREATE TABLE IF NOT EXISTS sync_locks (
  scope_kind TEXT NOT NULL,
  scope_name TEXT NOT NULL,
  holder_instance_id TEXT NOT NULL,
  holder_token UUID NOT NULL,
  acquired_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  lease_expires_at TIMESTAMPTZ NOT NULL,
  heartbeat_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (scope_kind, scope_name),
  CONSTRAINT sync_locks_scope_kind_nonempty CHECK (scope_kind <> ''),
  CONSTRAINT sync_locks_scope_name_nonempty CHECK (scope_name <> ''),
  CONSTRAINT sync_locks_holder_instance_id_nonempty CHECK (holder_instance_id <> '')
);

CREATE INDEX IF NOT EXISTS sync_locks_lease_expires_at_idx ON sync_locks (lease_expires_at);
CREATE INDEX IF NOT EXISTS sync_locks_holder_instance_id_idx ON sync_locks (holder_instance_id);

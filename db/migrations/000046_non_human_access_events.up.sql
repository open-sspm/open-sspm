CREATE TABLE IF NOT EXISTS non_human_access_events (
  id BIGSERIAL PRIMARY KEY,
  auth_user_id BIGINT REFERENCES auth_users(id) ON DELETE SET NULL,
  auth_user_role TEXT NOT NULL DEFAULT '',
  event_kind TEXT NOT NULL,
  principal_ref TEXT NOT NULL DEFAULT '',
  target_kind TEXT NOT NULL DEFAULT '',
  target_ref TEXT NOT NULL DEFAULT '',
  filter_signature TEXT NOT NULL DEFAULT '',
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_non_human_access_events_occurred_at
  ON non_human_access_events (occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_non_human_access_events_auth_user_id
  ON non_human_access_events (auth_user_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_non_human_access_events_event_kind
  ON non_human_access_events (event_kind, occurred_at DESC);

CREATE TABLE IF NOT EXISTS connected_app_governance (
  app_asset_id BIGINT PRIMARY KEY REFERENCES app_assets(id) ON DELETE CASCADE,
  review_state TEXT NOT NULL DEFAULT 'unreviewed',
  owner_identity_id BIGINT REFERENCES identities(id) ON DELETE SET NULL,
  ticket_ref TEXT NOT NULL DEFAULT '',
  notes TEXT NOT NULL DEFAULT '',
  updated_by_auth_user_id BIGINT REFERENCES auth_users(id) ON DELETE SET NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT connected_app_governance_review_state_check CHECK (
    review_state IN ('unreviewed', 'under_review', 'sanctioned', 'needs_revocation', 'ticketed')
  )
);

CREATE INDEX IF NOT EXISTS idx_connected_app_governance_owner_identity_id
  ON connected_app_governance (owner_identity_id);

CREATE INDEX IF NOT EXISTS idx_connected_app_governance_review_state
  ON connected_app_governance (review_state);

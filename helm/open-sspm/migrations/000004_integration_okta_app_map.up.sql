-- Map supported integrations (GitHub/Datadog) to their Okta app external IDs.

CREATE TABLE IF NOT EXISTS integration_okta_app_map (
  integration_kind TEXT PRIMARY KEY CHECK (integration_kind IN ('github', 'datadog')),
  okta_app_external_id TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

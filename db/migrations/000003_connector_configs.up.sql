-- Connector configuration stored in Postgres

CREATE TABLE IF NOT EXISTS connector_configs (
  kind TEXT PRIMARY KEY,
  enabled BOOLEAN NOT NULL DEFAULT false,
  config JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO connector_configs (kind, enabled, config)
VALUES
  ('okta', false, '{}'::jsonb),
  ('github', false, '{}'::jsonb),
  ('datadog', false, '{}'::jsonb),
  ('aws_identity_center', false, '{}'::jsonb),
  ('vault', false, '{}'::jsonb)
ON CONFLICT (kind) DO NOTHING;

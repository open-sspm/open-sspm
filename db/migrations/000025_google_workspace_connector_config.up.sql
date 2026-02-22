INSERT INTO connector_configs (kind, enabled, config)
VALUES ('google_workspace', false, '{}'::jsonb)
ON CONFLICT (kind) DO NOTHING;

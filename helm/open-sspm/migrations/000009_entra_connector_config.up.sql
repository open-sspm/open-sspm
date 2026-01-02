-- Add Microsoft Entra ID connector config row

INSERT INTO connector_configs (kind, enabled, config)
VALUES ('entra', false, '{}'::jsonb)
ON CONFLICT (kind) DO NOTHING;


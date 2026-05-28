UPDATE connector_configs
SET
  config = CASE
    WHEN config ? 'discovery_ingest_mode' THEN config - 'event_inbox_mode'
    ELSE (config - 'event_inbox_mode') || jsonb_build_object('discovery_ingest_mode', config->'event_inbox_mode')
  END,
  updated_at = now()
WHERE kind = 'okta'
  AND config ? 'event_inbox_mode';

UPDATE connector_configs
SET
  config = CASE
    WHEN config ? 'event_inbox_mode' THEN config - 'discovery_ingest_mode'
    ELSE (config - 'discovery_ingest_mode') || jsonb_build_object('event_inbox_mode', config->'discovery_ingest_mode')
  END,
  updated_at = now()
WHERE kind = 'okta'
  AND config ? 'discovery_ingest_mode';

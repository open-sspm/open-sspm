ALTER TABLE integration_okta_app_map
  ADD COLUMN IF NOT EXISTS okta_source_kind TEXT NOT NULL DEFAULT 'okta',
  ADD COLUMN IF NOT EXISTS okta_source_name TEXT NOT NULL DEFAULT '';

WITH okta_source AS (
  SELECT lower(trim(regexp_replace(regexp_replace(config ->> 'domain', '^https?://', '', 'i'), '/.*$', ''))) AS source_name
  FROM connector_configs
  WHERE kind = 'okta'
    AND trim(regexp_replace(regexp_replace(config ->> 'domain', '^https?://', '', 'i'), '/.*$', '')) <> ''
  ORDER BY updated_at DESC, created_at DESC
  LIMIT 1
)
UPDATE integration_okta_app_map
SET okta_source_kind = 'okta',
    okta_source_name = (SELECT source_name FROM okta_source)
WHERE okta_source_name = ''
  AND EXISTS (SELECT 1 FROM okta_source);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'okta_groups_source_external_id_key'
  ) THEN
    ALTER TABLE okta_groups
      ADD CONSTRAINT okta_groups_source_external_id_key
      UNIQUE (source_kind, source_name, external_id);
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'okta_apps_source_external_id_key'
  ) THEN
    ALTER TABLE okta_apps
      ADD CONSTRAINT okta_apps_source_external_id_key
      UNIQUE (source_kind, source_name, external_id);
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'integration_okta_app_map_source_external_id_key'
  ) THEN
    ALTER TABLE integration_okta_app_map
      ADD CONSTRAINT integration_okta_app_map_source_external_id_key
      UNIQUE (okta_source_kind, okta_source_name, okta_app_external_id);
  END IF;
END
$$;

ALTER TABLE okta_groups
  DROP CONSTRAINT IF EXISTS okta_groups_external_id_key;

ALTER TABLE okta_apps
  DROP CONSTRAINT IF EXISTS okta_apps_external_id_key;

ALTER TABLE integration_okta_app_map
  DROP CONSTRAINT IF EXISTS integration_okta_app_map_okta_app_external_id_key;

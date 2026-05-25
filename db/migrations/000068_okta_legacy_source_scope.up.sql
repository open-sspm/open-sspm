ALTER TABLE okta_groups
  ADD COLUMN IF NOT EXISTS source_kind TEXT NOT NULL DEFAULT 'okta',
  ADD COLUMN IF NOT EXISTS source_name TEXT NOT NULL DEFAULT '';

ALTER TABLE okta_apps
  ADD COLUMN IF NOT EXISTS source_kind TEXT NOT NULL DEFAULT 'okta',
  ADD COLUMN IF NOT EXISTS source_name TEXT NOT NULL DEFAULT '';

ALTER TABLE okta_app_group_assignments
  ADD COLUMN IF NOT EXISTS source_kind TEXT NOT NULL DEFAULT 'okta',
  ADD COLUMN IF NOT EXISTS source_name TEXT NOT NULL DEFAULT '';

-- Keep legacy external_id uniqueness until every Okta app/group lookup and mapping is source-scoped.
-- Backfill existing rows to the configured Okta source so source-filtered writes continue to find them.
WITH okta_source AS (
  SELECT lower(trim(regexp_replace(regexp_replace(config ->> 'domain', '^https?://', '', 'i'), '/.*$', ''))) AS source_name
  FROM connector_configs
  WHERE kind = 'okta'
    AND trim(regexp_replace(regexp_replace(config ->> 'domain', '^https?://', '', 'i'), '/.*$', '')) <> ''
  ORDER BY updated_at DESC, created_at DESC
  LIMIT 1
)
UPDATE okta_groups
SET source_kind = 'okta',
    source_name = (SELECT source_name FROM okta_source)
WHERE source_name = ''
  AND EXISTS (SELECT 1 FROM okta_source);

WITH okta_source AS (
  SELECT lower(trim(regexp_replace(regexp_replace(config ->> 'domain', '^https?://', '', 'i'), '/.*$', ''))) AS source_name
  FROM connector_configs
  WHERE kind = 'okta'
    AND trim(regexp_replace(regexp_replace(config ->> 'domain', '^https?://', '', 'i'), '/.*$', '')) <> ''
  ORDER BY updated_at DESC, created_at DESC
  LIMIT 1
)
UPDATE okta_apps
SET source_kind = 'okta',
    source_name = (SELECT source_name FROM okta_source)
WHERE source_name = ''
  AND EXISTS (SELECT 1 FROM okta_source);

WITH okta_source AS (
  SELECT lower(trim(regexp_replace(regexp_replace(config ->> 'domain', '^https?://', '', 'i'), '/.*$', ''))) AS source_name
  FROM connector_configs
  WHERE kind = 'okta'
    AND trim(regexp_replace(regexp_replace(config ->> 'domain', '^https?://', '', 'i'), '/.*$', '')) <> ''
  ORDER BY updated_at DESC, created_at DESC
  LIMIT 1
)
UPDATE okta_app_group_assignments
SET source_kind = 'okta',
    source_name = (SELECT source_name FROM okta_source)
WHERE source_name = ''
  AND EXISTS (SELECT 1 FROM okta_source);

CREATE INDEX IF NOT EXISTS idx_okta_groups_source
  ON okta_groups (source_kind, source_name);

CREATE INDEX IF NOT EXISTS idx_okta_apps_source
  ON okta_apps (source_kind, source_name);

CREATE INDEX IF NOT EXISTS idx_okta_app_group_assignments_source
  ON okta_app_group_assignments (source_kind, source_name);

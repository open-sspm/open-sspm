ALTER TABLE okta_groups
  ADD COLUMN IF NOT EXISTS source_kind TEXT NOT NULL DEFAULT 'okta',
  ADD COLUMN IF NOT EXISTS source_name TEXT NOT NULL DEFAULT '';

ALTER TABLE okta_apps
  ADD COLUMN IF NOT EXISTS source_kind TEXT NOT NULL DEFAULT 'okta',
  ADD COLUMN IF NOT EXISTS source_name TEXT NOT NULL DEFAULT '';

ALTER TABLE okta_app_group_assignments
  ADD COLUMN IF NOT EXISTS source_kind TEXT NOT NULL DEFAULT 'okta',
  ADD COLUMN IF NOT EXISTS source_name TEXT NOT NULL DEFAULT '';

ALTER TABLE okta_groups
  DROP CONSTRAINT IF EXISTS okta_groups_external_id_key;

ALTER TABLE okta_apps
  DROP CONSTRAINT IF EXISTS okta_apps_external_id_key;

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

CREATE INDEX IF NOT EXISTS idx_okta_groups_source
  ON okta_groups (source_kind, source_name);

CREATE INDEX IF NOT EXISTS idx_okta_apps_source
  ON okta_apps (source_kind, source_name);

CREATE INDEX IF NOT EXISTS idx_okta_app_group_assignments_source
  ON okta_app_group_assignments (source_kind, source_name);

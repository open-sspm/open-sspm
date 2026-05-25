DROP INDEX IF EXISTS idx_okta_app_group_assignments_source;
DROP INDEX IF EXISTS idx_okta_apps_source;
DROP INDEX IF EXISTS idx_okta_groups_source;

ALTER TABLE okta_apps
  DROP CONSTRAINT IF EXISTS okta_apps_source_external_id_key;

ALTER TABLE okta_groups
  DROP CONSTRAINT IF EXISTS okta_groups_source_external_id_key;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'okta_apps_external_id_key'
  ) THEN
    ALTER TABLE okta_apps
      ADD CONSTRAINT okta_apps_external_id_key
      UNIQUE (external_id);
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'okta_groups_external_id_key'
  ) THEN
    ALTER TABLE okta_groups
      ADD CONSTRAINT okta_groups_external_id_key
      UNIQUE (external_id);
  END IF;
END
$$;

ALTER TABLE okta_app_group_assignments
  DROP COLUMN IF EXISTS source_name,
  DROP COLUMN IF EXISTS source_kind;

ALTER TABLE okta_apps
  DROP COLUMN IF EXISTS source_name,
  DROP COLUMN IF EXISTS source_kind;

ALTER TABLE okta_groups
  DROP COLUMN IF EXISTS source_name,
  DROP COLUMN IF EXISTS source_kind;

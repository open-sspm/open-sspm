ALTER TABLE accounts
  ADD COLUMN IF NOT EXISTS entity_category TEXT NOT NULL DEFAULT 'unknown';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'accounts_entity_category_check'
      AND conrelid = 'accounts'::regclass
  ) THEN
    ALTER TABLE accounts
      ADD CONSTRAINT accounts_entity_category_check
      CHECK (
        entity_category IN (
          'user',
          'group',
          'service_principal',
          'service_account',
          'team',
          'role',
          'auth_role',
          'entity',
          'unknown'
        )
      );
  END IF;
END $$;

UPDATE accounts
SET entity_category = CASE lower(trim(COALESCE(raw_json ->> 'entity_category', '')))
  WHEN 'user' THEN 'user'
  WHEN 'group' THEN 'group'
  WHEN 'service_principal' THEN 'service_principal'
  WHEN 'service_account' THEN 'service_account'
  WHEN 'team' THEN 'team'
  WHEN 'role' THEN 'role'
  WHEN 'auth_role' THEN 'auth_role'
  WHEN 'entity' THEN 'entity'
  ELSE 'unknown'
END
WHERE entity_category = 'unknown';

CREATE INDEX IF NOT EXISTS idx_accounts_active_source_kind_name_entity_category
  ON accounts (source_kind, source_name, entity_category)
  WHERE expired_at IS NULL
    AND last_observed_run_id IS NOT NULL;

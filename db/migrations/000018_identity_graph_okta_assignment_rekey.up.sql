-- Rekey Okta assignment tables to point to accounts while keeping legacy columns for compatibility.

ALTER TABLE okta_user_groups
  ADD COLUMN IF NOT EXISTS okta_user_account_id BIGINT;

ALTER TABLE okta_user_app_assignments
  ADD COLUMN IF NOT EXISTS okta_user_account_id BIGINT;

WITH mapped AS (
  SELECT
    ug.id AS okta_user_group_row_id,
    a.id AS account_id
  FROM okta_user_groups ug
  JOIN idp_users iu ON iu.id = ug.idp_user_id
  JOIN LATERAL (
    SELECT acc.id
    FROM accounts acc
    WHERE acc.source_kind = 'okta'
      AND acc.external_id = iu.external_id
    ORDER BY acc.id
    LIMIT 1
  ) a ON TRUE
)
UPDATE okta_user_groups ug
SET okta_user_account_id = mapped.account_id
FROM mapped
WHERE ug.id = mapped.okta_user_group_row_id;

WITH mapped AS (
  SELECT
    ouaa.id AS okta_user_app_assignment_row_id,
    a.id AS account_id
  FROM okta_user_app_assignments ouaa
  JOIN idp_users iu ON iu.id = ouaa.idp_user_id
  JOIN LATERAL (
    SELECT acc.id
    FROM accounts acc
    WHERE acc.source_kind = 'okta'
      AND acc.external_id = iu.external_id
    ORDER BY acc.id
    LIMIT 1
  ) a ON TRUE
)
UPDATE okta_user_app_assignments ouaa
SET okta_user_account_id = mapped.account_id
FROM mapped
WHERE ouaa.id = mapped.okta_user_app_assignment_row_id;

ALTER TABLE okta_user_groups
  ALTER COLUMN okta_user_account_id SET NOT NULL;

ALTER TABLE okta_user_app_assignments
  ALTER COLUMN okta_user_account_id SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'okta_user_groups_okta_user_account_id_fkey'
  ) THEN
    ALTER TABLE okta_user_groups
      ADD CONSTRAINT okta_user_groups_okta_user_account_id_fkey
      FOREIGN KEY (okta_user_account_id)
      REFERENCES accounts(id)
      ON DELETE CASCADE;
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'okta_user_app_assignments_okta_user_account_id_fkey'
  ) THEN
    ALTER TABLE okta_user_app_assignments
      ADD CONSTRAINT okta_user_app_assignments_okta_user_account_id_fkey
      FOREIGN KEY (okta_user_account_id)
      REFERENCES accounts(id)
      ON DELETE CASCADE;
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'okta_user_groups_okta_user_account_group_key'
  ) THEN
    ALTER TABLE okta_user_groups
      ADD CONSTRAINT okta_user_groups_okta_user_account_group_key
      UNIQUE (okta_user_account_id, okta_group_id);
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'okta_user_app_assignments_okta_user_account_app_key'
  ) THEN
    ALTER TABLE okta_user_app_assignments
      ADD CONSTRAINT okta_user_app_assignments_okta_user_account_app_key
      UNIQUE (okta_user_account_id, okta_app_id);
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_okta_user_groups_okta_user_account
  ON okta_user_groups (okta_user_account_id);

CREATE INDEX IF NOT EXISTS idx_okta_user_app_assignments_okta_user_account
  ON okta_user_app_assignments (okta_user_account_id);

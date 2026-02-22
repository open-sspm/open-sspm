ALTER TABLE accounts
  ADD COLUMN IF NOT EXISTS account_kind TEXT NOT NULL DEFAULT 'unknown';

UPDATE accounts
SET account_kind = CASE
  WHEN lower(trim(account_kind)) IN ('human', 'service', 'bot', 'unknown') THEN lower(trim(account_kind))
  ELSE 'unknown'
END;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'accounts_account_kind_check'
      AND conrelid = 'accounts'::regclass
  ) THEN
    ALTER TABLE accounts
      ADD CONSTRAINT accounts_account_kind_check
      CHECK (account_kind IN ('human', 'service', 'bot', 'unknown'));
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_accounts_active_account_kind
  ON accounts (account_kind)
  WHERE expired_at IS NULL
    AND last_observed_run_id IS NOT NULL;

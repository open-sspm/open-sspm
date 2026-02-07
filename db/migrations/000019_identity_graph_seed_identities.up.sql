-- Seed identities and account links from existing Okta-centric data.

DO $$
DECLARE
  account_row RECORD;
  identity_id BIGINT;
BEGIN
  FOR account_row IN
    SELECT a.id, lower(trim(a.email)) AS email, a.display_name
    FROM accounts a
    WHERE a.source_kind = 'okta'
      AND NOT EXISTS (
        SELECT 1
        FROM identity_accounts ia
        WHERE ia.account_id = a.id
      )
    ORDER BY a.id
  LOOP
    INSERT INTO identities (kind, display_name, primary_email)
    VALUES ('unknown', account_row.display_name, account_row.email)
    RETURNING id INTO identity_id;

    INSERT INTO identity_accounts (identity_id, account_id, link_reason, confidence)
    VALUES (identity_id, account_row.id, 'seed_migration', 1.0)
    ON CONFLICT (account_id) DO NOTHING;
  END LOOP;
END
$$;

INSERT INTO identity_accounts (identity_id, account_id, link_reason, confidence, updated_at)
SELECT
  okta_identity.identity_id,
  il.app_user_id,
  il.link_reason,
  1.0,
  now()
FROM identity_links il
JOIN idp_users iu ON iu.id = il.idp_user_id
JOIN LATERAL (
  SELECT acc.id
  FROM accounts acc
  WHERE acc.source_kind = 'okta'
    AND acc.external_id = iu.external_id
  ORDER BY acc.id
  LIMIT 1
) okta_account ON TRUE
JOIN LATERAL (
  SELECT ia.identity_id
  FROM identity_accounts ia
  WHERE ia.account_id = okta_account.id
  ORDER BY ia.id
  LIMIT 1
) okta_identity ON TRUE
ON CONFLICT (account_id) DO UPDATE SET
  identity_id = EXCLUDED.identity_id,
  link_reason = EXCLUDED.link_reason,
  confidence = EXCLUDED.confidence,
  updated_at = EXCLUDED.updated_at;

DO $$
DECLARE
  account_row RECORD;
  identity_id BIGINT;
BEGIN
  FOR account_row IN
    SELECT a.id, lower(trim(a.email)) AS email, a.display_name
    FROM accounts a
    WHERE NOT EXISTS (
      SELECT 1
      FROM identity_accounts ia
      WHERE ia.account_id = a.id
    )
    ORDER BY a.id
  LOOP
    INSERT INTO identities (kind, display_name, primary_email)
    VALUES ('unknown', account_row.display_name, account_row.email)
    RETURNING id INTO identity_id;

    INSERT INTO identity_accounts (identity_id, account_id, link_reason, confidence)
    VALUES (identity_id, account_row.id, 'seed_orphan', 1.0)
    ON CONFLICT (account_id) DO NOTHING;
  END LOOP;
END
$$;

INSERT INTO identity_source_settings (source_kind, source_name, is_authoritative)
SELECT DISTINCT
  a.source_kind,
  a.source_name,
  CASE WHEN a.source_kind = 'okta' THEN TRUE ELSE FALSE END AS is_authoritative
FROM accounts a
WHERE a.source_kind IN ('okta', 'entra')
ON CONFLICT (source_kind, source_name) DO UPDATE SET
  is_authoritative = EXCLUDED.is_authoritative,
  updated_at = now();

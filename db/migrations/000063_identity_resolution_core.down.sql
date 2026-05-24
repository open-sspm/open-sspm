DROP TABLE IF EXISTS identity_link_evidence;
DROP TABLE IF EXISTS identity_match_candidates;
DROP TABLE IF EXISTS identity_anchors;
DROP TABLE IF EXISTS account_anchors;

ALTER TABLE identity_accounts
  DROP CONSTRAINT IF EXISTS identity_accounts_link_state_check,
  DROP COLUMN IF EXISTS reviewed_at,
  DROP COLUMN IF EXISTS reviewed_by,
  DROP COLUMN IF EXISTS linked_at,
  DROP COLUMN IF EXISTS resolver_version,
  DROP COLUMN IF EXISTS link_state;

ALTER TABLE identities
  DROP COLUMN IF EXISTS primary_email_id;

DROP TABLE IF EXISTS identity_emails;

ALTER TABLE identities
  DROP CONSTRAINT IF EXISTS identities_identity_kind_check,
  DROP CONSTRAINT IF EXISTS identities_resolution_state_check,
  DROP COLUMN IF EXISTS identity_kind,
  DROP COLUMN IF EXISTS resolution_state;

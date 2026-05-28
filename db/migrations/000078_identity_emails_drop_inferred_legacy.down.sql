ALTER TABLE identity_emails
  DROP CONSTRAINT IF EXISTS identity_emails_verification_state_check;

ALTER TABLE identity_emails
  ADD CONSTRAINT identity_emails_verification_state_check
  CHECK (verification_state IN ('verified_authoritative', 'verified_source', 'manual', 'observed', 'inferred_legacy', 'unverified'));

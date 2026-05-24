COMMENT ON TABLE accounts IS
  'Concrete source account or principal observed from a connector. Entitlements attach to accounts and roll up to identities through identity_accounts.';
COMMENT ON COLUMN accounts.source_kind IS
  'Connector kind that produced this source account, such as okta, entra, github, datadog, google_workspace, or aws.';
COMMENT ON COLUMN accounts.source_name IS
  'Connector instance identifier scoped with source_kind.';
COMMENT ON COLUMN accounts.external_id IS
  'Provider-native stable account identifier within source_kind and source_name.';
COMMENT ON COLUMN accounts.account_kind IS
  'High-level identity classification for this account: human, service, bot, or unknown.';
COMMENT ON COLUMN accounts.entity_category IS
  'Provider entity category used to separate users from groups, roles, teams, service principals, and other principal-like objects.';

COMMENT ON TABLE identities IS
  'Normalized identity rollup. Human identities may be managed by an authoritative source anchor or provisional until anchored; non-human identities represent service and bot principals.';
COMMENT ON COLUMN identities.kind IS
  'Normalized identity classification: human, service, bot, or unknown.';
COMMENT ON COLUMN identities.primary_email IS
  'Preferred normalized email for display and deterministic matching. Alias history belongs in a separate future table.';

COMMENT ON TABLE identity_accounts IS
  'Exclusive mapping from source accounts to normalized identities. One identity can have many accounts, but each account belongs to exactly one identity.';
COMMENT ON COLUMN identity_accounts.identity_id IS
  'Normalized identity that owns this source account membership.';
COMMENT ON COLUMN identity_accounts.account_id IS
  'Source account membership. This column is intentionally unique and must not be relaxed to model shared-account ownership.';
COMMENT ON COLUMN identity_accounts.link_reason IS
  'Reason this account was linked to the identity, such as manual, auto_email, seed_migration, auto_provisional_identity, or auto_provisional_ambiguous_email (link to an existing identity whose email match was not unambiguous).';
COMMENT ON COLUMN identity_accounts.confidence IS
  'Confidence for the accepted account-to-identity link. Detailed evidence belongs in a separate future evidence table.';

COMMENT ON TABLE identity_source_settings IS
  'Per-source identity settings. Authoritative sources provide identity anchors for managed human posture and attribute preference.';
COMMENT ON COLUMN identity_source_settings.is_authoritative IS
  'When true, active accounts from this source make linked human identities anchored/managed and are preferred for identity attributes.';

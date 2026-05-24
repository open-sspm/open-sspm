# Identity Graph

Open-SSPM normalizes source accounts into an identity graph without losing the
source-account boundary. This model is intentionally small and stable so access
questions can be answered consistently across connectors.

## Core Tables

- `accounts` are concrete accounts or principals observed from a connector.
  They keep the provider identity (`source_kind`, `source_name`, `external_id`),
  lifecycle metadata, account classification, and raw provider payload. Access
  facts such as entitlements attach to `accounts`.
- `identities` are normalized rollups. Human identities can be managed when they
  have an authoritative source anchor, unmanaged/provisional when they only come
  from app accounts, or non-human when classified as `service` or `bot`.
- `identity_accounts` links source accounts into identities. One identity can
  have many source accounts, but a source account belongs to exactly one
  identity through the `UNIQUE(account_id)` invariant.
- `identity_source_settings` marks configured sources as authoritative anchors.
  Anchors decide whether a human identity is managed and which source attributes
  win during identity refresh.

## Invariants

- Do not relax `identity_accounts.account_id` uniqueness. It is the boundary
  between "this account is part of this normalized identity" and every other
  relationship we may add later.
- Do not model shared deploy accounts, service-account custodians, or account
  usage attribution by linking one account to multiple identities. Add a
  separate ownership or attribution table for those relationships.
- Keep entitlements attached to `accounts`. Identity-level access views should
  roll up through `identity_accounts` so provider-specific account evidence is
  still available.
- Treat `auto_provisional_identity` links as provisional account rollups until
  they gain an authoritative anchor. Provisional identities are useful for
  surfacing unmanaged access, not proof that the account is a managed human.
- Treat `auto_provisional_ambiguous_email` links as provisional with extra
  caveat: the resolver linked the account to an existing identity that shares
  its email, but two or more identities tie at the top tier (no authoritative
  winner, or multiple authoritative anchors disagree). The link is
  deterministic (lowest-id authoritative-anchored identity, then lowest-id
  overall) but should not be trusted as a match — surface it for manual review
  rather than treat it as proof of ownership. UI surfaces should refuse to
  follow these links as if they were verified.

## Link Reasons

- `manual` — explicit operator decision. Never overwritten by the resolver.
- `auto_email` — exactly one identity owns the account's email at the top tier
  (authoritative-anchored beats non-authoritative). Highest confidence the
  automated resolver produces.
- `auto_provisional_identity` — no identity owns this email yet; the resolver
  minted a new (provisional) identity for the account.
- `auto_provisional_ambiguous_email` — two or more identities tie at the top
  tier for this email. The account is linked to a deterministic existing
  identity rather than minting a duplicate, and the link is flagged so the UI
  can prompt for manual resolution.
- `seed_migration` / `seed_orphan` — historical seed during a backfill or
  migration. Treated like `auto_*` for re-resolution purposes.

## Future Extension Points

Alias tables, match-evidence tables, candidate review queues, and shared-account
ownership tables should build on this model rather than changing the exclusive
`identity_accounts` mapping. The matcher layer can propose or update links, but
the graph invariant remains one source account to one normalized identity.

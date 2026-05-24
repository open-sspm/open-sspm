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
- `identity_emails` stores active, historical, observed, and verified emails for
  an identity. `identities.primary_email` remains a display/preferred cache and
  compatibility field; resolver and owner-write email lookups read
  `identity_emails` first with a legacy primary-email fallback.
- `account_anchors` stores deterministic provider anchor observations extracted
  from source accounts.
- `identity_anchors` stores accepted canonical anchors for identities. An active
  deterministic anchor can belong to only one identity.
- `identity_match_candidates` stores unresolved account-to-identity proposals
  that require review.
- `identity_link_evidence` stores structured positive or negative evidence for
  accepted links and pending candidates.
- `identity_merge_events` and `identity_merge_redirects` preserve audit and
  lookup history when a provisional or duplicate identity is merged.
- `account_identity_relationships` models ownership, custody, approval, and
  attribution relationships for accounts without changing account membership.

## Invariants

- Do not relax `identity_accounts.account_id` uniqueness. It is the boundary
  between "this account is part of this normalized identity" and every other
  relationship we may add later.
- Do not model shared deploy accounts, service-account custodians, or account
  usage attribution by linking one account to multiple identities. Use
  `account_identity_relationships` for those relationships.
- Keep entitlements attached to `accounts`. Identity-level access views should
  roll up through `identity_accounts` so provider-specific account evidence is
  still available.
- Do not treat email or name as a globally unique identity key. Duplicate
  active emails are allowed and should become candidates or review items unless
  stronger deterministic evidence resolves the match.
- Deterministic anchors are separated into observed `account_anchors` and
  accepted `identity_anchors` so provider evidence and canonical identity facts
  remain auditable.
- Treat `auto_provisional_identity` links as provisional account rollups until
  they gain an authoritative anchor. Provisional identities are useful for
  surfacing unmanaged access, not proof that the account is a managed human.
- Treat `auto_provisional_ambiguous_email` links as provisional with extra
  caveat: two or more existing identities share the account email at the top
  tier (no authoritative winner, or multiple authoritative anchors disagree).
  The resolver must not attach the account to a deterministic lowest-id
  candidate in this case. It creates a safe provisional rollup that should be
  surfaced for manual review rather than treated as proof of ownership.

## Link Reasons

- `manual` — explicit operator decision. Never overwritten by the resolver.
- `auto_anchor` — one active deterministic `account_anchors` observation
  matches accepted `identity_anchors` for exactly one identity. This outranks
  email evidence. The resolver may upgrade an existing provisional link to
  `auto_anchor` when the deterministic anchor appears later.
- `auto_email` — exactly one identity owns the account's email at the top tier
  (authoritative-anchored beats non-authoritative).
- `auto_provisional_identity` — no identity owns this email yet; the resolver
  minted a new (provisional) identity for the account.
- `auto_provisional_ambiguous_email` — two or more identities tie at the top
  tier for this email. The account is linked to a provisional identity, not to
  one of the candidate identities, so the UI can prompt for manual resolution.
- `auto_provisional_conflicting_anchor` — account anchors point at two or more
  identities. The account is linked to a provisional identity and candidate
  rows carry the conflicting anchor evidence for review.
- `seed_migration` / `seed_orphan` — historical seed during a backfill or
  migration. Treated like `auto_*` for re-resolution purposes.

## Review And Merge

Candidates are reviewable in the admin UI at `/identity-resolution`, with
status tabs and grouping filters for ambiguous email, anchor conflict,
service/shared warnings, and confidence bands. Candidates can also be listed
through `GET /api/identity-resolution/candidates`; `status` and `group` query
parameters use the same filtering model as the UI. Structured candidate
details, evidence, and active account-to-identity relationships are exposed
through `GET /api/identity-resolution/candidates/:id`. Admin review actions can
accept, reject, classify, or merge a candidate through:

- `POST /api/identity-resolution/candidates/:id/accept`
- `POST /api/identity-resolution/candidates/:id/reject`
- `POST /api/identity-resolution/candidates/:id/mark-service`
- `POST /api/identity-resolution/candidates/:id/mark-shared`
- `GET /api/identities/:id/emails`
- `POST /api/identities/:id/emails`
- `GET /api/identities/:id/anchors`
- `POST /api/identities/:id/anchors`
- `POST /identity-resolution/candidates/:id/accept`
- `POST /identity-resolution/candidates/:id/reject`
- `POST /identity-resolution/candidates/:id/mark-service`
- `POST /identity-resolution/candidates/:id/mark-shared`

Accepting a candidate transactionally links the account to the selected
identity with a manual-confirmed link, records the account email as a manual
login alias, records manual evidence, accepts the candidate, and supersedes
competing pending candidates for the same account.
When the candidate has a provisional rollup, reviewers can accept and merge the
provisional identity into the selected identity. The merge moves remaining
account links, non-conflicting emails, and non-conflicting anchors, records an
`identity_merge_events` audit row plus an `identity_merge_redirects` lookup
redirect, and marks the provisional identity as `merged`.
Rejecting a candidate suppresses the same resolver fingerprint; the candidate
can reappear only after the resolver emits materially different evidence.
Reviewers can also mark the source account as service or shared. That updates
the current rollup classification, writes negative candidate evidence, and
rejects pending human-match candidates without making `identity_accounts`
many-to-many. When the reviewer keeps the candidate as a custodian or owner,
that relationship is stored in `account_identity_relationships`, not in
`identity_accounts`.

Identity detail pages show the accepted graph facts for a person: active and
historical emails, verification state, deterministic anchors, trust level, and
source provenance.

Non-human identity detail pages show active `account_identity_relationships`
for linked service/bot source accounts. Admins can add an owner, custodian,
approver, attributed user, or last-observed user through
`POST /non-human-identities/:ref/relationships`; the target email is resolved
with the same strict unambiguous owner lookup used by governance write paths.
The write fans out across the source accounts linked to the non-human identity
and never changes `identity_accounts` membership.

Identity merges should be recorded with `identity_merge_events` and
`identity_merge_redirects`; access facts still stay on accounts and account
membership still moves through `identity_accounts`.

## Future Extension Points

The remaining maturity work is richer anchor extraction per connector,
relationship audit/history beyond the active-row model, and broader dashboards
that separate unresolved human identity work from service-account custody work.
The matcher layer can propose or update links, but the graph invariant remains
one source account to one normalized identity.

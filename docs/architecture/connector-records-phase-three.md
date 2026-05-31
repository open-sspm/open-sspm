# Phase Three Connector Records Audit

Phase three moves active connectors toward the same records-first, capability-declared, canonical-event shape proven by Okta.

## Migrated In This Phase

- Okta remained the reference implementation for full inventory records, generic ingest, and canonical event dispatch.
- Entra full and discovery runs now emit `records.StateUpsert` values into the generic SQL state projector. Graph delta bootstrap, cursor reset, explicit delete handling, and finalization still live in the Entra delta finalizer so incremental semantics are unchanged.
- Google Workspace full and discovery runs now emit `records.StateUpsert` values into the generic SQL state projector. Reports tail still uses its watermark/overlap cursor, but writes canonical events through the shared record dispatcher.
- AWS CloudTrail tail and Datadog Audit Logs tail now write canonical event records through the shared record dispatcher.

## Intentionally Deferred

- AWS and Datadog full inventory paths remain on the shared registry bulk row writers for this phase. They have no connector-specific cursor or delete protocol, and the row writers already share source scoping, run marking, and finalization behavior. Migrating them is mostly mechanical and should be batched with a reusable bulk record emission helper.
- GitHub full inventory remains on direct bulk writers because it has the broadest app asset, owner, credential, and programmatic access surface. Migrating it safely should preserve the existing GitHub App installation, fine-grained PAT request, PAT, and entitlement semantics with dedicated record tests.
- Vault full inventory remains on direct bulk writers because policy, entity, group, auth role, and mount synthesis needs a first-class policy record mapping before it can be represented cleanly as generic state records.

## Invariants

- Source-scoped writes remain keyed by `source_kind` and `source_name`.
- Entra delta cursor updates and explicit delete expiry still happen only during successful finalization.
- Discovery ingestion remains incremental; stale discovery expiration still happens through the existing finalizers rather than "not seen in this run" alone.
- Tail cursors for Google Workspace, AWS, and Datadog still use watermark overlap and only advance after canonical event dispatch succeeds.

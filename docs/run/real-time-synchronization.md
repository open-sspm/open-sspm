# Real-Time Synchronization

Open-SSPM uses provider push where available, cursor-based tailing where possible, and periodic full reconciliation everywhere. Push and tail improve freshness, but full sync remains the correctness repair loop.

## Data Model

- `event_inbox` stores raw push deliveries for retry, dead-letter triage, and delivery-level dedupe.
- `events` stores canonical provider evidence. It is partitioned by `received_at`.
- `event_targets` stores every normalized target for multi-target provider events. The singular `events.target_*` fields are only primary-target denormalization for common queries.
- `event_dedupe_keys` is the global idempotency ledger. It prevents duplicate canonical events across partition boundaries.
- `connector_cursor_state` stores tail cursors, watermarks, error state, and full-resync flags per source/resource.

## Worker Lanes

| Command | Role |
|---------|------|
| `open-sspm worker` | Full reconciliation and drift repair |
| `open-sspm worker-ingest` | Okta push inbox processing and current compatibility writes |
| `open-sspm worker-tail` | Cursor-based incremental tail jobs |
| `open-sspm worker-riskpolicy` | Shadow event policy evaluation and shadow finding projection |

Worker processes also maintain daily `events` and `event_targets` partitions. They create partitions ahead of time and drop expired daily partitions according to `EVENT_RETENTION_DAYS`.

## Provider Coverage

| Provider | Push | Tail | Full |
|----------|------|------|------|
| Okta | Event Hook and EventBridge ingest with current compatibility | System Log watermark-overlap tail | Users, groups, apps, assignments |
| Google Workspace | Not enabled yet | Reports activities watermark-overlap tail | Users, groups, grants, app inventory |
| Datadog | Preview forwarding is not enabled | Audit Logs API tail | Users and role assignments |
| AWS Identity Center | Customer relay is not enabled | CloudTrail LookupEvents tail when CloudTrail is configured | Identity Store and SSO Admin inventory |
| Microsoft Entra ID | Not enabled yet | Delta remains coupled to the full sync path | Graph reconciliation |
| GitHub | Not enabled yet | Audit API not enabled yet | REST/GraphQL reconciliation |
| Vault | No native direct push | Not enabled without a customer audit collector | Vault API reconciliation |

Capability metadata reflects executable behavior. Future push or tail surfaces should not be advertised until verification, signature validation, dedupe, cursor handling, and canonical writes exist.

## Shadow Policy Migration

Canonical event projectors and event riskpolicy evaluation are shadow-only. They are used for parity measurement and future migration work; they do not replace the user-facing current findings/rules UI until suppression, attestation, override, evidence, and historical reporting semantics are explicitly accepted.

Run a parity projection:

```bash
just event-projection -- --source-kind okta --source-name your-org.okta.com
```

Omit `--since` to resume from the source checkpoint; pass `--since` for an explicit replay window.

Run the shadow event policy worker:

```bash
just worker-riskpolicy
```

## Operational Checks

- `opensspm_worker_lane_up{lane="tail"}` should be `1` when the tail worker is running.
- `opensspm_worker_lane_up{lane="riskpolicy-event"}` should be `1` when the riskpolicy event worker is running.
- `opensspm_event_partition_maintenance_runs_total{status="success"}` should increase on worker startup and then every maintenance interval.
- `opensspm_event_partition_ensured_until_timestamp_seconds` should stay ahead of the current date.
- Tail cursor failures are visible in `connector_cursor_state.last_error` and sync run failures for the corresponding `*_tail` source kind.

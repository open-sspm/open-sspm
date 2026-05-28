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
| `open-sspm worker --lane event-inbox` | Event inbox processing |
| `open-sspm worker --lane tail` | Cursor-based incremental tail jobs |
| `open-sspm worker --lane evaluator` | Event policy evaluation and canonical finding projection |

Worker processes also maintain daily `events` and `event_targets` partitions. They create partitions ahead of time and drop expired daily partitions according to `EVENT_RETENTION_DAYS`.

## Provider Coverage

| Provider | Push | Tail | Full |
|----------|------|------|------|
| Okta | Event Hook and EventBridge ingest through the generic event inbox | System Log watermark-overlap tail | Users, groups, apps, assignments |
| Google Workspace | Not enabled yet | Reports activities watermark-overlap tail | Users, groups, grants, app inventory |
| Datadog | Preview forwarding is not enabled | Audit Logs API tail | Users and role assignments |
| AWS Identity Center | Customer relay is not enabled | CloudTrail LookupEvents tail when CloudTrail is configured | Identity Store and SSO Admin inventory |
| Microsoft Entra ID | Not enabled yet | Delta remains coupled to the full sync path | Graph reconciliation |
| GitHub | Not enabled yet | Audit API not enabled yet | REST/GraphQL reconciliation |
| Vault | No native direct push | Not enabled without a customer audit collector | Vault API reconciliation |

Capability metadata reflects executable behavior. Future push or tail surfaces should not be advertised until verification, signature validation, dedupe, cursor handling, and canonical writes exist.

## Event Policy Evaluation

Canonical event evaluation writes canonical findings. Suppression, attestation, override, evidence, and historical reporting metadata stay on that findings path so each view can render user-facing state without querying raw event policy output.

Run the event evaluator worker:

```bash
just worker evaluator
```

## Operational Checks

- `opensspm_worker_lane_up{lane="tail"}` should be `1` when the tail worker is running.
- `opensspm_worker_lane_up{lane="evaluator"}` should be `1` when the event evaluator worker is running.
- `opensspm_event_partition_maintenance_runs_total{status="success"}` should increase on worker startup and then every maintenance interval.
- `opensspm_event_partition_ensured_until_timestamp_seconds` should stay ahead of the current date.
- Tail cursor failures are visible in `connector_cursor_state.last_error` and sync run failures where `sync_runs.run_mode = 'tail'`.

<p align="center">Made with love in Paris 🇫🇷</p>

# Open-SSPM

Open-SSPM is a small “who has access to what” service. It syncs identities from Okta and Microsoft Entra ID (IdP sources), permissions from connected apps (Google Workspace, GitHub, Datadog, AWS Identity Center), links accounts (auto by email + manual links), and renders a server-side UI.

## Demo
- URL: `https://demo.opensspm.com`
- Login: `admin@admin.com` / `admin`

## Features
- API/web server (`open-sspm api`) + background worker lanes for full sync (`open-sspm worker`), discovery (`open-sspm worker --lane discovery`), event inbox processing (`open-sspm worker --lane event-inbox`), incremental tail sync (`open-sspm worker --lane tail`), and event evaluation (`open-sspm worker --lane evaluator`) + one-off sync lanes (`open-sspm admin sync`, `open-sspm admin sync --lane discovery`) + in-app “Resync” (queued async by default).
- Realtime synchronization foundation: canonical provider events, multi-target event storage, generic event inboxing, cursor-locked tail jobs, and event evaluation.
- Okta: users, groups, apps, and assignments (IdP source).
- Microsoft Entra ID: users plus application/service principal governance metadata.
- Google Workspace: users, groups, admin roles, OAuth app/grant inventory, and token audit activity.
- SaaS Discovery: discovered app inventory + hotspots from IdP SSO and OAuth grant evidence (Okta System Log polling or push + Entra sign-ins/grants), with governance and binding workflows.
- GitHub: org members/teams/repo permissions (optional SCIM lookup for emails).
- Datadog: users + role assignments.
- AWS Identity Center: users + account/permission set assignments.
- Programmatic access governance: browse app assets and credentials with risk labels, expiry filters, and actor attribution links.
- Matching: automatic by email (case-insensitive) + manual linking for accounts without email.
- Findings: Okta CIS benchmark findings (rules must be seeded; see below).
- Server-rendered UI: Echo + templ; Tailwind v4 with custom component library; minimal vanilla JS for UX.

## Requirements
- Go 1.26.x (go.mod uses toolchain `go1.26.0`)
- Docker + Docker Compose
- Node.js + npm (for building CSS)
- Optional: `air` (live reload), `templ` (template generation), `sqlc` (regen DB code)

## Quickstart
1. Copy `.env.example` to `.env` and update as needed.
2. Start Postgres: `just dev-up`
3. Run migrations: `just migrate`
4. Install JS deps + build CSS: `npm install && just ui`
5. Run the API/web server: `just run`
6. Run background workers: `just worker` (full lane), `just worker discovery` (discovery lane), `just worker event-inbox` (event inbox), `just worker tail` (incremental tail), and `just worker evaluator` (event evaluation).
7. Generate a stable connector secret key and export it before configuring connectors:
   - `export CONNECTOR_SECRET_KEY="$(openssl rand -base64 32)"`
8. Open `http://localhost:8080`, configure connectors under Settings → Connectors, then run a sync (Settings → Resync queues workers by default, or use `just sync` for one-off inline execution).
8. Optional: enable SaaS discovery on Okta/Entra connector settings, run sync, then review `http://localhost:8080/discovery/apps` and `http://localhost:8080/discovery/hotspots`.

## Findings / rules (Okta benchmark)
Rulesets are loaded from generated Open SSPM spec Go entities (`github.com/open-sspm/open-sspm-spec/gen/go/opensspm/spec/v2`) and must be seeded into Postgres before they show up in the UI:
- `go run ./cmd/open-sspm admin seed-rules`

After seeding, run an Okta sync and open `http://localhost:8080/findings/rulesets/cis.okta.idaas_stig.v2`.

## Dev workflows
- Live-reload server: `just dev` (requires `air` + `templ`)
- Run background full sync worker: `just worker`
- Run background discovery sync worker: `just worker discovery`
- Run background event inbox worker: `just worker event-inbox`
- Run background incremental tail worker: `just worker tail`
- Run background event evaluator worker: `just worker evaluator`
- Watch CSS: `just ui-watch`
- Sync vendored runtime JS: `npm run vendor:sync` (also runs automatically after `npm install` / `npm ci`)
- Check vendored runtime JS drift: `npm run vendor:check`
- Run frontend JS unit tests: `just ui-test` (or `npm run test:js`)
- Regenerate templ templates: `just templ` (watch: `just templ-watch`)
- Regenerate SQLC code: `just sqlc` (generated code is checked in under `internal/db/gen`)

## Configuration
- Process-level env vars: `.env.example` (database, HTTP address, sync interval/workers).
- Structured logging for service/ops commands:
  - `LOG_FORMAT=json|text` (default: `json`)
  - `LOG_LEVEL=debug|info|warn|error` (default: `info`)
  - Invalid logging values fail fast at startup.
- Discovery lane: `SYNC_DISCOVERY_ENABLED=1` (default) enables the separate SaaS discovery lane; set `0` to disable discovery workers and discovery resyncs system-wide.
- Tail lane: `SYNC_TAIL_INTERVAL=5m` controls the incremental audit/delta tail cadence used by `open-sspm worker --lane tail`.
- Event partitions: worker processes keep `events` and `event_targets` daily partitions created ahead of time and drop partitions older than `EVENT_RETENTION_DAYS` (default `90`).
- Event evaluator worker: `EVENT_EVALUATOR_WORKER_POLL_INTERVAL`, `EVENT_EVALUATOR_WORKER_BATCH_SIZE`, and `EVENT_EVALUATOR_WORKER_MAX_ATTEMPTS` tune event evaluation throughput and poison-event dead-lettering.
- Manual resync mode: `RESYNC_MODE=signal` (default, creates a durable sync job for background workers) or `RESYNC_MODE=inline` (request runs sync directly).
- Connector credentials: configured in-app under Settings → Connectors. Public connector metadata stays in Postgres, and secret values are stored separately in encrypted form using `CONNECTOR_SECRET_KEY` / `CONNECTOR_SECRET_KEY_FILE`.
- AWS Identity Center uses the AWS SDK default credentials chain (env/shared config/role), not DB-stored keys.
- SaaS discovery is per-connector (`discovery_enabled`) for Okta, Entra, and Google Workspace.
  - Okta discovery uses System Log access for polling backfill; optional Event Hook and EventBridge receivers run in `open-sspm api` and queue event inbox work independently of polling discovery.
  - Entra discovery uses sign-in and OAuth grant APIs (`AuditLog.Read.All`, `Directory.Read.All`, `DelegatedPermissionGrant.Read.All`).
  - Google Workspace discovery uses Reports API login/token activity and token inventory.

### Google Workspace connector setup
- Source identity: `customer_id` is the canonical `source_name` (`source_kind=google_workspace`); `primary_domain` is display metadata only.
- Required Google Admin delegated scopes:
  - `https://www.googleapis.com/auth/admin.directory.user.readonly`
  - `https://www.googleapis.com/auth/admin.directory.group.readonly`
  - `https://www.googleapis.com/auth/admin.directory.group.member.readonly`
  - `https://www.googleapis.com/auth/admin.directory.rolemanagement.readonly`
  - `https://www.googleapis.com/auth/admin.directory.user.security`
  - `https://www.googleapis.com/auth/admin.reports.audit.readonly`
- Domain-Wide Delegation prerequisites:
  - Create or choose a service account and enable Domain-Wide Delegation for it.
  - In Admin Console, authorize the service account client ID with the scopes above.
  - Set `delegated_admin_email` to a super admin (or delegated admin with equivalent read privileges).
- Auth mode options:
  - `service_account_json`: provide full JSON key in connector settings.
  - `adc`: run Open-SSPM with ADC/workload identity that can call IAM Credentials `signJwt` on `service_account_email`.

## Metrics
- Metrics are served on a dedicated listener (`METRICS_ADDR`) and are best-effort.
- Metrics collection failures after successful syncs are tracked in `opensspm_sync_metrics_collection_failures_total`.
- Worker liveness and loop metrics are exposed as `opensspm_worker_lane_*` for full, discovery, event-inbox, tail, and evaluator workers.
- Event partition maintenance exposes `opensspm_event_partition_maintenance_runs_total`, `opensspm_event_partition_ensured_until_timestamp_seconds`, and `opensspm_event_partitions_dropped_total`.
- Discovery metrics include:
  - `opensspm_discovery_events_ingested_total`
  - `opensspm_discovery_ingest_failures_total`
  - `opensspm_discovery_apps_total`
  - `opensspm_discovery_hotspots_total`
- Event inbox metrics include:
  - `opensspm_event_inbox_deliveries_received_total{source_kind,source_name,channel,status}`
  - `opensspm_event_inbox_deliveries_processed_total{source_kind,source_name,channel,status}`
  - `opensspm_event_inbox_processing_duration_seconds{source_kind,source_name,channel}`
  - `opensspm_event_inbox_queue_depth{source_kind,source_name,channel}`
  - `opensspm_event_inbox_dead_letter_rows{source_kind,source_name,channel}`
  - `opensspm_event_inbox_last_received_timestamp_seconds{source_kind,source_name,channel}`
  - `opensspm_event_inbox_last_processed_timestamp_seconds{source_kind,source_name,channel}`

## Security notes
- Open-SSPM includes in-app authentication (email/password) using server-side sessions stored in Postgres.
- Avoid logging connector secrets.
- Set a stable base64-encoded 32-byte `CONNECTOR_SECRET_KEY` (or `CONNECTOR_SECRET_KEY_FILE`) anywhere you run `api`, any `worker` lane, or sync commands. Losing that key means stored connector secrets must be re-entered.

## Contributing

See `CONTRIBUTING.md`.

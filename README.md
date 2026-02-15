<p align="center">Made with love in Paris 🇫🇷</p>

# Open-SSPM

Open-SSPM is a small “who has access to what” service. It syncs identities from Okta and Microsoft Entra ID (IdP sources), permissions from connected apps (GitHub, Datadog, AWS Identity Center), links accounts (auto by email + manual links), and renders a server-side UI.

## Demo
- URL: `https://demo.opensspm.com`
- Login: `admin@admin.com` / `admin`

## Features
- HTTP server (`open-sspm serve`) + background full sync worker (`open-sspm worker`) + background discovery worker (`open-sspm worker-discovery`) + one-off syncs (`open-sspm sync`, `open-sspm sync-discovery`) + in-app “Resync” (queued async by default).
- Okta: users, groups, apps, and assignments (IdP source).
- Microsoft Entra ID: users plus application/service principal governance metadata.
- SaaS Discovery: discovered app inventory + hotspots from IdP SSO and OAuth grant evidence (Okta System Log + Entra sign-ins/grants), with governance and binding workflows.
- GitHub: org members/teams/repo permissions (optional SCIM lookup for emails).
- Datadog: users + role assignments.
- AWS Identity Center: users + account/permission set assignments.
- Programmatic access governance: browse app assets and credentials with risk labels, expiry filters, and actor attribution links.
- Matching: automatic by email (case-insensitive) + manual linking for accounts without email.
- Findings: Okta CIS benchmark rule evaluations (rules must be seeded; see below).
- Server-rendered UI: Echo + templ; Tailwind v4 + Basecoat; minimal vanilla JS for UX.

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
5. Run the server: `just run`
6. Run background workers: `just worker` (full lane) and `go run ./cmd/open-sspm worker-discovery` (discovery lane).
7. Open `http://localhost:8080`, configure connectors under Settings → Connectors, then run a sync (Settings → Resync queues workers by default, or use `just sync` for one-off inline execution).
8. Optional: enable SaaS discovery on Okta/Entra connector settings, run sync, then review `http://localhost:8080/discovery/apps` and `http://localhost:8080/discovery/hotspots`.

## Findings / rules (Okta benchmark)
Rulesets are loaded from generated Open SSPM spec Go entities (`github.com/open-sspm/open-sspm-spec/gen/go/opensspm/spec/v2`) and must be seeded into Postgres before they show up in the UI:
- `go run ./cmd/open-sspm seed-rules`

After seeding, run an Okta sync and open `http://localhost:8080/findings/rulesets/cis.okta.idaas_stig.v2`.

## Dev workflows
- Live-reload server: `just dev` (requires `air` + `templ`)
- Run background full sync worker: `just worker`
- Run background discovery sync worker: `go run ./cmd/open-sspm worker-discovery`
- Watch CSS: `just ui-watch`
- Sync vendored runtime JS: `npm run vendor:sync` (also runs automatically after `npm install` / `npm ci`)
- Check vendored runtime JS drift: `npm run vendor:check`
- Run frontend JS unit tests: `just ui-test` (or `npm run test:js`)
- Regenerate templ templates: `just templ` (watch: `just templ-watch`)
- Regenerate SQLC code: `just sqlc` (generated code is checked in under `internal/db/gen`)

## Configuration
- Process-level env vars: `.env.example` (database, HTTP address, sync interval/workers).
- Manual resync mode: `RESYNC_MODE=signal` (default, queues workers via Postgres `NOTIFY`) or `RESYNC_MODE=inline` (request runs sync directly).
- Connector credentials: configured in-app under Settings → Connectors and stored in Postgres.
- AWS Identity Center uses the AWS SDK default credentials chain (env/shared config/role), not DB-stored keys.
- SaaS discovery is per-connector (`discovery_enabled`) for Okta and Entra.
  - Okta discovery uses System Log access.
  - Entra discovery uses sign-in and OAuth grant APIs (`AuditLog.Read.All`, `Directory.Read.All`, `DelegatedPermissionGrant.Read.All`).

## Metrics
- Metrics are served on a dedicated listener (`METRICS_ADDR`) and are best-effort.
- Metrics collection failures after successful syncs are tracked in `opensspm_sync_metrics_collection_failures_total`.
- Discovery metrics include:
  - `opensspm_discovery_events_ingested_total`
  - `opensspm_discovery_ingest_failures_total`
  - `opensspm_discovery_apps_total`
  - `opensspm_discovery_hotspots_total`

## Security notes
- Open-SSPM includes in-app authentication (email/password) using server-side sessions stored in Postgres.
- Avoid logging connector secrets; tokens are stored in Postgres.

## Contributing

See `CONTRIBUTING.md`.

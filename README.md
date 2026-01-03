
<div style="text-align: center;">Made with love in Paris 🇫🇷</div>

# Open-SSPM

Open-SSPM is a small “who has access to what” service. It syncs identities from Okta (IdP) and permissions from connected apps (GitHub, Datadog, AWS Identity Center), links accounts (auto by email + manual links), and renders a server-side UI.

## Features
- HTTP server (`open-sspm serve`) + background sync worker (`open-sspm worker`) + one-off sync (`open-sspm sync`) + in-app “Resync”.
- Okta: users, groups, apps, and assignments (IdP source).
- GitHub: org members/teams/repo permissions (optional SCIM lookup for emails).
- Datadog: users + role assignments.
- AWS Identity Center: users + account/permission set assignments.
- Matching: automatic by email (case-insensitive) + manual linking for accounts without email.
- Findings: Okta CIS benchmark rule evaluations (rules must be seeded; see below).
- Server-rendered UI: Echo + templ; Tailwind v4 + Basecoat; minimal vanilla JS for UX.

## Requirements
- Go 1.25.x (go.mod uses toolchain `go1.25.5`)
- Docker + Docker Compose
- Node.js + npm (for building CSS)
- Optional: `air` (live reload), `templ` (template generation), `sqlc` (regen DB code)

## Quickstart
1. Copy `.env.example` to `.env` and update as needed.
2. Start Postgres: `make dev-up`
3. Run migrations: `make migrate`
4. Install JS deps + build CSS: `npm install && make ui`
5. Run the server: `make run`
6. Optional: run the background sync worker: `make worker`
7. Open `http://localhost:8080`, configure connectors under Settings → Connectors, then run a sync (Settings → Resync, or `make sync`).

## Findings / rules (Okta benchmark)
Rulesets are embedded from a pinned Open SSPM descriptor snapshot (`internal/opensspm/specassets/descriptor.v1.json`) and must be seeded into Postgres before they show up in the UI:
- `go run ./cmd/open-sspm seed-rules`

After seeding, run an Okta sync and open `http://localhost:8080/findings/rulesets/cis.okta.idaas_stig.v1`.

## Dev workflows
- Live-reload server: `make dev` (requires `air` + `templ`)
- Run background sync worker: `make worker`
- Watch CSS: `make ui-watch`
- Regenerate templ templates: `make templ` (watch: `make templ-watch`)
- Regenerate SQLC code: `make sqlc` (generated code is checked in under `internal/db/gen`)

## Configuration
- Process-level env vars: `.env.example` (database, HTTP address, sync interval/workers).
- Connector credentials: configured in-app under Settings → Connectors and stored in Postgres.
- AWS Identity Center uses the AWS SDK default credentials chain (env/shared config/role), not DB-stored keys.

## Security notes
- There is no in-app authentication layer right now; run on a trusted network or behind your own auth proxy.
- Avoid logging connector secrets; tokens are stored in Postgres.

## Contributing

See `CONTRIBUTING.md`.

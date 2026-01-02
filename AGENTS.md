# Agent Notes (Open-SSPM)

## What this repo is
- We’re building a modern SSPM (SaaS Security Posture Management) security solution that answers “who has access to what”.
- Integrations (MVP): Okta (IdP) + GitHub org permissions + Datadog roles + AWS Identity Center assignments (+ Vault stub).
- Server-rendered UI (no SPA framework): `echo` + `templ` + Tailwind v4 + Basecoat (minimal vanilla JS for UX).
- Findings/benchmarks: rules are embedded from a pinned Open SSPM descriptor snapshot (`internal/opensspm/specassets`) and seeded to Postgres via `open-sspm seed-rules`.

## Common commands
- Start Postgres: `make dev-up` (stops with `make dev-down`)
- Run DB migration: `make migrate`
- Regenerate SQLC code: `make sqlc`
- Regenerate templ templates: `make templ`
- Watch templ templates: `make templ-watch`
- Seed benchmark rules: `go run ./cmd/open-sspm seed-rules`
- Sync once: `make sync`
- Run server: `make run`
- Run background sync worker: `make worker`
- Live-reload server (requires `air`): `make dev`
- Run tests: `make test` (`go test ./...`)
- Build CSS: `make ui` (watch: `make ui-watch`)

## Repo layout
- CLI entrypoint: `cmd/open-sspm` (`serve`, `worker`, `sync`, `migrate`, `seed-rules`)
- Config: `internal/config` (env loading + runtime settings)
- HTTP server + templates: `internal/http` (`handlers`, `viewmodels`, `views/*.templ`)
- Sync orchestration + scheduler: `internal/sync`
- Matching (linking): `internal/matching` (auto-link by email; manual links via UI)
- Rules engine: `internal/rules` (evaluation runs during Okta sync; results shown under `/findings`)
- Connectors: `internal/connectors/{okta,github,datadog,aws,vault}` (+ shared `registry`, `configstore`, `oktaapi`)
- DB schema + queries: `db/migrations`, `db/queries`
- SQLC generated code: `internal/db/gen` (do not edit by hand)
- Static assets: `web/static/src/input.css` → output `web/static/app.css` (gitignored)
- `air` config: `.air.toml` (writes temp binaries to `tmp/`)

## Conventions / guardrails
- Keep things KISS: one Go binary, Postgres as the only store, periodic sync.
- Prefer standard library patterns (`net/http`, context-aware calls, explicit timeouts).
- Database access goes through SQLC (`internal/db/gen`); change SQL in `db/queries/*.sql` then run `make sqlc`.
- When touching templates/CSS, keep HTML server-rendered; avoid adding a JS framework unless explicitly requested.
- UI primitives should be Basecoat-first (Basecoat components + Tailwind utilities for layout). Avoid adding DaisyUI or a second component system.
- Avoid logging secrets; runtime config comes from env vars (`.env.example` is the reference) and connector credentials are stored in Postgres.

# Repository Guidelines

## Project Structure & Module Organization

- `cmd/open-sspm/`: CLI entrypoint and subcommands (`serve`, `worker`, `sync`, `migrate`, `seed-rules`).
- `internal/`: application code (connectors, sync engine, HTTP server/UI, rules, access graph).
- `internal/opensspm/specassets/`: pinned Open SSPM spec lockfile metadata (`spec.lock.json`) used to track generated descriptor provenance/hash.
- `db/migrations/`: Postgres migrations.
- `db/queries/` + `sqlc.yaml`: SQL sources for SQLC; generated Go is checked in under `internal/db/gen/`.
- `internal/http/views/`: `templ` templates (generated `*_templ.go` files are checked in).
- `web/static/`: UI assets; Tailwind input in `web/static/src/`, built output `web/static/app.css` is gitignored.
- `helm/open-sspm/`: Helm chart.

## Build, Test, and Development Commands

Prereqs: Go 1.25.x (see `go.mod` toolchain), Docker + Compose, Node.js + npm.

- `just dev-up` / `just dev-down`: start/stop local Postgres.
- `just migrate`: run DB migrations (`open-sspm migrate`).
- `just run`: start the server at `http://localhost:8080`.
- `just worker`: run the background worker; `just sync`: run a one-off sync.
- `just test`: run unit tests (`go test ./...`). CI also runs `go vet ./...`.
- UI: `npm install && just ui` (build CSS) or `just ui-watch` (watch).
- Dev loop: `just dev` (live reload; requires `air` + `templ` installed).
- Spec update: `just update-open-sspm-spec` (expects `OPEN_SSPM_SPEC_REF`; `OPEN_SSPM_SPEC_REPO` defaults to and must remain `https://github.com/open-sspm/open-sspm-spec`; refreshes lockfile metadata and Go module pin to generated v2 types).

## Coding Style & Naming Conventions

- Go: run `gofmt` on changed files; keep packages lower-case and filenames `snake_case.go`.
- Don’t hand-edit generated code:
  - SQLC: `internal/db/gen/` (regen with `just sqlc`)
  - templ: `internal/http/views/*_templ.go` (regen with `just templ`)

## Testing Guidelines

- Tests use Go’s standard `testing` package and live as `*_test.go` next to the code they cover.
- Prefer small, deterministic tests; avoid network calls (mock at the connector boundary).

## Commit & Pull Request Guidelines

- PRs are squash-merged; the PR title becomes the commit message on `main`.
- PR titles must follow Conventional Commits: `type(scope): summary` (scope optional).
  - Types: `feat`, `fix`, `perf`, `refactor`, `docs`, `test`, `build`, `ci`, `chore`
  - Example: `feat(sync): add connector health`
- Include a clear description, link issues (e.g., “closes #123”), and add screenshots for UI changes.

## Security & Configuration Tips

- Copy `.env.example` → `.env` for local dev; never commit secrets (the repo ignores `.env`).
- Connector credentials are configured in-app and stored in Postgres; avoid logging tokens or secret fields.

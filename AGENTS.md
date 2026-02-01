# Repository Guidelines

## Project Structure & Module Organization

- `cmd/open-sspm/`: CLI entrypoint and subcommands (`serve`, `worker`, `sync`, `migrate`, `seed-rules`).
- `internal/`: application code (connectors, sync engine, HTTP server/UI, rules, access graph).
- `db/migrations/`: Postgres migrations.
- `db/queries/` + `sqlc.yaml`: SQL sources for SQLC; generated Go is checked in under `internal/db/gen/`.
- `internal/http/views/`: `templ` templates (generated `*_templ.go` files are checked in).
- `web/static/`: UI assets; Tailwind input in `web/static/src/`, built output `web/static/app.css` is gitignored.
- `helm/open-sspm/`: Helm chart.

## Build, Test, and Development Commands

Prereqs: Go 1.25.x (see `go.mod` toolchain), Docker + Compose, Node.js + npm.

- `make dev-up` / `make dev-down`: start/stop local Postgres.
- `make migrate`: run DB migrations (`open-sspm migrate`).
- `make run`: start the server at `http://localhost:8080`.
- `make worker`: run the background worker; `make sync`: run a one-off sync.
- `make test`: run unit tests (`go test ./...`). CI also runs `go vet ./...`.
- UI: `npm install && make ui` (build CSS) or `make ui-watch` (watch).
- Dev loop: `make dev` (live reload; requires `air` + `templ` installed).

## Coding Style & Naming Conventions

- Go: run `gofmt` on changed files; keep packages lower-case and filenames `snake_case.go`.
- Don’t hand-edit generated code:
  - SQLC: `internal/db/gen/` (regen with `make sqlc`)
  - templ: `internal/http/views/*_templ.go` (regen with `make templ`)

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

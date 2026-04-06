# Repository Guidelines

## Purpose

- Keep this file focused on durable engineering rules and product invariants.
- Treat `justfile`, `go.mod`, `package.json`, `sqlc.yaml`, and `.github/workflows/` as the source of truth for commands, tool versions, and CI behavior.

## Core Workflow

- Use `just` targets for routine local work; prefer existing tasks over ad hoc commands.
- Common flows live in `justfile` (`run`, `worker`, `sync`, `migrate`, `test`, `ui`, and codegen targets). Use `just --list` if unsure.
- When changing templates or Tailwind input, rebuild CSS with `just ui` or run `just ui-watch`; `web/static/app.css` is generated and gitignored.
- For Open SSPM spec updates, use `just update-open-sspm-spec`; do not hand-edit pinned spec metadata.

## Code Generation And Style

- Run `gofmt` on changed Go files. Keep package names lower-case and filenames `snake_case.go`.
- Do not hand-edit generated code.
- Regenerate SQLC output in `internal/db/gen/` via `just sqlc`.
- Regenerate templ output in `internal/http/views/*_templ.go` via `just templ`.

## Verification

- Run the smallest relevant checks for the area you changed, then the broader repo checks before handoff when appropriate.
- The normal verification bar for this repo includes Go tests, `go vet`, frontend JS tests, and Helm validation; `.github/workflows/ci.yml` is the source of truth.
- Tests should protect behavior, regressions, or safety-critical contracts.
- Frontend tests should assert user-visible behavior or interaction outcomes, not raw HTML structure, tag choices, or Tailwind classes.

## Product Invariants

- Empty `source_kind` and `source_name` mean "All configured"; aggregate across configured sources instead of defaulting to the first source.
- Default discovery rollups should operate on configured sources only and should not pull legacy or unconfigured rows into the default view.
- Sync runs must fail early on partial-stage errors; do not finalize or expire rows after a partial refresh.
- Discovery ingestion is incremental, not a full snapshot. Expiration must be staleness-based rather than "not seen in this run" alone.

## UI Conventions

- Prefer GET-based filters that auto-apply on select change. Free-text query should apply on Enter and include an inline clear action.
- Tailwind v4 uses `@source`; keep dynamic class strings in scanned sources or the CSS build may purge them.
- If `htmx:beforeRequest` sets loading state, clear it on every completion and error path: `htmx:afterRequest`, `htmx:afterSwap`, `htmx:onLoadError`, `htmx:swapError`, `htmx:sendAbort`, `htmx:sendError`, `htmx:timeout`, and `htmx:responseError`.
- Do not assume HTMX swap targets are always `HTMLElement`s.

## Security And Configuration

- Start from `.env.example` for local setup and never commit secrets.
- Connector credentials are stored in Postgres; do not log tokens, client secrets, or similar sensitive fields.


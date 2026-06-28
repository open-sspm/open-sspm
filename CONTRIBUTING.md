# Contributing

Thanks for helping improve Open-SSPM!

## Pull requests

- Create a short-lived branch from `main` (`feat/...`, `fix/...`, `chore/...`).
- Open a PR against `main`.
- Maintainers use **squash merge** so the PR title becomes the commit message on `main`.

## PR title format (required)

PR titles must use this format:

- `type: summary`

Allowed `type` values:
- `feat`, `fix`, `docs`, `chore`, `ci`, `refactor`, `perf`

Examples:
- `feat: add incremental GitHub sync`
- `fix: run seed-rules as pre-install hook`
- `docs: clarify managed Postgres requirement`
- `chore: update demo deployment docs`
- `ci: tighten Helm validation job`
- `refactor: simplify sync scheduler`
- `perf: reduce sync query overhead`

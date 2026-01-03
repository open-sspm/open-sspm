# Contributing

Thanks for helping improve Open-SSPM!

## Pull requests

- Create a short-lived branch from `main` (`feat/...`, `fix/...`, `chore/...`).
- Open a PR against `main`.
- Maintainers use **squash merge** so the PR title becomes the commit message on `main`.

## PR title format (required)

PR titles must follow Conventional Commits:

- `type(scope): summary`
- `type(scope)!: summary` for breaking changes

Allowed `type` values:
- `feat`, `fix`, `perf`, `refactor`, `docs`, `test`, `build`, `ci`, `chore`

Suggested `scope` values:
- `okta`, `github`, `datadog`, `aws`, `rules`, `sync`, `http`, `db`, `ui`, `helm`, `docker`

Examples:
- `feat(sync): add incremental GitHub sync`
- `fix(helm): run seed-rules as pre-install hook`
- `docs(readme): clarify managed Postgres requirement`
- `feat(api)!: change findings JSON output`

If you use `!` (breaking change), include migration notes in the PR description.


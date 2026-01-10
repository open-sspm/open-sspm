# Demo deployment (GitHub Actions)

Target:
- a single Scaleway VM (Ubuntu 24.04) with local Postgres
- `open-sspm` runs as a systemd service and serves HTTP on `127.0.0.1:8080`
- nginx listens on `:80` and reverse-proxies to `open-sspm`

This repo currently expects **runtime files** to exist next to the binary:
- static assets: `web/static/` (CSS build output must be present)
- migrations: `db/migrations/` (used by `open-sspm migrate`)

The Ansible bootstrap creates an empty working directory at:
- `/opt/open-sspm/web/static`
- `/opt/open-sspm/db/migrations`

## Recommended CI deploy artifact

Create a tarball containing:
- `open-sspm` (linux/amd64)
- `web/static/` (including built CSS)
- `db/migrations/`

Extract it on the server into `/opt/open-sspm/`.

## Manual deploy flow (high-level)

1) Build CSS in CI: `npm ci && npm run build:css`
2) Build the binary in CI: `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o open-sspm ./cmd/open-sspm`
3) Package runtime dirs:
   - `db/migrations/`
   - `web/static/`
4) Copy to the server over SSH (GitHub Actions secret key).
5) Run on the server:
   - stop `open-sspm` (if running)
   - reset the demo database (drop + recreate)
   - `open-sspm migrate`
   - `open-sspm seed-rules`
   - apply demo seed SQL (`demo/data/001_seed_demo.sql`)
   - restart `open-sspm` via systemd

## Ansible deploy (local build + sync)

If you deploy via Ansible, you still must build CSS on the control machine so `web/static/app.css` exists.

```bash
make ui
cd demo/infra/ansible
ansible-playbook -i inventory.ini deploy.yml
```

## GitHub Actions workflow

Workflow: `.github/workflows/deploy-demo.yml`

Note: the demo deploy workflow **drops and recreates** the Postgres database on every deploy to guarantee a consistent demo dataset.

Required GitHub secret:
- `DEMO_SSH_PRIVATE_KEY` (SSH private key for the `deploy` user)

Run it via Actions → “Deploy demo (Scaleway VM)” and pass the VM IP as the `host` input.

## Seeding demo data

This repo includes an **upsert-only** SQL seed at `demo/data/001_seed_demo.sql`.

The deploy workflow resets the demo database and applies this seed on every deployment.

## Demo login

Default demo credentials (bootstrapped automatically during deploy):
- email: `admin@admin.com`
- password: `admin`

You can also apply it manually from your machine, if you want to re-seed without redeploying:

```bash
cd demo/infra/ansible
ansible-playbook -i inventory.ini seed-demo.yml
```

## Secrets to plan for

- `SCW_ACCESS_KEY`, `SCW_SECRET_KEY`, `SCW_DEFAULT_PROJECT_ID` (Terraform)
- `DEMO_HOST`, `DEMO_SSH_USER`, `DEMO_SSH_PRIVATE_KEY` (deploy)
- DB password (already stored on the server in `/etc/open-sspm.env`)

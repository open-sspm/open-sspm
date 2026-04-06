# Docker-Backed Local Setup

This repository uses Docker for the local PostgreSQL dependency. The checked-in `docker-compose.yml` does **not** start the Open-SSPM web server or workers; it starts Postgres only.

## Prerequisites

- Docker 20.10+
- Docker Compose 2.0+
- Go 1.26.x
- Node.js + npm

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/open-sspm/open-sspm.git
cd open-sspm
```

### 2. Configure Environment Variables

```bash
cp .env.example .env
```

At minimum, make sure `.env` contains a working `DATABASE_URL`. Generate a stable connector key before saving any connector credentials:

```bash
export CONNECTOR_SECRET_KEY="$(openssl rand -base64 32)"
```

### 3. Start PostgreSQL

```bash
just dev-up
```

The local Postgres service listens on `localhost:5432`.

### 4. Build UI Assets

```bash
npm install
just ui
```

### 5. Run Migrations

```bash
just migrate
```

### 6. Create the First Admin User

```bash
printf '%s\n' 'change-me-now' | go run ./cmd/open-sspm users bootstrap-admin \
  --email admin@example.com \
  --password-stdin
```

`bootstrap-admin` is idempotent: if an admin already exists, it exits successfully without creating another one.

### 7. Start Open-SSPM

Run each process in its own terminal:

```bash
just run
```

```bash
just worker
```

```bash
just worker-discovery
```

The discovery worker is optional, but it must be running if you want SaaS discovery syncs and `SYNC_DISCOVERY_ENABLED=1`.

### 8. Access the Web UI

Open `http://localhost:8080` in your browser and sign in with the admin user you created.

## What `docker-compose.yml` Contains

The repository compose file currently defines:

- `db` - PostgreSQL with a persisted local data volume

That is why repo-local commands use `just run`, `just worker`, and `just worker-discovery` instead of `docker compose exec web ...`.

## Optional: Fully Containerized Compose Example

If you want to run the published container image in Docker Compose, create your own `compose.yaml`. The repository does not ship this file, but the following is a working starting point:

```yaml
services:
  db:
    image: postgres:17
    environment:
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: opensspm
    ports:
      - "5432:5432"
    volumes:
      - db-data:/var/lib/postgresql/data

  serve:
    image: ghcr.io/open-sspm/open-sspm:latest
    command: ["serve"]
    depends_on:
      - db
    environment:
      DATABASE_URL: postgres://postgres:postgres@db:5432/opensspm?sslmode=disable
      CONNECTOR_SECRET_KEY: ${CONNECTOR_SECRET_KEY}
      AUTH_COOKIE_SECURE: "0"
    ports:
      - "8080:8080"

  worker:
    image: ghcr.io/open-sspm/open-sspm:latest
    command: ["worker"]
    depends_on:
      - db
    environment:
      DATABASE_URL: postgres://postgres:postgres@db:5432/opensspm?sslmode=disable
      CONNECTOR_SECRET_KEY: ${CONNECTOR_SECRET_KEY}

  worker-discovery:
    image: ghcr.io/open-sspm/open-sspm:latest
    command: ["worker-discovery"]
    depends_on:
      - db
    environment:
      DATABASE_URL: postgres://postgres:postgres@db:5432/opensspm?sslmode=disable
      CONNECTOR_SECRET_KEY: ${CONNECTOR_SECRET_KEY}

volumes:
  db-data:
```

For that sample file:

```bash
docker compose run --rm serve migrate
printf '%s\n' 'change-me-now' | docker compose run --rm -T serve users bootstrap-admin \
  --email admin@example.com \
  --password-stdin
docker compose up -d
```

## Troubleshooting

### Postgres does not start

Check the local Docker service:

```bash
docker compose ps
docker compose logs db
```

### The UI loads without styles

Build the CSS bundle again:

```bash
just ui
```

### Syncs are not running

Make sure the background worker is running:

```bash
just worker
```

For discovery syncs, also run:

```bash
just worker-discovery
```

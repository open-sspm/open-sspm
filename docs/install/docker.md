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
printf '%s\n' 'change-me-now' | go run ./cmd/open-sspm admin users bootstrap-admin \
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
just worker discovery
```

```bash
just worker event-inbox
```

The discovery worker is optional, but it must be running if you want polling-based SaaS discovery syncs and `SYNC_DISCOVERY_ENABLED=1`. The event inbox worker is optional unless you enable the event inbox for sources such as Okta Event Hooks or EventBridge.

### 8. Access the Web UI

Open `http://localhost:8080` in your browser and sign in with the admin user you created.

## What `docker-compose.yml` Contains

The repository compose file currently defines:

- `db` - PostgreSQL with a persisted local data volume

That is why repo-local commands use `just run`, `just worker`, `just worker discovery`, and `just worker event-inbox` instead of `docker compose exec web ...`.

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

  api:
    image: ghcr.io/open-sspm/open-sspm:latest
    command: ["api"]
    depends_on:
      - db
    environment:
      DATABASE_URL: postgres://postgres:postgres@db:5432/opensspm?sslmode=disable
      CONNECTOR_SECRET_KEY: ${CONNECTOR_SECRET_KEY}
      AUTH_COOKIE_SECURE: "0"
    ports:
      - "8080:8080"

  worker-full:
    image: ghcr.io/open-sspm/open-sspm:latest
    command: ["worker", "--lane=full"]
    depends_on:
      - db
    environment:
      DATABASE_URL: postgres://postgres:postgres@db:5432/opensspm?sslmode=disable
      CONNECTOR_SECRET_KEY: ${CONNECTOR_SECRET_KEY}

  worker-lane-discovery:
    image: ghcr.io/open-sspm/open-sspm:latest
    command: ["worker", "--lane=discovery"]
    depends_on:
      - db
    environment:
      DATABASE_URL: postgres://postgres:postgres@db:5432/opensspm?sslmode=disable
      CONNECTOR_SECRET_KEY: ${CONNECTOR_SECRET_KEY}

  worker-lane-event-inbox:
    image: ghcr.io/open-sspm/open-sspm:latest
    command: ["worker", "--lane=event-inbox"]
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
docker compose run --rm api admin migrate
printf '%s\n' 'change-me-now' | docker compose run --rm -T api admin users bootstrap-admin \
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
just worker discovery
```

For event inbox processing, also run:

```bash
just worker event-inbox
```

# Installation

This guide covers the repo-supported ways to run Open-SSPM.

## Choose Your Installation Method

| Method | Best For | Complexity |
|--------|----------|------------|
| [Docker-backed local setup](/install/docker) | Local development and evaluation | Low |
| [Kubernetes (Helm)](/install/kubernetes) | Containerized deployments | Medium |

## Prerequisites

All installation methods require:

- **PostgreSQL 14+** - Open-SSPM stores application, sync, and auth data in Postgres

Before you configure connectors, you also need:

- **Connector Secret Key** - A base64-encoded 32-byte key for encrypting stored connector credentials

For repo-local development you also need:

- **Go 1.26.x**
- **Node.js + npm** for building CSS assets
- **Docker Compose** if you want to use the checked-in local Postgres service

### Generate a Connector Secret Key

Before configuring any connectors, generate a secret key:

```bash
openssl rand -base64 32
```

Save this value. You will need it for the `CONNECTOR_SECRET_KEY` environment variable or an equivalent secret file/secret manager entry.

::: warning Important
If you lose this key, you will need to re-enter all stored connector credentials. Store it securely.
:::

## Database Setup

Open-SSPM requires a PostgreSQL database. You have several options:

### Option 1: Repo-local Postgres with Docker

The repository ships a `docker-compose.yml` for local development that starts PostgreSQL only:

```bash
just dev-up
```

This exposes Postgres on `localhost:5432`.

### Option 2: Managed PostgreSQL

Use a managed service such as:

- AWS RDS
- Google Cloud SQL
- Azure Database for PostgreSQL
- DigitalOcean Managed Databases

### Database Connection URL

Format your connection URL as:

```text
postgres://USER:PASSWORD@HOST:PORT/DATABASE?sslmode=require
```

Example local URL:

```text
postgres://postgres:postgres@localhost:5432/opensspm?sslmode=disable
```

## Next Steps

After installation:

1. Run migrations.
2. Create the first admin user.
3. Start `serve` and the background worker processes.
4. Configure connectors in the web UI.
5. Run an initial sync.

See the [Configuration Guide](/config/) for environment variables and connector setup.

# Configuration

This section covers the main configuration surfaces for Open-SSPM.

## Configuration Methods

Open-SSPM is configured with environment variables. Common ways to provide them are:

1. A local `.env` file for repo-local development
2. Kubernetes Secrets plus Helm values
3. Environment variables injected by your container runtime or process supervisor

## Core Configuration

These values matter first:

| Variable | Description | Example |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection URL | `postgres://user:pass@host:5432/opensspm?sslmode=require` |
| `CONNECTOR_SECRET_KEY` or `CONNECTOR_SECRET_KEY_FILE` | Base64-encoded 32-byte key used to encrypt stored connector secrets; required before saving connector credentials | `MDEyMzQ1Njc4OWFiY2RlZj...` |

## Configuration Sections

- [Environment Variables Reference](/config/environment-variables) - Process-level settings and defaults
- [Database Configuration](/config/database) - PostgreSQL setup and migrations
- [Authentication](/config/authentication) - Login, sessions, cookies, and admin bootstrap
- [Connectors](/config/connectors/) - IdP and SaaS connector setup

## Quick Configuration Example

### Local `.env`

```bash
# Database
DATABASE_URL=postgres://postgres:postgres@localhost:5432/opensspm?sslmode=disable

# Security
CONNECTOR_SECRET_KEY=$(openssl rand -base64 32)

# Server
HTTP_ADDR=:8080
AUTH_COOKIE_SECURE=0

# Logging
LOG_FORMAT=json
LOG_LEVEL=info

# Sync
SYNC_INTERVAL=15m
SYNC_DISCOVERY_INTERVAL=15m
SYNC_DISCOVERY_ENABLED=1
RESYNC_MODE=signal
```

### Helm Values

```yaml
config:
  syncInterval: 15m
  syncDiscoveryInterval: 15m
  syncDiscoveryEnabled: true
  logFormat: json
  logLevel: info
  authCookieSecure: true

database:
  existingSecret:
    name: open-sspm-db
    key: DATABASE_URL

connectorSecret:
  existingSecret:
    name: open-sspm-app
    key: CONNECTOR_SECRET_KEY
```

## First-Time Setup Checklist

After setting configuration:

1. Verify the database connection.
2. Run migrations.
3. Bootstrap the first admin user.
4. Start `api` and the worker processes.
5. Configure connectors in the UI.
6. Run an initial sync.

## Next Steps

- Review the [Environment Variables Reference](/config/environment-variables)
- Configure [Connectors](/config/connectors/)
- See the [Running Guide](/run/) for day-to-day operations

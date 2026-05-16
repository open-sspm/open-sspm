# Environment Variables Reference

This page documents the main environment variables used by Open-SSPM.

## Required Variables

### DATABASE_URL

**Required** - PostgreSQL connection URL.

```bash
DATABASE_URL=postgres://user:password@host:port/database?sslmode=require
```

Examples:

- Local development: `postgres://postgres:postgres@localhost:5432/opensspm?sslmode=disable`
- Production with SSL: `postgres://user:pass@db.example.com:5432/opensspm?sslmode=require`

### CONNECTOR_SECRET_KEY

Required if you want to store connector credentials in the database.

Generate a new key:

```bash
openssl rand -base64 32
```

Example:

```bash
CONNECTOR_SECRET_KEY=MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=
```

::: warning
Store this key securely. Losing it means stored connector credentials must be entered again.
:::

### CONNECTOR_SECRET_KEY_FILE

Alternative to `CONNECTOR_SECRET_KEY`. Path to a file containing the base64-encoded key.

```bash
CONNECTOR_SECRET_KEY_FILE=/etc/open-sspm/connector-secret-key
```

## Server Configuration

### HTTP_ADDR

Address and port for the HTTP server.

- **Default:** `:8080`

```bash
HTTP_ADDR=:8080
```

### METRICS_ADDR

Address for the metrics endpoint.

- **Default in the Go binary:** disabled (`""`)
- **Disable explicitly with:** `off`, `disabled`, or `false`
- **Common deployment value:** `127.0.0.1:9090`

```bash
METRICS_ADDR=127.0.0.1:9090
```

### STATIC_DIR

Absolute path to static assets. If unset, Open-SSPM searches common `web/static` locations relative to the working directory and executable path.

```bash
STATIC_DIR=/opt/open-sspm/web/static
```

## Queue Configuration

### QUEUE_BACKEND

Backend used for push ingest dispatch. Postgres remains the durable inbox and source of truth.

- **Values:** `postgres`, `redis`
- **Default:** `postgres`

```bash
QUEUE_BACKEND=redis
```

### REDIS_URL

Redis connection URL. Required by the `api` and `worker-ingest` processes when `QUEUE_BACKEND=redis`.
Use `rediss://` for TLS-enabled Redis endpoints. If Redis is unavailable, push processing falls back to Postgres inbox polling.

```bash
REDIS_URL=redis://localhost:6379/0
```

### REDIS_KEY_PREFIX

Prefix for Open-SSPM Redis keys.

- **Default:** `open-sspm`

```bash
REDIS_KEY_PREFIX=open-sspm
```

## Logging

### LOG_FORMAT

- **Values:** `json`, `text`
- **Default:** `json`

```bash
LOG_FORMAT=json
```

### LOG_LEVEL

- **Values:** `debug`, `info`, `warn`, `error`
- **Default:** `info`

```bash
LOG_LEVEL=info
```

## Authentication

### AUTH_COOKIE_SECURE

- **Values:** `0`, `1`
- **Default:** `0`

```bash
AUTH_COOKIE_SECURE=1
```

Set this to `1` when users access Open-SSPM over HTTPS.

### TRUSTED_PROXY_CIDRS

Comma-separated CIDRs to trust when deriving client IPs from `X-Forwarded-For`.

```bash
TRUSTED_PROXY_CIDRS=35.191.0.0/16,130.211.0.0/22
```

By default, private, link-local, and loopback ranges are trusted.

### DEV_SEED_ADMIN

Development-only helper that creates `admin@admin.com` / `admin` if no auth users exist.

- **Values:** `0`, `1`
- **Default:** `0`

```bash
DEV_SEED_ADMIN=1
```

## SMTP

Open-SSPM can be configured to send email through an SMTP relay. SMTP is disabled by default.

::: info
The current release adds SMTP transport configuration only. There is not yet a built-in password reset, invitation, or email verification flow.
:::

### SMTP_ENABLED

- **Values:** `0`, `1`
- **Default:** `0`

```bash
SMTP_ENABLED=1
```

### SMTP_HOST

SMTP server hostname. Required when `SMTP_ENABLED=1`.

```bash
SMTP_HOST=smtp.example.com
```

### SMTP_PORT

- **Default:** `587`

```bash
SMTP_PORT=587
```

### SMTP_TLS_MODE

- **Values:** `starttls`, `tls`, `plain`
- **Default:** `starttls`

```bash
SMTP_TLS_MODE=starttls
```

Use `tls` for implicit TLS (commonly port `465`). Use `plain` only for trusted relays without authentication, or for localhost SMTP testing.

### SMTP_FROM_ADDRESS

Sender email address. Required when `SMTP_ENABLED=1`.

```bash
SMTP_FROM_ADDRESS=noreply@example.com
```

Supports either a bare email address or a full mailbox such as `noreply@example.com` or `"Open SSPM <noreply@example.com>"`. Prefer setting any display name in `SMTP_FROM_NAME`; if `SMTP_FROM_NAME` is empty and a display name is included here, it will be used.

### SMTP_FROM_NAME

Optional sender display name.

```bash
SMTP_FROM_NAME="Open SSPM"
```

### SMTP_USERNAME / SMTP_PASSWORD

Optional SMTP authentication credentials. Set both or leave both empty.

```bash
SMTP_USERNAME=mailer
SMTP_PASSWORD=change-me
```

When `SMTP_TLS_MODE=plain`, authenticated SMTP is supported only for localhost relays.

## Sync

### SYNC_FULL_ENABLED

- **Values:** `0`, `1`
- **Default:** `1`

Controls the full-sync lane everywhere it can be triggered. When disabled, the
full worker should not run and API-triggered resyncs will not enqueue full-sync
jobs.

```bash
SYNC_FULL_ENABLED=1
```

### SYNC_INTERVAL

- **Default:** `15m`

```bash
SYNC_INTERVAL=15m
```

### SYNC_DISCOVERY_INTERVAL

- **Default:** `15m`

```bash
SYNC_DISCOVERY_INTERVAL=15m
```

### SYNC_TAIL_INTERVAL

Cadence for the incremental tail worker. Tail jobs are also woken by push ingest when a provider event should trigger a scoped catch-up.

- **Default:** `5m`

```bash
SYNC_TAIL_INTERVAL=5m
```

### SYNC_DISCOVERY_ENABLED

- **Values:** `0`, `1`
- **Default:** `1`

```bash
SYNC_DISCOVERY_ENABLED=1
```

### OKTA_PUSH_INGEST_ENABLED

Controls the Okta push ingest lane. If unset, it follows `SYNC_DISCOVERY_ENABLED` for backward compatibility.

- **Values:** `0`, `1`
- **Default:** inherited from `SYNC_DISCOVERY_ENABLED`

```bash
OKTA_PUSH_INGEST_ENABLED=1
```

### RESYNC_ENABLED

- **Values:** `0`, `1`
- **Default:** `1`

```bash
RESYNC_ENABLED=1
```

### RESYNC_MODE

- **Values:** `signal`, `inline`
- **Default:** `signal`

```bash
RESYNC_MODE=signal
```

### GLOBAL_EVAL_MODE

- **Values:** `best_effort`, `strict`
- **Default:** `best_effort`

```bash
GLOBAL_EVAL_MODE=best_effort
```

## Sync Locking

### SYNC_LOCK_MODE

Locking mechanism for sync coordination.

- **Values:** `lease`, `advisory`
- **Default:** `lease`

```bash
SYNC_LOCK_MODE=lease
```

### SYNC_LOCK_TTL

- **Default:** `60s`

```bash
SYNC_LOCK_TTL=60s
```

### SYNC_LOCK_HEARTBEAT_INTERVAL

- **Default:** `15s`

```bash
SYNC_LOCK_HEARTBEAT_INTERVAL=15s
```

### SYNC_LOCK_HEARTBEAT_TIMEOUT

- **Default:** `15s`

```bash
SYNC_LOCK_HEARTBEAT_TIMEOUT=15s
```

### SYNC_LOCK_INSTANCE_ID

Optional instance identifier for lock observability. If unset, Open-SSPM falls back to `HOSTNAME` or the OS hostname.

```bash
SYNC_LOCK_INSTANCE_ID=worker-01
```

## Per-Connector Intervals

Leave these unset to inherit `SYNC_INTERVAL`:

```bash
SYNC_OKTA_INTERVAL=15m
SYNC_ENTRA_INTERVAL=15m
SYNC_GOOGLE_WORKSPACE_INTERVAL=15m
SYNC_GITHUB_INTERVAL=15m
SYNC_DATADOG_INTERVAL=15m
SYNC_AWS_INTERVAL=15m
```

### SYNC_FAILURE_BACKOFF_MAX

Maximum delay after repeated sync failures.

If unset, the workers derive it from the active lane interval:

- Full sync worker: `SYNC_INTERVAL * 10`
- Discovery worker: `SYNC_DISCOVERY_INTERVAL * 10`

```bash
SYNC_FAILURE_BACKOFF_MAX=2h
```

## Okta Push Ingest

These settings tune the `worker-ingest` Okta push inbox processor.

```bash
OKTA_PUSH_INGEST_BATCH_SIZE=500
OKTA_PUSH_INGEST_POLL_INTERVAL=5s
OKTA_PUSH_INGEST_CLEANUP_INTERVAL=1h
OKTA_PUSH_INGEST_RETRY_DELAY=30s
OKTA_PUSH_INGEST_RETRY_MAX_DELAY=15m
OKTA_PUSH_INGEST_STALE_PROCESSING_TIMEOUT=5m
OKTA_PUSH_INGEST_MAX_ATTEMPTS=10
OKTA_PUSH_INGEST_PROCESSED_RETENTION_DAYS=30
OKTA_PUSH_INGEST_DEAD_LETTER_RETENTION_DAYS=90
```

## Riskpolicy Event Worker

These settings tune `worker-riskpolicy`, which evaluates canonical provider events in shadow mode and projects shadow findings. It does not replace the user-facing current rules/finding behavior by itself.

```bash
RISKPOLICY_EVENT_WORKER_POLL_INTERVAL=5s
RISKPOLICY_EVENT_WORKER_BATCH_SIZE=100
RISKPOLICY_EVENT_WORKER_MAX_ATTEMPTS=10
```

## Event Storage Maintenance

Worker processes maintain daily partitions for canonical `events` and `event_targets`. They create partitions ahead of time and drop expired daily partitions as the retention window advances.

```bash
EVENT_PARTITION_MAINTENANCE_INTERVAL=12h
EVENT_PARTITION_FUTURE_DAYS=7
EVENT_RETENTION_DAYS=90
```

## Worker Concurrency

### SYNC_OKTA_WORKERS

- **Default:** `3`

```bash
SYNC_OKTA_WORKERS=3
```

### SYNC_GITHUB_WORKERS

- **Default:** `6`

```bash
SYNC_GITHUB_WORKERS=6
```

### SYNC_DATADOG_WORKERS

- **Default:** `3`

```bash
SYNC_DATADOG_WORKERS=3
```

## Example

```bash
DATABASE_URL=postgres://postgres:postgres@localhost:5432/opensspm?sslmode=disable
CONNECTOR_SECRET_KEY=$(openssl rand -base64 32)

HTTP_ADDR=:8080
LOG_FORMAT=json
LOG_LEVEL=info
AUTH_COOKIE_SECURE=0

SYNC_INTERVAL=15m
SYNC_DISCOVERY_INTERVAL=15m
SYNC_TAIL_INTERVAL=5m
SYNC_DISCOVERY_ENABLED=1
RESYNC_MODE=signal

EVENT_RETENTION_DAYS=90
RISKPOLICY_EVENT_WORKER_BATCH_SIZE=100
RISKPOLICY_EVENT_WORKER_MAX_ATTEMPTS=10

SYNC_OKTA_WORKERS=3
SYNC_GITHUB_WORKERS=6
SYNC_DATADOG_WORKERS=3
```

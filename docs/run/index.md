# Running Open-SSPM

This guide covers day-to-day operation of Open-SSPM.

## Components

| Command | Purpose | Normally Running? |
|---------|---------|-------------------|
| `open-sspm api` | Web UI and API | Yes |
| `open-sspm worker` | Background full sync loop | Yes |
| `open-sspm worker-discovery` | Background discovery sync loop | Optional |
| `open-sspm worker-ingest` | Background push ingest queue processing | Optional |

## Starting the Application

### Repo-Local Workflow

Start Postgres:

```bash
just dev-up
```

Run the app processes in separate terminals:

```bash
just run
```

```bash
just worker
```

```bash
just worker-discovery
```

```bash
just worker-ingest
```

The discovery worker is only needed when `SYNC_DISCOVERY_ENABLED=1` and you want polling-based discovery data. The ingest worker is needed when you enable push ingest such as Okta Event Hooks or EventBridge.

### Kubernetes

The Helm chart runs the commands as Deployments:

```bash
kubectl get deployments -l app.kubernetes.io/name=open-sspm
kubectl get pods -l app.kubernetes.io/name=open-sspm
```

Scale a component:

```bash
kubectl scale deployment open-sspm-api --replicas=2
kubectl scale deployment open-sspm-worker --replicas=1
kubectl scale deployment open-sspm-worker-discovery --replicas=1
kubectl scale deployment open-sspm-worker-ingest --replicas=1
```

## Stopping the Application

### Repo-Local Workflow

- Stop `api` and worker processes with `Ctrl-C` in each terminal.
- Stop the local Postgres dependency with:

```bash
just dev-down
```

### Kubernetes

Scale Deployments to zero:

```bash
kubectl scale deployment open-sspm-api --replicas=0
kubectl scale deployment open-sspm-worker --replicas=0
kubectl scale deployment open-sspm-worker-discovery --replicas=0
kubectl scale deployment open-sspm-worker-ingest --replicas=0
```

## Viewing Logs

### Repo-Local Workflow

`just run`, `just worker`, `just worker-discovery`, and `just worker-ingest` log directly to their terminal sessions.

For the local Postgres container:

```bash
docker compose logs db
```

### Kubernetes

```bash
kubectl logs -l app.kubernetes.io/component=api -f
kubectl logs -l app.kubernetes.io/component=worker -f
kubectl logs -l app.kubernetes.io/component=worker-discovery -f
kubectl logs -l app.kubernetes.io/component=worker-ingest -f
```

## Manual Sync Operations

### Trigger a Sync from the UI

- **Settings → Resync data → Resync now** queues an immediate full sync, and also queues discovery when discovery is enabled globally.
- **Settings → Connector health → Trigger sync** queues a sync for a specific connector.

### Trigger a Sync from the CLI

```bash
just sync
```

`open-sspm sync` runs the full sync lane and then runs discovery if `SYNC_DISCOVERY_ENABLED=1`.

To run only the discovery lane:

```bash
just sync-discovery
```

### Manual Resync Mode

`RESYNC_MODE` controls how UI-triggered resyncs execute:

| Mode | Behavior | Use Case |
|------|----------|----------|
| `signal` | Queue durable jobs for background workers | Normal operation |
| `inline` | Run the sync inside the HTTP request | Debugging and single-process setups |

## Monitoring Sync Status

Use **Settings → Connector health** to review:

- Last successful run
- Last attempted run
- Recent success rate
- Average successful run duration
- Latest connector-specific errors

## Health Checks

### Web Health Endpoint

```bash
curl http://localhost:8080/healthz
```

Expected response:

```text
ok
```

### Metrics Endpoint

Metrics are only exposed when `METRICS_ADDR` is set to a non-empty value.

Example:

```bash
curl http://127.0.0.1:9090/metrics
```

## Backup Operations

### Database Backup

```bash
pg_dump "$DATABASE_URL" > opensspm-backup-$(date +%Y%m%d).sql
```

If you are using the repo-local Postgres container:

```bash
docker compose exec -T db pg_dump -U postgres opensspm > backup.sql
```

### Restore

```bash
psql "$DATABASE_URL" < backup.sql
```

For the repo-local Postgres container:

```bash
docker compose exec -T db psql -U postgres opensspm < backup.sql
```

## Updates and Upgrades

### Repo-Local Workflow

After updating the checkout:

```bash
just migrate
just ui
```

Then restart `api` and the worker processes.

### Kubernetes

```bash
helm upgrade open-sspm ./helm/open-sspm -f values.yaml
kubectl rollout status deployment/open-sspm-api
```

## Troubleshooting

### Web UI not loading

Check:

1. `just run` is running.
2. `HTTP_ADDR` is correct.
3. CSS assets were built with `just ui`.
4. The application can reach `DATABASE_URL`.

### Syncs are not running

Check:

1. `just worker` is running.
2. `RESYNC_MODE=signal` has a background worker available.
3. Connector health shows recent runs and errors.

### Discovery data is missing

Check:

1. `SYNC_DISCOVERY_ENABLED=1`
2. `just worker-discovery` is running
3. Discovery is enabled on the relevant IdP connector

### Okta push ingest is not processing

Check:

1. `just worker-ingest` is running.
2. The Okta Event Hook or EventBridge endpoint reaches the API process.
3. The connector ingest mode and shared secret match the delivery channel.

### Database connection errors

Test the connection directly:

```bash
psql "$DATABASE_URL" -c "SELECT 1"
```

## Getting Help

Review application logs first, then open an issue on GitHub with:

- The command or deployment mode you are using
- Relevant error messages
- Steps to reproduce the problem

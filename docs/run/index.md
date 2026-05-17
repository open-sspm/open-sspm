# Running Open-SSPM

This guide covers day-to-day operation of Open-SSPM.

For the realtime architecture and provider matrix, see [Real-Time Synchronization](/run/real-time-synchronization).

## Components

| Command | Purpose | Normally Running? |
|---------|---------|-------------------|
| `open-sspm api` | Web UI and API | Yes |
| `open-sspm worker` | Background full sync loop | Yes |
| `open-sspm worker-discovery` | Background discovery sync loop | Optional |
| `open-sspm worker-ingest` | Background push ingest queue processing | Optional |
| `open-sspm worker-tail` | Background incremental audit/delta tail loop | Yes for realtime sync |
| `open-sspm worker-riskpolicy` | Background shadow canonical-event riskpolicy loop | Yes for event shadow evaluation |

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

```bash
just worker-tail
```

```bash
just worker-riskpolicy
```

The discovery worker is only needed when `SYNC_DISCOVERY_ENABLED=1` and you want polling-based discovery data. The ingest worker is needed when you enable push ingest such as Okta Event Hooks or EventBridge. The tail worker keeps cursor-based providers caught up, and the riskpolicy worker evaluates canonical events in shadow mode.

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
kubectl scale deployment open-sspm-worker-tail --replicas=1
kubectl scale deployment open-sspm-worker-riskpolicy --replicas=1
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
kubectl scale deployment open-sspm-worker-tail --replicas=0
kubectl scale deployment open-sspm-worker-riskpolicy --replicas=0
```

## Viewing Logs

### Repo-Local Workflow

`just run`, `just worker`, `just worker-discovery`, `just worker-ingest`, `just worker-tail`, and `just worker-riskpolicy` log directly to their terminal sessions.

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
kubectl logs -l app.kubernetes.io/component=worker-tail -f
kubectl logs -l app.kubernetes.io/component=worker-riskpolicy -f
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

### Shadow Event Projection

Canonical events can be replayed into the shadow discovery projection and compared against baseline discovery event rows:

```bash
just event-projection -- --source-kind okta --source-name your-org.okta.com
```

This command records parity diff rows and does not cut the UI over to the shadow read model.
When `--since` is omitted, projection resumes from the last stored checkpoint for that source.

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

### Tail jobs are not running

Check:

1. `just worker-tail` is running.
2. `SYNC_TAIL_INTERVAL` is a positive duration.
3. The connector declares an executable tail capability. Current executable tails include Okta System Log, Google Workspace Reports activities, Datadog Audit Logs, and AWS CloudTrail when the provider client is configured.

### Shadow event findings are not updating

Check:

1. `just worker-riskpolicy` is running.
2. Canonical events are being written for the source.
3. `riskpolicy_event_queue` rows are not stuck in `processing`; stale rows are requeued by the worker.

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

### Event partitions are not advancing

Check:

1. At least one worker process is running.
2. `EVENT_PARTITION_MAINTENANCE_INTERVAL`, `EVENT_PARTITION_FUTURE_DAYS`, and `EVENT_RETENTION_DAYS` are positive.
3. Worker logs include `event partition maintenance complete`.

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

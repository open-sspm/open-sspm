# Running Open-SSPM

This guide covers day-to-day operation of Open-SSPM.

For the realtime architecture and provider matrix, see [Real-Time Synchronization](/run/real-time-synchronization).

## Components

| Command | Purpose | Normally Running? |
|---------|---------|-------------------|
| `open-sspm api` | Web UI and API | Yes |
| `open-sspm worker` | Background full sync loop | Yes |
| `open-sspm worker --lane discovery` | Background discovery sync loop | Optional |
| `open-sspm worker --lane event-inbox` | Background event inbox processing | Optional |
| `open-sspm worker --lane tail` | Background incremental audit/delta tail loop | Yes for realtime sync |
| `open-sspm worker --lane evaluator` | Background canonical-event evaluator loop | Yes for event evaluation |

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
just worker discovery
```

```bash
just worker event-inbox
```

```bash
just worker tail
```

```bash
just worker evaluator
```

The discovery worker is only needed when `SYNC_DISCOVERY_ENABLED=1` and you want polling-based discovery data. The event inbox worker is needed when you enable the event inbox for sources such as Okta Event Hooks or EventBridge. The tail worker keeps cursor-based providers caught up, and the evaluator worker evaluates canonical events.

### Kubernetes

The Helm chart runs the commands as Deployments:

```bash
kubectl get deployments -l app.kubernetes.io/name=open-sspm
kubectl get pods -l app.kubernetes.io/name=open-sspm
```

Scale a component:

```bash
kubectl scale deployment open-sspm-api --replicas=2
kubectl scale deployment open-sspm-worker-lane-full --replicas=1
kubectl scale deployment open-sspm-worker-lane-discovery --replicas=1
kubectl scale deployment open-sspm-worker-lane-event-inbox --replicas=1
kubectl scale deployment open-sspm-worker-lane-tail --replicas=1
kubectl scale deployment open-sspm-worker-lane-evaluator --replicas=1
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
kubectl scale deployment open-sspm-worker-lane-full --replicas=0
kubectl scale deployment open-sspm-worker-lane-discovery --replicas=0
kubectl scale deployment open-sspm-worker-lane-event-inbox --replicas=0
kubectl scale deployment open-sspm-worker-lane-tail --replicas=0
kubectl scale deployment open-sspm-worker-lane-evaluator --replicas=0
```

## Viewing Logs

### Repo-Local Workflow

`just run`, `just worker`, `just worker discovery`, `just worker event-inbox`, `just worker tail`, and `just worker evaluator` log directly to their terminal sessions.

For the local Postgres container:

```bash
docker compose logs db
```

### Kubernetes

```bash
kubectl logs -l app.kubernetes.io/component=api -f
kubectl logs -l app.kubernetes.io/component=worker,open-sspm.io/worker-lane=full -f
kubectl logs -l app.kubernetes.io/component=worker,open-sspm.io/worker-lane=discovery -f
kubectl logs -l app.kubernetes.io/component=worker,open-sspm.io/worker-lane=event-inbox -f
kubectl logs -l app.kubernetes.io/component=worker,open-sspm.io/worker-lane=tail -f
kubectl logs -l app.kubernetes.io/component=worker,open-sspm.io/worker-lane=evaluator -f
```

## Manual Sync Operations

### Trigger a Sync from the UI

- **Settings → Resync data → Resync now** queues an immediate full sync, and also queues discovery when discovery is enabled globally.
- **Settings → Connector health → Trigger sync** queues a sync for a specific connector.

### Trigger a Sync from the CLI

```bash
just sync
```

`open-sspm admin sync` runs the full sync lane and then runs discovery if `SYNC_DISCOVERY_ENABLED=1`. Use `open-sspm admin sync --lane full` to run only full reconciliation.

To run only the discovery lane:

```bash
just sync discovery
```

Direct command:

```bash
open-sspm admin sync --lane discovery
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

### Tail jobs are not running

Check:

1. `just worker tail` is running.
   Direct command: `open-sspm worker --lane tail`.
2. `SYNC_TAIL_INTERVAL` is a positive duration.
3. The connector declares an executable tail capability. Current executable tails include Okta System Log, Google Workspace Reports activities, Datadog Audit Logs, and AWS CloudTrail when the provider client is configured.

### Event findings are not updating

Check:

1. `just worker evaluator` is running.
   Direct command: `open-sspm worker --lane evaluator`.
2. Canonical events are being written for the source.
3. Event evaluator queue rows (`event_evaluation_queue`) are not stuck in `processing`; stale rows are requeued by the worker.

### Discovery data is missing

Check:

1. `SYNC_DISCOVERY_ENABLED=1`
2. `just worker discovery` is running
   Direct command: `open-sspm worker --lane discovery`.
3. Discovery is enabled on the relevant IdP connector

### Event inbox is not processing

Check:

1. `EVENT_INBOX_ENABLED=1`.
2. `just worker event-inbox` is running.
   Direct command: `open-sspm worker --lane event-inbox`.
3. The Okta Event Hook or EventBridge endpoint reaches the API process.
4. The connector event inbox mode and shared secret match the delivery channel.

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

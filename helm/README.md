# Helm charts

This folder contains Helm charts for deploying Open-SSPM to Kubernetes.

## Chart: `open-sspm`

Location: `helm/open-sspm`

Deploys:
- `open-sspm serve` (HTTP/UI) as a Deployment + Service (+ optional Ingress)
- `open-sspm worker` (background full sync loop) as a Deployment
- `open-sspm worker-discovery` (background SaaS discovery sync loop) as a Deployment (enabled by default)
- Helm hook Jobs:
  - `open-sspm migrate` as a pre-install/pre-upgrade Job
  - `open-sspm seed-rules` as a pre-install Job (optionally also pre-upgrade)
  - `open-sspm users bootstrap-admin` as a pre-install Job (optionally also pre-upgrade; disabled by default)
  - Hook Jobs disable Istio sidecar injection to avoid hangs in Istio-injection-enabled namespaces.

This chart assumes a **managed Postgres** (RDS, Cloud SQL, etc). It does **not** deploy Postgres.

### Prereqs

- Helm v3
- A reachable Postgres database
- A container image for Open-SSPM in a registry (GHCR recommended)

### Image

Build and push the image using the repo `Dockerfile`, then configure:
- `image.repository`
- `image.tag`

If your GHCR repo is private, set `imagePullSecrets`.

### Database secret (`DATABASE_URL`)

The chart requires `DATABASE_URL` via an existing Kubernetes Secret:
- `database.existingSecret.name`
- `database.existingSecret.key` (default: `DATABASE_URL`)

Example:
```bash
kubectl create secret generic open-sspm-db \
  --from-literal=DATABASE_URL='postgres://USER:PASSWORD@HOST:5432/opensspm?sslmode=require'
```

### Connector secret key (`CONNECTOR_SECRET_KEY`)

If you plan to configure connector credentials, provide a stable base64-encoded 32-byte key via an existing Kubernetes Secret:
- `connectorSecret.existingSecret.name`
- `connectorSecret.existingSecret.key` (default: `CONNECTOR_SECRET_KEY`)

Example:
```bash
kubectl create secret generic open-sspm-app \
  --from-literal=CONNECTOR_SECRET_KEY="$(openssl rand -base64 32)"
```

### SMTP relay credentials (`SMTP_USERNAME` / `SMTP_PASSWORD`)

If you want Open-SSPM to send email through an SMTP relay, configure the `smtp.*` values and optionally provide SMTP credentials via an existing Secret.

Example:
```bash
kubectl create secret generic open-sspm-smtp \
  --from-literal=SMTP_USERNAME='mailer' \
  --from-literal=SMTP_PASSWORD='change-me'
```

### Install / upgrade

Minimal install:
```bash
helm upgrade --install open-sspm ./helm/open-sspm \
  --set image.repository=ghcr.io/<org>/<repo> \
  --set image.tag=<tag> \
  --set database.existingSecret.name=open-sspm-db \
  --set connectorSecret.existingSecret.name=open-sspm-app
```

Example with SMTP enabled:
```bash
helm upgrade --install open-sspm ./helm/open-sspm \
  --set image.repository=ghcr.io/<org>/<repo> \
  --set image.tag=<tag> \
  --set database.existingSecret.name=open-sspm-db \
  --set connectorSecret.existingSecret.name=open-sspm-app \
  --set smtp.enabled=true \
  --set smtp.host=smtp.example.com \
  --set smtp.port=587 \
  --set smtp.tlsMode=starttls \
  --set smtp.fromAddress=noreply@example.com \
  --set smtp.fromName="Open SSPM" \
  --set smtp.existingSecret.name=open-sspm-smtp
```

### Migrations (pre-install/pre-upgrade Job)

Migrations are bundled in the Open-SSPM container image under `db/migrations/` and executed by a Helm hook Job.

Notes:
- Hooks run `pre-install,pre-upgrade` (migrations) so the schema is updated before the app rolls.
- Build the image from this repo’s `Dockerfile` (it copies `db/migrations/` into the image).
- On a **first install**, Helm `pre-install` hooks run before non-hook resources (including this chart’s ServiceAccount) are created. If you need the migrate/seed hook Jobs to run under a specific ServiceAccount (IRSA, locked-down clusters), pre-create it and set `serviceAccount.create=false` + `serviceAccount.name=<existing-sa>`.

### Seed rules (benchmark rulesets)

The chart can run `open-sspm seed-rules` as a Helm hook Job.

Default behavior:
- Runs on **install only** (`pre-install`)
- Does **not** run on upgrade unless you opt in

Enable on-upgrade seeding:
```bash
helm upgrade --install open-sspm ./helm/open-sspm \
  --set seedRules.onUpgrade=true \
  --set database.existingSecret.name=open-sspm-db
```

Seeding behavior:
- Upserts rulesets/rules (updates definitions if they changed)
- Does **not** modify user attestations/overrides/results
- May mark rules as inactive if they disappear from the embedded descriptor (they remain in the DB, but won’t appear in “active rules” lists)

### Ingress (controller-agnostic)

The chart templates a standard `networking.k8s.io/v1` Ingress without assuming a specific controller.
Configure `ingress.className` and `ingress.annotations` for your controller (nginx/traefik/ALB/etc).

Example (values file):
```yaml
ingress:
  enabled: true
  className: nginx
  hosts:
    - host: open-sspm.example.com
      paths:
        - path: /
          pathType: Prefix
  tls:
    - secretName: open-sspm-tls
      hosts:
        - open-sspm.example.com
```

### AWS Identity Center (optional)

AWS Identity Center uses the AWS SDK default credentials chain. On Kubernetes, the typical approach is a dedicated ServiceAccount with cloud IAM binding (e.g., IRSA on EKS).

Example (IRSA on EKS):
```yaml
serviceAccount:
  create: true
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::<account-id>:role/<role-name>
```

### Port-forward (no ingress)

By default the Service listens on port `80` and targets container port `8080`.

```bash
kubectl get svc
kubectl port-forward svc/<service-name> 8080:80
```

### Worker lanes

- Full sync worker interval is configured with `config.syncInterval`.
- Discovery sync worker interval is configured with `config.syncDiscoveryInterval`.
- Set `config.syncDiscoveryEnabled=false` to disable the discovery lane system-wide.
- Set `discoveryWorker.enabled=false` to omit only the discovery worker Deployment. The chart also disables discovery queuing on `serve` when this is false so manual resyncs do not strand discovery jobs.

### Structured logging

- `config.logFormat` controls `LOG_FORMAT` (default: `json`; allowed: `json`, `text`)
- `config.logLevel` controls `LOG_LEVEL` (default: `info`; allowed: `debug`, `info`, `warn`, `error`)
- Invalid values fail fast during command startup.

### SMTP

- `smtp.enabled` controls `SMTP_ENABLED`
- `smtp.host`, `smtp.port`, `smtp.tlsMode`, `smtp.fromAddress`, and `smtp.fromName` configure the relay
- `smtp.existingSecret.name` injects optional `SMTP_USERNAME` / `SMTP_PASSWORD` credentials from a Secret
- The chart passes SMTP env vars to the Deployments and hook Jobs so `migrate`, `seed-rules`, and `bootstrap-admin` all see the same config contract

### Metrics service component selector

If `metrics.service.enabled=true`, choose which pod to target with `metrics.service.component`:
- `serve`
- `worker`
- `worker-discovery`

### UI authentication

Open-SSPM includes in-app authentication (email/password) using server-side sessions stored in Postgres.

On a fresh install there are **no users**. You must create the first admin user.

Recommended (Helm hook Job):

1) Create a Secret with the initial admin credentials:
```bash
kubectl create secret generic open-sspm-admin \
  --from-literal=ADMIN_EMAIL='admin@example.com' \
  --from-literal=ADMIN_PASSWORD='change-me'
```

2) Enable the bootstrap hook:
```bash
helm upgrade --install open-sspm ./helm/open-sspm \
  --set bootstrapAdmin.enabled=true \
  --set bootstrapAdmin.existingSecret.name=open-sspm-admin \
  --set database.existingSecret.name=open-sspm-db
```

Notes:
- The bootstrap runs `open-sspm users bootstrap-admin` and is **idempotent** (it exits successfully if an admin already exists).
- Avoid putting passwords directly in Helm values (`--set`), since Helm stores release values.
- Losing `CONNECTOR_SECRET_KEY` means stored connector secrets can no longer be decrypted and must be re-entered.

### Cookie security (`AUTH_COOKIE_SECURE`)

Set `config.authCookieSecure=true` when users access the UI over HTTPS (Ingress TLS termination, etc.).
Keep it `false` for plain HTTP / port-forward, otherwise the browser will not store session/CSRF cookies.

### Trusted proxy CIDRs (`TRUSTED_PROXY_CIDRS`)

`/login` rate limiting uses the client IP derived from `X-Forwarded-For`.
By default, Open SSPM trusts private, link-local, and loopback upstream hops, which matches typical in-cluster ingress setups.
If your direct upstream load balancer uses public IPs, pass `TRUSTED_PROXY_CIDRS` via `serve.extraEnv` so the app can trust those ingress CIDRs too.

### Dev-only seeding (`DEV_SEED_ADMIN`)

For development only, you can set `config.devSeedAdmin=true` to seed `admin@admin.com` / `admin` **only if** no UI users exist yet.

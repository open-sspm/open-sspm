# Kubernetes Installation (Helm)

This guide covers deploying Open-SSPM to Kubernetes with the checked-in Helm chart at `./helm/open-sspm`.

## Prerequisites

- Kubernetes 1.24+
- Helm 3.12+
- A PostgreSQL database
- `kubectl` configured for your cluster
- An Open-SSPM container image, such as `ghcr.io/open-sspm/open-sspm`

::: tip
The chart does not deploy PostgreSQL. Use a managed or separately operated Postgres instance.
:::

## What the Helm Chart Deploys

The chart creates:

- **API Deployment** - `open-sspm api`
- **Worker Deployment** - `open-sspm worker`
- **Discovery Worker Deployment** - `open-sspm worker-discovery` (enabled by default)
- **Ingest Worker Deployment** - `open-sspm worker-ingest` (enabled by default when discovery is enabled)
- **Tail Worker Deployment** - `open-sspm worker-tail` (enabled by default)
- **Riskpolicy Worker Deployment** - `open-sspm worker-riskpolicy` (enabled by default for shadow event evaluation)
- **Service** - Network access for the web UI
- **Ingress** (optional)
- **Hook Jobs** - Migrations, optional rule seeding, and optional admin bootstrap

## Quick Start

### 1. Create Required Secrets

```bash
kubectl create secret generic open-sspm-db \
  --from-literal=DATABASE_URL='postgres://USER:PASSWORD@HOST:5432/opensspm?sslmode=require'

kubectl create secret generic open-sspm-app \
  --from-literal=CONNECTOR_SECRET_KEY="$(openssl rand -base64 32)"
```

### 2. Install Open-SSPM

```bash
helm upgrade --install open-sspm ./helm/open-sspm \
  --set image.repository=ghcr.io/open-sspm/open-sspm \
  --set image.tag=latest \
  --set database.existingSecret.name=open-sspm-db \
  --set connectorSecret.existingSecret.name=open-sspm-app
```

### 3. Bootstrap the First Admin User

Create the credentials secret:

```bash
kubectl create secret generic open-sspm-admin \
  --from-literal=ADMIN_EMAIL='admin@example.com' \
  --from-literal=ADMIN_PASSWORD='change-me-now'
```

Then enable the bootstrap hook:

```bash
helm upgrade --install open-sspm ./helm/open-sspm \
  --set image.repository=ghcr.io/open-sspm/open-sspm \
  --set image.tag=latest \
  --set bootstrapAdmin.enabled=true \
  --set bootstrapAdmin.existingSecret.name=open-sspm-admin \
  --set database.existingSecret.name=open-sspm-db \
  --set connectorSecret.existingSecret.name=open-sspm-app
```

`bootstrap-admin` is idempotent and exits successfully if an admin already exists.

## Configuration

### Example Values File

```yaml
image:
  repository: ghcr.io/open-sspm/open-sspm
  tag: latest
  pullPolicy: IfNotPresent

database:
  existingSecret:
    name: open-sspm-db
    key: DATABASE_URL

connectorSecret:
  existingSecret:
    name: open-sspm-app
    key: CONNECTOR_SECRET_KEY

smtp:
  enabled: true
  host: smtp.example.com
  port: 587
  tlsMode: starttls
  fromAddress: noreply@example.com
  fromName: Open SSPM
  existingSecret:
    name: open-sspm-smtp

config:
  syncInterval: 15m
  syncDiscoveryInterval: 15m
  syncTailInterval: 5m
  syncDiscoveryEnabled: true
  eventRetentionDays: 90
  eventPartitionFutureDays: 7
  logFormat: json
  logLevel: info
  authCookieSecure: true

api:
  replicaCount: 1
  resources:
    limits:
      cpu: 500m
      memory: 512Mi
    requests:
      cpu: 100m
      memory: 128Mi

worker:
  replicaCount: 1
  resources:
    limits:
      cpu: 500m
      memory: 512Mi

discoveryWorker:
  enabled: true
  replicaCount: 1
  resources:
    limits:
      cpu: 500m
      memory: 512Mi

ingestWorker:
  enabled: true
  replicaCount: 1
  resources:
    limits:
      cpu: 250m
      memory: 256Mi

tailWorker:
  enabled: true
  replicaCount: 1
  resources:
    limits:
      cpu: 250m
      memory: 256Mi

riskpolicyWorker:
  enabled: true
  replicaCount: 1
  resources:
    limits:
      cpu: 250m
      memory: 256Mi

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

Install with a values file:

```bash
helm upgrade --install open-sspm ./helm/open-sspm -f values.yaml
```

### SMTP Relay

If you want Open-SSPM to send email through an SMTP relay, create a Secret with the relay credentials and reference it from `smtp.existingSecret.name`.

```bash
kubectl create secret generic open-sspm-smtp \
  --from-literal=SMTP_USERNAME='mailer' \
  --from-literal=SMTP_PASSWORD='change-me'
```

Then configure the chart:

```yaml
smtp:
  enabled: true
  host: smtp.example.com
  port: 587
  tlsMode: starttls
  fromAddress: noreply@example.com
  fromName: Open SSPM
  existingSecret:
    name: open-sspm-smtp
```

SMTP credentials are optional; if your relay does not require authentication, leave `smtp.existingSecret.name` empty.

### Ingress

The chart uses a standard Kubernetes Ingress resource. Configure it for your controller:

```yaml
ingress:
  enabled: true
  className: nginx
  annotations:
    cert-manager.io/cluster-issuer: letsencrypt-prod
    nginx.ingress.kubernetes.io/ssl-redirect: "true"
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

### AWS Identity Center on EKS

For the AWS Identity Center connector, the recommended cluster setup is runtime credentials via IRSA:

```yaml
serviceAccount:
  create: true
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::ACCOUNT:role/open-sspm-role
```

### Trusted Proxy CIDRs

If your direct upstream proxy uses public IPs, pass `TRUSTED_PROXY_CIDRS` via `api.extraEnv`:

```yaml
api:
  extraEnv:
    - name: TRUSTED_PROXY_CIDRS
      value: "35.191.0.0/16,130.211.0.0/22"
```

## Helm Hooks

The chart includes hook Jobs for:

- **Migrations** - `open-sspm migrate` on install and upgrade
- **Rule seeding** - `open-sspm seed-rules` on install by default
- **Admin bootstrap** - `open-sspm users bootstrap-admin` when enabled

Enable rule seeding on upgrades:

```yaml
seedRules:
  onUpgrade: true
```

Enable admin bootstrap on upgrades:

```yaml
bootstrapAdmin:
  enabled: true
  onUpgrade: true
  existingSecret:
    name: open-sspm-admin
```

## Upgrading

```bash
helm upgrade open-sspm ./helm/open-sspm -f values.yaml
kubectl rollout status deployment/open-sspm-api
```

## Troubleshooting

### Check Pod Status

```bash
kubectl get pods -l app.kubernetes.io/name=open-sspm
```

### View Logs

```bash
kubectl logs -l app.kubernetes.io/component=api
kubectl logs -l app.kubernetes.io/component=worker
kubectl logs -l app.kubernetes.io/component=worker-discovery
kubectl logs -l app.kubernetes.io/component=worker-ingest
kubectl logs -l app.kubernetes.io/component=worker-tail
kubectl logs -l app.kubernetes.io/component=worker-riskpolicy
```

### Database Connection Issues

Verify the secret contents:

```bash
kubectl get secret open-sspm-db -o jsonpath='{.data.DATABASE_URL}' | base64 -d
```

### Hook Failures

```bash
kubectl get jobs
kubectl describe job open-sspm-migrate
```

## Uninstalling

```bash
helm uninstall open-sspm
```

::: warning
Uninstalling the release does not delete the database or Kubernetes secrets.
:::

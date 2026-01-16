# Helm charts

This folder contains Helm charts for deploying Open-SSPM to Kubernetes.

## Chart: `open-sspm`

Location: `helm/open-sspm`

Deploys:
- `open-sspm serve` (HTTP/UI) as a Deployment + Service (+ optional Ingress)
- `open-sspm worker` (background sync loop) as a Deployment
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

### Install / upgrade

Minimal install:
```bash
helm upgrade --install open-sspm ./helm/open-sspm \
  --set image.repository=ghcr.io/<org>/<repo> \
  --set image.tag=<tag> \
  --set database.existingSecret.name=open-sspm-db
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

### Cookie security (`AUTH_COOKIE_SECURE`)

Set `config.authCookieSecure=true` when users access the UI over HTTPS (Ingress TLS termination, etc.).
Keep it `false` for plain HTTP / port-forward, otherwise the browser will not store session/CSRF cookies.

### Dev-only seeding (`DEV_SEED_ADMIN`)

For development only, you can set `config.devSeedAdmin=true` to seed `admin@admin.com` / `admin` **only if** no UI users exist yet.

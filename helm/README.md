# Helm charts

This folder contains Helm charts for deploying Open-SSPM to Kubernetes.

## Chart: `open-sspm`

Location: `helm/open-sspm`

Deploys:
- `open-sspm serve` (HTTP/UI) as a Deployment + Service (+ optional Ingress)
- `open-sspm worker` (background sync loop) as a Deployment
- DB bootstrap hooks:
  - `open-sspm migrate` as a pre-install/pre-upgrade Job
  - `open-sspm seed-rules` as a pre-install Job (optionally also pre-upgrade)

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

### Security note

Open-SSPM currently has **no in-app authentication**; deploy behind your auth proxy / private ingress.

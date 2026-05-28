# Demo data seeding

This folder contains **demo-only** data seeds for Open-SSPM.

Goals:
- **Upsert-only** (safe to re-run; no deletes/truncates).
- Populate enough data for a credible demo of:
  - Okta users / groups / app assignments
  - GitHub users + entitlements
  - Datadog users + roles
  - SaaS discovery list / hotspots / detail pages
  - Google Workspace users, groups, users needing anchors, and OAuth clients
  - AWS Identity Center users and users needing anchors
  - Vault principals, entitlements, mounts, and auth roles
  - Programmatic access governance (app assets, owners, credentials, audit events)
  - Some Findings (pass/fail, with `schema_version=1` evidence envelope)

Discovery posture is computed live from:
- `saas_apps`
- `saas_app_sources`
- `saas_app_events`
- `saas_app_bindings`
- `governance_subject_overrides`

Do not seed or expect persisted discovery posture columns on `saas_apps`; managed state, risk, and governance rollups come from the posture view/functions at read time.

## Apply (locally)

After running migrations and `open-sspm admin seed-rules`, apply:

```bash
while IFS= read -r seed_file; do
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$seed_file"
done < <(find demo/data -maxdepth 1 -type f -name '*.sql' | sort)
```

Seed files are applied in lexical order (for example: `001_...`, then `002_...`).

## Apply (demo VM via Ansible)

From `demo/infra/ansible`:

```bash
ansible-playbook -i inventory.ini seed-demo.yml
```

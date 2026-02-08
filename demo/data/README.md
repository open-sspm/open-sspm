# Demo data seeding

This folder contains **demo-only** data seeds for Open-SSPM.

Goals:
- **Upsert-only** (safe to re-run; no deletes/truncates).
- Populate enough data for a credible demo of:
  - Okta users / groups / app assignments
  - GitHub users + entitlements
  - Datadog users + roles
  - Programmatic access governance (app assets, owners, credentials, audit events)
  - Some Findings (pass/fail, with `schema_version=1` evidence envelope)

## Apply (locally)

After running migrations and `open-sspm seed-rules`, apply:

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

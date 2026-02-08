# Ansible (demo VM bootstrap)

Bootstraps the Scaleway VM for the demo:
- installs Postgres locally
- prepares an `open-sspm` system user and service scaffolding

This is intentionally minimal; the application binary/container is expected to be deployed by GitHub Actions.

## Usage

1) Copy the inventory example:

```bash
cp inventory.ini.example inventory.ini
```

2) Run:

```bash
ansible-playbook -i inventory.ini site.yml
```

## Add SSH deploy keys

To authorize multiple SSH public keys for the `deploy` user (idempotent):

```bash
ansible-playbook -i inventory.ini add-keys.yml \
  -e '{"deploy_public_key_files":["/Users/sardo/.ssh/id_deploy.pub","/Users/sardo/.ssh/<other>.pub"]}'
```

## Seed demo data

The GitHub Actions demo deploy workflow already resets the demo database and applies all demo seed SQL files from `demo/data/` in lexical order.

If you want to re-seed without redeploying, after the app has been deployed (migrations + `open-sspm seed-rules`), run:

```bash
ansible-playbook -i inventory.ini seed-demo.yml
```

## Deploy runtime assets (CSS + static + migrations)

Build CSS on the control machine first:

```bash
make ui
```

Then sync runtime assets:

```bash
ansible-playbook -i inventory.ini deploy.yml
```

# Demo infrastructure (Scaleway)

This folder holds the infrastructure-as-code for running a **demo** Open-SSPM instance on **Scaleway**:

- Zone: `fr-par-2` (Paris 2)
- Instance type: `DEV1-S`
- Image: `ubuntu_noble` (Ubuntu 24.04 “Noble Numbat”)

Goal:
- Single VM with **Postgres installed locally**.
- `open-sspm` runs on the VM and is **deployed from GitHub Actions** (manual workflow dispatch).
- Public HTTP is served via **nginx reverse proxy** (Cloudflare in front).

## Layout

- `terraform/`: provisions the Scaleway instance (IP, security group, server).
- `ansible/`: configures the instance (Postgres, system user, reverse proxy placeholders).

## Quickstart (local)

1) Provision the VM

```bash
cd demo/infra/terraform
cp terraform.tfvars.example terraform.tfvars
tofu init
tofu apply
```

2) Configure the VM

```bash
cd ../ansible
cp inventory.ini.example inventory.ini
# fill the server IP from terraform output
ansible-playbook -i inventory.ini site.yml
```

3) Seed demo data (after deploy)

```bash
ansible-playbook -i inventory.ini seed-demo.yml
```

## Notes

- Secrets (Scaleway API keys, SSH private keys, DB passwords, Basic Auth credentials) must **not** be committed.
- Use GitHub Actions secrets for CI deployment, and Ansible Vault (or equivalent) for server-side secrets.
- The demo is intended to be behind Cloudflare; nginx listens on `:80` and proxies to `open-sspm` on `127.0.0.1:8080`.

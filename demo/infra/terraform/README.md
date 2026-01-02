# OpenTofu / Terraform (Scaleway)

Creates:
- 1 public IP
- 1 security group (SSH + HTTP/HTTPS)
- 1 instance server (Ubuntu 24.04 in `fr-par-2`)

## Prereqs

- OpenTofu >= 1.6 (or Terraform >= 1.6)
- Scaleway credentials in env vars:
  - `SCW_ACCESS_KEY`
  - `SCW_SECRET_KEY`
  - `SCW_DEFAULT_PROJECT_ID` (or set `project_id` in `terraform.tfvars`)

## Usage

```bash
cp terraform.tfvars.example terraform.tfvars
tofu init
tofu apply
```

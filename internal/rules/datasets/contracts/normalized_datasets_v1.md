# Normalized datasets (contract v1)

This document describes the current contract v1 for `normalized:*` datasets emitted by the rules engine dataset provider.

All datasets are returned as an array of JSON objects (`[]any`) and are intended to be accessed via JSON Pointer paths.

## `normalized:identities` (v1)

Row fields (selected):

- `/id` (string) — internal `idp_users.id` as a string
- `/external_id` (string) — `idp_users.external_id`
- `/email` (string)
- `/display_name` (string)
- `/status` (string) — one of `active|inactive|deprovisioned`

## `normalized:entitlement_assignments` (v1)

Row fields (selected):

- `/resource_id` (string) — `entitlement:<entitlements.id>`
- `/identity/id` (string) — internal `idp_users.id` as a string
- `/identity/status` (string)
- `/entitlement/kind` (string)
- `/entitlement/resource` (string)
- `/entitlement/permission` (string)
- `/entitlement/tags` (array of strings) — includes `admin` when permission suggests admin-like access


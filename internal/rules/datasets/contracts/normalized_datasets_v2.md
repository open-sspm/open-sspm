# Normalized datasets (contract v2)

This document describes the contract v2 for `normalized:*` datasets emitted by the rules engine dataset provider.

All datasets are returned as an array of JSON objects (`[]any`) and are intended to be accessed via JSON Pointer paths.

## `normalized:identities` (v2)

Row fields (selected):

- `/id` (string) — internal `identities.id`
- `/kind` (string) — one of `human|service|bot|unknown`
- `/email` (string) — `identities.primary_email`
- `/display_name` (string) — `identities.display_name`
- `/managed` (boolean) — `true` when identity is linked to any account from a source marked authoritative in `identity_source_settings`
- `/authoritative_account/source_kind` (string)
- `/authoritative_account/source_name` (string)
- `/authoritative_account/external_id` (string)

`managed=false` identities are intentionally included to represent unmanaged/Shadow IT accounts.

## `normalized:entitlement_assignments` (v2)

Row fields (selected):

- `/resource_id` (string) — `entitlement:<entitlements.id>`
- `/identity/id` (string) — internal `identities.id`
- `/identity/kind` (string)
- `/identity/email` (string)
- `/identity/display_name` (string)
- `/identity/managed` (boolean)
- `/app_user/source_kind` (string)
- `/app_user/source_name` (string)
- `/app_user/external_id` (string)
- `/entitlement/kind` (string)
- `/entitlement/resource` (string)
- `/entitlement/permission` (string)
- `/entitlement/tags` (array of strings) — includes `admin` when permission suggests admin-like access

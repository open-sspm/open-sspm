# Normalized Datasets Contract

This document describes the versioned `normalized:*` datasets emitted by the
rules engine dataset provider. Datasets are returned as arrays of JSON objects
and are intended to be accessed through JSON Pointer paths.

## Versions

- `dataset_version: 1` is the legacy identity/account status contract.
- `dataset_version: 2` is the managed identity contract with
  `authoritative_account`.
- `dataset_version: 3` adds posture and anchor state. New rules should target
  this version.

## `normalized:identities`

### Version 1

- `/id` (string) - internal `identities.id`
- `/external_id` (string) - authoritative account external ID
- `/email` (string)
- `/display_name` (string)
- `/status` (string) - normalized legacy account status

### Version 2

- `/id` (string)
- `/kind` (string) - one of `human|service|bot|unknown`
- `/email` (string)
- `/display_name` (string)
- `/managed` (boolean)
- `/authoritative_account/source_kind` (string)
- `/authoritative_account/source_name` (string)
- `/authoritative_account/external_id` (string)

### Version 3

- `/id` (string)
- `/kind` (string) - one of `human|service|bot|unknown`
- `/email` (string)
- `/display_name` (string)
- `/managed` (boolean)
- `/posture` (string) - one of `managed|unmanaged|non_human`
- `/anchor/state` (string) - one of `anchored|missing_anchor|not_applicable`
- `/anchor/source_kind` (string)
- `/anchor/source_name` (string)
- `/anchor/external_id` (string)

`posture=unmanaged` identities are provisional account rollups that do not yet
have an authoritative identity anchor.

## `normalized:entitlement_assignments`

### Version 1

- `/resource_id` (string) - `entitlement:<entitlements.id>`
- `/identity/id` (string)
- `/identity/email` (string)
- `/identity/display_name` (string)
- `/identity/status` (string)
- `/account/source_kind` (string)
- `/account/source_name` (string)
- `/account/external_id` (string)
- `/entitlement/kind` (string)
- `/entitlement/resource` (string)
- `/entitlement/permission` (string)
- `/entitlement/tags` (array of strings)

### Version 2

- `/resource_id` (string)
- `/identity/id` (string)
- `/identity/kind` (string)
- `/identity/email` (string)
- `/identity/display_name` (string)
- `/identity/managed` (boolean)
- `/account/source_kind` (string)
- `/account/source_name` (string)
- `/account/external_id` (string)
- `/entitlement/kind` (string)
- `/entitlement/resource` (string)
- `/entitlement/permission` (string)
- `/entitlement/tags` (array of strings)

### Version 3

- `/resource_id` (string)
- `/identity/id` (string)
- `/identity/kind` (string)
- `/identity/email` (string)
- `/identity/display_name` (string)
- `/identity/managed` (boolean)
- `/identity/posture` (string)
- `/identity/anchor/state` (string)
- `/identity/anchor/source_kind` (string)
- `/identity/anchor/source_name` (string)
- `/identity/anchor/external_id` (string)
- `/account/source_kind` (string)
- `/account/source_name` (string)
- `/account/external_id` (string)
- `/entitlement/kind` (string)
- `/entitlement/resource` (string)
- `/entitlement/permission` (string)
- `/entitlement/tags` (array of strings) - includes `admin` when permission suggests admin-like access

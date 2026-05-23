# Normalized Datasets Contract

This document describes the current `normalized:*` datasets emitted by the
rules engine dataset provider. Datasets are returned as arrays of JSON objects
and are intended to be accessed through JSON Pointer paths.

## `normalized:identities`

Row fields:

- `/id` (string) - internal `identities.id`
- `/kind` (string) - one of `human|service|bot|unknown`
- `/email` (string) - `identities.primary_email`
- `/display_name` (string) - `identities.display_name`
- `/managed` (boolean) - `true` when the identity has an active authoritative account
- `/posture` (string) - one of `managed|unmanaged|non_human`
- `/anchor/state` (string) - one of `anchored|missing_anchor|not_applicable`
- `/anchor/source_kind` (string)
- `/anchor/source_name` (string)
- `/anchor/external_id` (string)

`posture=unmanaged` identities are provisional account rollups that do not yet
have an authoritative identity anchor.

## `normalized:entitlement_assignments`

Row fields:

- `/resource_id` (string) - `entitlement:<entitlements.id>`
- `/identity/id` (string) - internal `identities.id`
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

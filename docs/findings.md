# Findings (v1)

This document describes how Open-SSPM represents and renders **Findings** (rule evaluation results) in the DB and UI.

## What is a “finding”?

A finding is the **current evaluation outcome** for a single rule in a specific evaluation context.

That context is defined by:

- `scope_kind`: `connector_instance` or `global`
- `source_kind`: connector kind (example: `okta`) or empty for global
- `source_name`: connector instance name (example: `prod`) or empty for global

The “current” finding is stored in `rule_results_current` (one row per `(rule_id, scope_kind, source_kind, source_name)`).
Historical evaluations are stored in `rule_evaluations`.

## UI routes (HTML)

Findings are server-rendered (Echo + templ) from DB rows:

- `GET /findings` — list active rulesets
- `GET /findings/rulesets/:rulesetKey` — list active rules in a ruleset (with current status for the selected scope)
- `GET /findings/rulesets/:rulesetKey/rules/:ruleKey` — rule detail (metadata + current evidence + override/attestation controls)

## Status and error_kind

### Status

`status` is one of:

- `pass`
- `fail`
- `unknown`
- `not_applicable`
- `error`

### error_kind

When present, `error_kind` is a medium-strict enum:

- `missing_integration`
- `missing_dataset`
- `permission_denied`
- `sync_failed`
- `invalid_params`
- `join_unmatched`
- `engine_error`

`error_kind` is stored in `rule_results_current.error_kind` and also (when applicable) inside the JSON evidence envelope as `result.error_kind`.

## Evidence JSON envelope (schema_version=1)

`rule_results_current.evidence_json` and `rule_evaluations.evidence_json` use a stable JSON envelope so the UI can render findings without bespoke per-rule code.

### Required top-level fields

- `schema_version` (int): must be `1`
- `rule` (object): `{ ruleset_key, rule_key }`
- `check` (object): `{ type, dataset?, dataset_version?, left?, right? }`
- `params` (object): the effective params used for evaluation
- `result` (object): `{ status, error_kind? }`

### Common optional fields

- `selection` (object): `{ total, selected }`
- `violations` (list): items like `{ resource_id, display? }`
- `violations_truncated` (bool)
- `compare` (object): `{ op, value }` (count checks)
- `join` (object): `{ on_unmatched_left, unmatched_left }` (join checks)
- `override` (object): present when disabled by override (at minimum `{ enabled: false }`)
- `attestation` (object): present when an active attestation is used

### Size limits

For `dataset.field_compare`, `violations` is capped to 100 items and `violations_truncated=true` is set when truncation occurs.

## Examples

### dataset.field_compare (fail with violations)

```json
{
  "schema_version": 1,
  "rule": { "ruleset_key": "cis.okta.idaas_stig.v1", "rule_key": "some.rule" },
  "check": { "type": "dataset.field_compare", "dataset": "okta:policies/sign-on", "dataset_version": 1 },
  "params": { "max_idle_minutes": 15 },
  "result": { "status": "fail" },
  "selection": { "total": 12, "selected": 1 },
  "violations": [
    { "resource_id": "okta_policy_rule:00abc...", "display": "Default rule" }
  ],
  "violations_truncated": false
}
```

### Dataset error (permission denied → unknown)

```json
{
  "schema_version": 1,
  "rule": { "ruleset_key": "cis.okta.idaas_stig.v1", "rule_key": "some.rule" },
  "check": { "type": "dataset.field_compare", "dataset": "okta:policies/sign-on" },
  "params": {},
  "result": { "status": "unknown", "error_kind": "permission_denied" }
}
```

### Disabled by override (not_applicable)

```json
{
  "schema_version": 1,
  "rule": { "ruleset_key": "cis.okta.idaas_stig.v1", "rule_key": "some.rule" },
  "check": { "type": "dataset.field_compare" },
  "result": { "status": "not_applicable" },
  "override": { "enabled": false }
}
```

### Active attestation (suppresses automation)

```json
{
  "schema_version": 1,
  "rule": { "ruleset_key": "cis.okta.idaas_stig.v1", "rule_key": "some.rule" },
  "check": { "type": "manual.attestation" },
  "result": { "status": "pass" },
  "attestation": {
    "status": "pass",
    "notes": "Reviewed in ticket SEC-1234",
    "expires_at": "2026-01-31T00:00:00Z"
  }
}
```

## Affected resources

When available, evaluations also populate `rule_results_current.affected_resource_ids` (TEXT[]):

- For `dataset.field_compare`, this is derived from the failing `violations[*].resource_id`.
- For other check types and most error states, it is currently empty.

The UI uses this data for display and future filtering; it is not required to render the finding.

## Backwards/unknown evidence handling

If `evidence_json.schema_version != 1`, the UI falls back to showing `evidence_json` as pretty-printed JSON without envelope-specific rendering.

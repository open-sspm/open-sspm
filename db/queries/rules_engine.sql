-- name: UpsertRuleset :one
INSERT INTO rulesets (
  key,
  name,
  description,
  source,
  source_version,
  source_date,
  scope_kind,
  connector_kind,
  status,
  definition_hash,
  definition_json
)
VALUES (
  $1,
  $2,
  $3,
  $4,
  $5,
  $6,
  $7,
  $8,
  $9,
  $10,
  $11
)
ON CONFLICT (key) DO UPDATE SET
  name = EXCLUDED.name,
  description = EXCLUDED.description,
  source = EXCLUDED.source,
  source_version = EXCLUDED.source_version,
  source_date = EXCLUDED.source_date,
  scope_kind = EXCLUDED.scope_kind,
  connector_kind = EXCLUDED.connector_kind,
  status = EXCLUDED.status,
  definition_hash = EXCLUDED.definition_hash,
  definition_json = EXCLUDED.definition_json,
  updated_at = now()
RETURNING *;

-- name: ListRulesets :many
SELECT *
FROM rulesets
ORDER BY key;

-- name: GetRulesetByKey :one
SELECT *
FROM rulesets
WHERE key = $1;

-- name: UpsertRule :one
INSERT INTO rules (
  ruleset_id,
  key,
  title,
  summary,
  category,
  severity,
  monitoring_status,
  monitoring_reason,
  required_data,
  expected_params,
  rule_version,
  is_active,
  definition_json
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
ON CONFLICT (ruleset_id, key) DO UPDATE SET
  title = EXCLUDED.title,
  summary = EXCLUDED.summary,
  category = EXCLUDED.category,
  severity = EXCLUDED.severity,
  monitoring_status = EXCLUDED.monitoring_status,
  monitoring_reason = EXCLUDED.monitoring_reason,
  required_data = EXCLUDED.required_data,
  expected_params = EXCLUDED.expected_params,
  rule_version = EXCLUDED.rule_version,
  is_active = EXCLUDED.is_active,
  definition_json = EXCLUDED.definition_json,
  updated_at = now()
RETURNING *;

-- name: DeactivateRulesNotInKeys :exec
UPDATE rules
SET is_active = false, updated_at = now()
WHERE ruleset_id = sqlc.arg(ruleset_id)
  AND NOT (key = ANY(sqlc.arg(rule_keys)::text[]));

-- name: ListActiveRulesByRulesetID :many
SELECT *
FROM rules
WHERE ruleset_id = $1 AND is_active = true
ORDER BY key;

-- name: ListActiveRulesWithCurrentResultsByRulesetKey :many
SELECT
  r.id,
  r.ruleset_id,
  r.key,
  r.title,
  r.summary,
  r.category,
  r.severity,
  r.monitoring_status,
  r.monitoring_reason,
  r.required_data,
  r.expected_params,
  r.rule_version,
  r.is_active,
  r.definition_json,
  r.created_at,
  r.updated_at,
  COALESCE(rrc.status, 'unknown') AS current_status,
  rrc.evaluated_at AS current_evaluated_at,
  rrc.sync_run_id AS current_sync_run_id,
  COALESCE(rrc.evidence_summary, '') AS current_evidence_summary,
  COALESCE(rrc.error_kind, '') AS current_error_kind
FROM rules r
JOIN rulesets rs ON rs.id = r.ruleset_id
LEFT JOIN rule_results_current rrc
  ON rrc.rule_id = r.id
  AND rrc.scope_kind = $2
  AND rrc.source_kind = $3
  AND rrc.source_name = $4
WHERE rs.key = $1
  AND r.is_active = true
ORDER BY r.key;

-- name: GetRuleWithCurrentResultByRulesetKeyAndRuleKey :one
SELECT
  r.*,
  COALESCE(rrc.status, 'unknown') AS current_status,
  rrc.evaluated_at AS current_evaluated_at,
  rrc.sync_run_id AS current_sync_run_id,
  COALESCE(rrc.evidence_summary, '') AS current_evidence_summary,
  COALESCE(rrc.evidence_json, '{}'::jsonb) AS current_evidence_json,
  COALESCE(rrc.error_kind, '') AS current_error_kind
FROM rules r
JOIN rulesets rs ON rs.id = r.ruleset_id
LEFT JOIN rule_results_current rrc
  ON rrc.rule_id = r.id
  AND rrc.scope_kind = $3
  AND rrc.source_kind = $4
  AND rrc.source_name = $5
WHERE rs.key = $1
  AND r.key = $2;

-- name: UpsertRuleResultCurrent :one
INSERT INTO rule_results_current (
  rule_id,
  scope_kind,
  source_kind,
  source_name,
  status,
  evaluated_at,
  sync_run_id,
  evidence_summary,
  evidence_json,
  affected_resource_ids,
  error_kind
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (rule_id, scope_kind, source_kind, source_name) DO UPDATE SET
  status = EXCLUDED.status,
  evaluated_at = EXCLUDED.evaluated_at,
  sync_run_id = EXCLUDED.sync_run_id,
  evidence_summary = EXCLUDED.evidence_summary,
  evidence_json = EXCLUDED.evidence_json,
  affected_resource_ids = EXCLUDED.affected_resource_ids,
  error_kind = EXCLUDED.error_kind,
  updated_at = now()
RETURNING id, rule_id, scope_kind, source_kind, source_name, status, evaluated_at, sync_run_id, evidence_summary, evidence_json, affected_resource_ids, error_kind, created_at, updated_at;

-- name: InsertRuleEvaluation :one
INSERT INTO rule_evaluations (
  rule_id,
  scope_kind,
  source_kind,
  source_name,
  status,
  sync_run_id,
  evaluated_at,
  evidence_summary,
  evidence_json,
  affected_resource_ids,
  error_kind
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
RETURNING id, rule_id, scope_kind, source_kind, source_name, status, sync_run_id, evaluated_at, evidence_summary, evidence_json, affected_resource_ids, error_kind;

-- name: UpsertRulesetOverride :one
INSERT INTO ruleset_overrides (ruleset_id, scope_kind, source_kind, source_name, enabled)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (ruleset_id, scope_kind, source_kind, source_name) DO UPDATE SET
  enabled = EXCLUDED.enabled,
  updated_at = now()
RETURNING id, ruleset_id, scope_kind, source_kind, source_name, enabled, created_at, updated_at;

-- name: GetRulesetOverride :one
SELECT id, ruleset_id, scope_kind, source_kind, source_name, enabled, created_at, updated_at
FROM ruleset_overrides
WHERE ruleset_id = $1
  AND scope_kind = $2
  AND source_kind = $3
  AND source_name = $4;

-- name: UpsertRuleOverride :one
INSERT INTO rule_overrides (rule_id, scope_kind, source_kind, source_name, params, enabled)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (rule_id, scope_kind, source_kind, source_name) DO UPDATE SET
  params = EXCLUDED.params,
  enabled = EXCLUDED.enabled,
  updated_at = now()
RETURNING id, rule_id, scope_kind, source_kind, source_name, params, enabled, created_at, updated_at;

-- name: GetRuleOverride :one
SELECT id, rule_id, scope_kind, source_kind, source_name, params, enabled, created_at, updated_at
FROM rule_overrides
WHERE rule_id = $1
  AND scope_kind = $2
  AND source_kind = $3
  AND source_name = $4;

-- name: UpsertRuleAttestation :one
INSERT INTO rule_attestations (rule_id, scope_kind, source_kind, source_name, status, notes, expires_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (rule_id, scope_kind, source_kind, source_name) DO UPDATE SET
  status = EXCLUDED.status,
  notes = EXCLUDED.notes,
  expires_at = EXCLUDED.expires_at,
  updated_at = now()
RETURNING id, rule_id, scope_kind, source_kind, source_name, status, notes, expires_at, created_at, updated_at;

-- name: GetRuleAttestation :one
SELECT id, rule_id, scope_kind, source_kind, source_name, status, notes, expires_at, created_at, updated_at
FROM rule_attestations
WHERE rule_id = $1
  AND scope_kind = $2
  AND source_kind = $3
  AND source_name = $4;

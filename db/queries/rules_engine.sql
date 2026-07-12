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
  updated_at = CASE
    WHEN rulesets.definition_hash IS DISTINCT FROM EXCLUDED.definition_hash
      OR rulesets.definition_json IS DISTINCT FROM EXCLUDED.definition_json
      THEN now()
    ELSE rulesets.updated_at
  END
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
  is_active,
  definition_json
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (ruleset_id, key) DO UPDATE SET
  title = EXCLUDED.title,
  summary = EXCLUDED.summary,
  category = EXCLUDED.category,
  severity = EXCLUDED.severity,
  monitoring_status = EXCLUDED.monitoring_status,
  monitoring_reason = EXCLUDED.monitoring_reason,
  is_active = EXCLUDED.is_active,
  definition_json = EXCLUDED.definition_json,
  updated_at = CASE
    WHEN rules.definition_json IS DISTINCT FROM EXCLUDED.definition_json
      OR rules.is_active IS DISTINCT FROM EXCLUDED.is_active
      THEN now()
    ELSE rules.updated_at
  END
RETURNING *;

-- name: DeactivateRulesNotInKeys :exec
UPDATE rules
SET is_active = false, updated_at = now()
WHERE ruleset_id = sqlc.arg(ruleset_id)
  AND is_active = true
  AND NOT (key = ANY(sqlc.arg(rule_keys)::text[]));

-- name: ListActiveRulesByRulesetID :many
SELECT *
FROM rules
WHERE ruleset_id = $1 AND is_active = true
ORDER BY key;

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
  AND (
    source_name = $4
    OR ($4 <> '' AND source_name = '')
  )
ORDER BY CASE WHEN source_name = $4 THEN 0 ELSE 1 END
LIMIT 1;

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
  AND (
    source_name = $4
    OR ($4 <> '' AND source_name = '')
  )
ORDER BY CASE WHEN source_name = $4 THEN 0 ELSE 1 END
LIMIT 1;

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
  AND (
    source_name = $4
    OR ($4 <> '' AND source_name = '')
  )
ORDER BY CASE WHEN source_name = $4 THEN 0 ELSE 1 END
LIMIT 1;

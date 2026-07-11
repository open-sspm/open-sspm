-- name: UpsertFinding :one
WITH previous AS (
  SELECT
    status,
    effective_severity
  FROM findings
  WHERE finding_key = sqlc.arg(finding_key)::text
),
upserted AS (
INSERT INTO findings (
  finding_key,
  status,
  base_severity,
  effective_severity,
  severity_source,
  title,
  summary,
  evidence,
  remediation,
  source_kind,
  source_name,
  scope_kind,
  scope_source_kind,
  scope_source_name,
  entity_kind,
  entity_id,
  entity_name,
  resource_kind,
  resource_id,
  resource_name,
  policy_bundle_id,
  policy_bundle_version,
  policy_id,
  policy_title,
  rule_id,
  ruleset_id,
  event_received_at,
  event_id,
  first_seen_at,
  last_seen_at,
  resolved_at,
  suppressed_until,
  suppression_reason,
  suppressed_by,
  suppressed_at,
  output
)
VALUES (
  sqlc.arg(finding_key)::text,
  sqlc.arg(status)::text,
  sqlc.arg(base_severity)::text,
  sqlc.arg(effective_severity)::text,
  sqlc.arg(severity_source)::text,
  sqlc.arg(title)::text,
  sqlc.arg(summary)::text,
  sqlc.arg(evidence)::text,
  sqlc.arg(remediation)::text,
  sqlc.arg(source_kind)::text,
  sqlc.arg(source_name)::text,
  sqlc.arg(scope_kind)::text,
  sqlc.arg(scope_source_kind)::text,
  sqlc.arg(scope_source_name)::text,
  sqlc.arg(entity_kind)::text,
  sqlc.arg(entity_id)::text,
  sqlc.arg(entity_name)::text,
  sqlc.arg(resource_kind)::text,
  sqlc.arg(resource_id)::text,
  sqlc.arg(resource_name)::text,
  sqlc.arg(policy_bundle_id)::text,
  sqlc.arg(policy_bundle_version)::text,
  sqlc.arg(policy_id)::text,
  sqlc.arg(policy_title)::text,
  sqlc.narg(rule_id)::text,
  sqlc.narg(ruleset_id)::text,
  sqlc.narg(event_received_at)::timestamptz,
  sqlc.narg(event_id)::uuid,
  COALESCE(sqlc.narg(first_seen_at)::timestamptz, now()),
  COALESCE(sqlc.narg(last_seen_at)::timestamptz, now()),
  sqlc.narg(resolved_at)::timestamptz,
  sqlc.narg(suppressed_until)::timestamptz,
  sqlc.arg(suppression_reason)::text,
  sqlc.arg(suppressed_by)::text,
  sqlc.narg(suppressed_at)::timestamptz,
  sqlc.arg(output)::jsonb
)
ON CONFLICT (finding_key) DO UPDATE SET
  status = CASE
    WHEN findings.status IN ('suppressed', 'accepted_risk')
      AND (
        findings.suppressed_until IS NULL
        OR findings.suppressed_until > now()
      )
      THEN findings.status
    ELSE EXCLUDED.status
  END,
  base_severity = EXCLUDED.base_severity,
  effective_severity = EXCLUDED.effective_severity,
  severity_source = EXCLUDED.severity_source,
  title = EXCLUDED.title,
  summary = EXCLUDED.summary,
  evidence = EXCLUDED.evidence,
  remediation = EXCLUDED.remediation,
  source_kind = EXCLUDED.source_kind,
  source_name = EXCLUDED.source_name,
  scope_kind = EXCLUDED.scope_kind,
  scope_source_kind = EXCLUDED.scope_source_kind,
  scope_source_name = EXCLUDED.scope_source_name,
  entity_kind = EXCLUDED.entity_kind,
  entity_id = EXCLUDED.entity_id,
  entity_name = EXCLUDED.entity_name,
  resource_kind = EXCLUDED.resource_kind,
  resource_id = EXCLUDED.resource_id,
  resource_name = EXCLUDED.resource_name,
  policy_bundle_id = EXCLUDED.policy_bundle_id,
  policy_bundle_version = EXCLUDED.policy_bundle_version,
  policy_id = EXCLUDED.policy_id,
  policy_title = EXCLUDED.policy_title,
  rule_id = EXCLUDED.rule_id,
  ruleset_id = EXCLUDED.ruleset_id,
  event_received_at = EXCLUDED.event_received_at,
  event_id = EXCLUDED.event_id,
  last_seen_at = EXCLUDED.last_seen_at,
  resolved_at = CASE
    WHEN findings.status IN ('suppressed', 'accepted_risk')
      AND (
        findings.suppressed_until IS NULL
        OR findings.suppressed_until > now()
      )
      THEN findings.resolved_at
    WHEN EXCLUDED.status = 'resolved' THEN COALESCE(findings.resolved_at, EXCLUDED.resolved_at, now())
    ELSE NULL
  END,
  suppressed_until = CASE
    WHEN findings.status IN ('suppressed', 'accepted_risk')
      AND (
        findings.suppressed_until IS NULL
        OR findings.suppressed_until > now()
      )
      THEN findings.suppressed_until
    ELSE EXCLUDED.suppressed_until
  END,
  suppression_reason = CASE
    WHEN findings.status IN ('suppressed', 'accepted_risk')
      AND (
        findings.suppressed_until IS NULL
        OR findings.suppressed_until > now()
      )
      THEN findings.suppression_reason
    ELSE EXCLUDED.suppression_reason
  END,
  suppressed_by = CASE
    WHEN findings.status IN ('suppressed', 'accepted_risk')
      AND (
        findings.suppressed_until IS NULL
        OR findings.suppressed_until > now()
      )
      THEN findings.suppressed_by
    ELSE EXCLUDED.suppressed_by
  END,
  suppressed_at = CASE
    WHEN findings.status IN ('suppressed', 'accepted_risk')
      AND (
        findings.suppressed_until IS NULL
        OR findings.suppressed_until > now()
      )
      THEN findings.suppressed_at
    ELSE EXCLUDED.suppressed_at
  END,
  output = EXCLUDED.output,
  updated_at = now()
RETURNING *
)
SELECT
  upserted.*,
  COALESCE((SELECT status FROM previous), '')::text AS previous_status,
  COALESCE((SELECT effective_severity FROM previous), '')::text AS previous_effective_severity
FROM upserted;

-- name: InsertFindingEvent :exec
INSERT INTO finding_events (
  finding_key,
  event_key,
  event_type,
  occurred_at,
  actor_kind,
  actor_id,
  message,
  old_status,
  new_status,
  old_severity,
  new_severity,
  source_event_received_at,
  source_event_id,
  evaluation_run_id,
  sync_run_id,
  policy_control_id,
  payload
)
VALUES (
  sqlc.arg(finding_key)::text,
  sqlc.arg(event_key)::text,
  sqlc.arg(event_type)::text,
  sqlc.arg(occurred_at)::timestamptz,
  sqlc.arg(actor_kind)::text,
  sqlc.arg(actor_id)::text,
  sqlc.arg(message)::text,
  sqlc.arg(old_status)::text,
  sqlc.arg(new_status)::text,
  sqlc.arg(old_severity)::text,
  sqlc.arg(new_severity)::text,
  sqlc.narg(source_event_received_at)::timestamptz,
  sqlc.narg(source_event_id)::uuid,
  sqlc.narg(evaluation_run_id)::bigint,
  sqlc.narg(sync_run_id)::bigint,
  sqlc.narg(policy_control_id)::bigint,
  sqlc.arg(payload)::jsonb
)
ON CONFLICT (finding_key, event_key) DO NOTHING;

-- name: GetFindingByKey :one
SELECT *
FROM findings
WHERE finding_key = sqlc.arg(finding_key)::text;

-- name: ListFindingRulesetCurrentByRulesetKey :many
WITH selected_sources AS (
  SELECT
    css.source_kind,
    css.source_name
  FROM connector_source_state css
  WHERE css.configured
    AND css.source_kind = sqlc.arg(source_kind)::text
    AND (
      sqlc.arg(source_name)::text = ''
      OR css.source_name = sqlc.arg(source_name)::text
    )
),
rule_current AS (
  SELECT
    r.id AS rule_id,
    count(ss.source_name)::bigint AS source_count,
    count(*) FILTER (WHERE COALESCE(NULLIF(f.output #>> '{rule_result,status}', ''), 'unknown') = 'fail')::bigint AS fail_count,
    count(*) FILTER (WHERE COALESCE(NULLIF(f.output #>> '{rule_result,status}', ''), 'unknown') = 'error')::bigint AS error_count,
    count(*) FILTER (WHERE COALESCE(NULLIF(f.output #>> '{rule_result,status}', ''), 'unknown') = 'unknown')::bigint AS unknown_count,
    count(*) FILTER (WHERE COALESCE(NULLIF(f.output #>> '{rule_result,status}', ''), 'unknown') = 'not_applicable')::bigint AS not_applicable_count,
    count(*) FILTER (WHERE COALESCE(NULLIF(f.output #>> '{rule_result,status}', ''), 'unknown') = 'pass')::bigint AS pass_count,
    max(f.last_seen_at) AS current_evaluated_at,
    max(COALESCE(NULLIF(f.output #>> '{rule_result,sync_run_id}', '')::bigint, 0))::bigint AS current_sync_run_id,
    (array_remove(array_agg(NULLIF(f.summary, '') ORDER BY
      CASE COALESCE(NULLIF(f.output #>> '{rule_result,status}', ''), 'unknown')
        WHEN 'fail' THEN 1
        WHEN 'error' THEN 2
        WHEN 'unknown' THEN 3
        WHEN 'not_applicable' THEN 4
        WHEN 'pass' THEN 5
        ELSE 6
      END,
      f.last_seen_at DESC NULLS LAST
    ), NULL))[1]::text AS first_evidence_summary,
    (array_remove(array_agg(NULLIF(f.output #>> '{rule_result,error_kind}', '') ORDER BY f.last_seen_at DESC NULLS LAST), NULL))[1]::text AS first_error_kind
  FROM rules r
  JOIN rulesets rs ON rs.id = r.ruleset_id
  LEFT JOIN selected_sources ss ON TRUE
  LEFT JOIN findings f
    ON f.ruleset_id = rs.key
    AND f.rule_id = r.key
    AND f.scope_kind = sqlc.arg(scope_kind)::text
    AND f.scope_source_kind = ss.source_kind
    AND f.scope_source_name = ss.source_name
  WHERE rs.key = sqlc.arg(key)::text
    AND r.is_active = true
  GROUP BY r.id
)
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
  r.is_active,
  r.definition_json,
  r.created_at,
  r.updated_at,
  CASE
    WHEN sqlc.arg(scope_kind)::text = 'connector_instance' THEN
      CASE
        WHEN COALESCE(rc.source_count, 0) = 0 THEN 'unknown'
        WHEN COALESCE(rc.fail_count, 0) > 0 THEN 'fail'
        WHEN COALESCE(rc.error_count, 0) > 0 THEN 'error'
        WHEN COALESCE(rc.unknown_count, 0) > 0 THEN 'unknown'
        WHEN COALESCE(rc.not_applicable_count, 0) = rc.source_count THEN 'not_applicable'
        WHEN COALESCE(rc.pass_count, 0) = rc.source_count THEN 'pass'
        ELSE 'unknown'
      END
    ELSE COALESCE(NULLIF(f.output #>> '{rule_result,status}', ''), 'unknown')
  END::text AS current_status,
  CASE
    WHEN sqlc.arg(scope_kind)::text = 'connector_instance' THEN rc.current_evaluated_at
    ELSE f.last_seen_at
  END::timestamptz AS current_evaluated_at,
  CASE
    WHEN sqlc.arg(scope_kind)::text = 'connector_instance' THEN COALESCE(rc.current_sync_run_id, 0)
    ELSE COALESCE(NULLIF(f.output #>> '{rule_result,sync_run_id}', '')::bigint, 0)
  END::bigint AS current_sync_run_id,
  CASE
    WHEN sqlc.arg(scope_kind)::text = 'connector_instance' THEN
      CASE
        WHEN COALESCE(rc.source_count, 0) = 0 THEN ''
        WHEN rc.source_count = 1 THEN COALESCE(rc.first_evidence_summary, '')
        ELSE concat(
          rc.fail_count, ' fail, ',
          rc.error_count, ' error, ',
          rc.unknown_count, ' unknown, ',
          rc.not_applicable_count, ' not applicable, ',
          rc.pass_count, ' pass'
        )
      END
    ELSE COALESCE(f.summary, '')
  END::text AS current_evidence_summary,
  CASE
    WHEN sqlc.arg(scope_kind)::text = 'connector_instance' THEN COALESCE(rc.first_error_kind, '')
    ELSE COALESCE(f.output #>> '{rule_result,error_kind}', '')
  END::text AS current_error_kind
FROM rules r
JOIN rulesets rs ON rs.id = r.ruleset_id
LEFT JOIN rule_current rc ON rc.rule_id = r.id
LEFT JOIN findings f
  ON f.ruleset_id = rs.key
  AND f.rule_id = r.key
  AND f.scope_kind = sqlc.arg(scope_kind)::text
  AND f.scope_source_kind = sqlc.arg(source_kind)::text
  AND f.scope_source_name = sqlc.arg(source_name)::text
WHERE rs.key = sqlc.arg(key)::text
  AND r.is_active = true
ORDER BY r.key;

-- name: GetFindingRuleCurrentByRulesetKeyAndRuleKey :one
WITH selected_sources AS (
  SELECT
    css.source_kind,
    css.source_name
  FROM connector_source_state css
  WHERE css.configured
    AND css.source_kind = sqlc.arg(source_kind)::text
    AND (
      sqlc.arg(source_name)::text = ''
      OR css.source_name = sqlc.arg(source_name)::text
    )
),
rule_current AS (
  SELECT
    r.id AS rule_id,
    count(ss.source_name)::bigint AS source_count,
    count(*) FILTER (WHERE COALESCE(NULLIF(f.output #>> '{rule_result,status}', ''), 'unknown') = 'fail')::bigint AS fail_count,
    count(*) FILTER (WHERE COALESCE(NULLIF(f.output #>> '{rule_result,status}', ''), 'unknown') = 'error')::bigint AS error_count,
    count(*) FILTER (WHERE COALESCE(NULLIF(f.output #>> '{rule_result,status}', ''), 'unknown') = 'unknown')::bigint AS unknown_count,
    count(*) FILTER (WHERE COALESCE(NULLIF(f.output #>> '{rule_result,status}', ''), 'unknown') = 'not_applicable')::bigint AS not_applicable_count,
    count(*) FILTER (WHERE COALESCE(NULLIF(f.output #>> '{rule_result,status}', ''), 'unknown') = 'pass')::bigint AS pass_count,
    max(f.last_seen_at) AS current_evaluated_at,
    max(COALESCE(NULLIF(f.output #>> '{rule_result,sync_run_id}', '')::bigint, 0))::bigint AS current_sync_run_id,
    (array_remove(array_agg(NULLIF(f.summary, '') ORDER BY
      CASE COALESCE(NULLIF(f.output #>> '{rule_result,status}', ''), 'unknown')
        WHEN 'fail' THEN 1
        WHEN 'error' THEN 2
        WHEN 'unknown' THEN 3
        WHEN 'not_applicable' THEN 4
        WHEN 'pass' THEN 5
        ELSE 6
      END,
      f.last_seen_at DESC NULLS LAST
    ), NULL))[1]::text AS first_evidence_summary,
    (array_remove(array_agg(NULLIF(f.output #>> '{rule_result,error_kind}', '') ORDER BY f.last_seen_at DESC NULLS LAST), NULL))[1]::text AS first_error_kind,
    (jsonb_agg(f.output -> 'evidence' ORDER BY
      CASE COALESCE(NULLIF(f.output #>> '{rule_result,status}', ''), 'unknown')
        WHEN 'fail' THEN 1
        WHEN 'error' THEN 2
        WHEN 'unknown' THEN 3
        WHEN 'not_applicable' THEN 4
        WHEN 'pass' THEN 5
        ELSE 6
      END,
      f.last_seen_at DESC NULLS LAST
    ) FILTER (WHERE f.output ? 'evidence'))->0 AS first_evidence_json
  FROM rules r
  JOIN rulesets rs ON rs.id = r.ruleset_id
  LEFT JOIN selected_sources ss ON TRUE
  LEFT JOIN findings f
    ON f.ruleset_id = rs.key
    AND f.rule_id = r.key
    AND f.scope_kind = sqlc.arg(scope_kind)::text
    AND f.scope_source_kind = ss.source_kind
    AND f.scope_source_name = ss.source_name
  WHERE rs.key = sqlc.arg(ruleset_key)::text
    AND r.key = sqlc.arg(rule_key)::text
    AND r.is_active = true
  GROUP BY r.id
)
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
  r.is_active,
  r.definition_json,
  r.created_at,
  r.updated_at,
  CASE
    WHEN sqlc.arg(scope_kind)::text = 'connector_instance' THEN
      CASE
        WHEN COALESCE(rc.source_count, 0) = 0 THEN 'unknown'
        WHEN COALESCE(rc.fail_count, 0) > 0 THEN 'fail'
        WHEN COALESCE(rc.error_count, 0) > 0 THEN 'error'
        WHEN COALESCE(rc.unknown_count, 0) > 0 THEN 'unknown'
        WHEN COALESCE(rc.not_applicable_count, 0) = rc.source_count THEN 'not_applicable'
        WHEN COALESCE(rc.pass_count, 0) = rc.source_count THEN 'pass'
        ELSE 'unknown'
      END
    ELSE COALESCE(NULLIF(f.output #>> '{rule_result,status}', ''), 'unknown')
  END::text AS current_status,
  CASE
    WHEN sqlc.arg(scope_kind)::text = 'connector_instance' THEN rc.current_evaluated_at
    ELSE f.last_seen_at
  END::timestamptz AS current_evaluated_at,
  CASE
    WHEN sqlc.arg(scope_kind)::text = 'connector_instance' THEN COALESCE(rc.current_sync_run_id, 0)
    ELSE COALESCE(NULLIF(f.output #>> '{rule_result,sync_run_id}', '')::bigint, 0)
  END::bigint AS current_sync_run_id,
  CASE
    WHEN sqlc.arg(scope_kind)::text = 'connector_instance' THEN
      CASE
        WHEN COALESCE(rc.source_count, 0) = 0 THEN ''
        WHEN rc.source_count = 1 THEN COALESCE(rc.first_evidence_summary, '')
        ELSE concat(
          rc.fail_count, ' fail, ',
          rc.error_count, ' error, ',
          rc.unknown_count, ' unknown, ',
          rc.not_applicable_count, ' not applicable, ',
          rc.pass_count, ' pass'
        )
      END
    ELSE COALESCE(f.summary, '')
  END::text AS current_evidence_summary,
  CASE
    WHEN sqlc.arg(scope_kind)::text = 'connector_instance' THEN
      CASE
        WHEN COALESCE(rc.source_count, 0) = 1 THEN COALESCE(rc.first_evidence_json, '{}'::jsonb)
        ELSE '{}'::jsonb
      END
    ELSE COALESCE(f.output -> 'evidence', '{}'::jsonb)
  END::jsonb AS current_evidence_json,
  CASE
    WHEN sqlc.arg(scope_kind)::text = 'connector_instance' THEN COALESCE(rc.first_error_kind, '')
    ELSE COALESCE(f.output #>> '{rule_result,error_kind}', '')
  END::text AS current_error_kind
FROM rules r
JOIN rulesets rs ON rs.id = r.ruleset_id
LEFT JOIN rule_current rc ON rc.rule_id = r.id
LEFT JOIN findings f
  ON f.ruleset_id = rs.key
  AND f.rule_id = r.key
  AND f.scope_kind = sqlc.arg(scope_kind)::text
  AND f.scope_source_kind = sqlc.arg(source_kind)::text
  AND f.scope_source_name = sqlc.arg(source_name)::text
WHERE rs.key = sqlc.arg(ruleset_key)::text
  AND r.key = sqlc.arg(rule_key)::text
  AND r.is_active = true;

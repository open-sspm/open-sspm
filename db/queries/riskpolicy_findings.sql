-- name: ListRiskpolicyEventShadowSignalsForFindingProjection :many
SELECT
  s.event_received_at,
  s.event_id,
  e.source_kind::text AS source_kind,
  e.source_name::text AS source_name,
  COALESCE(NULLIF(trim(e.target_kind), ''), 'event')::text AS entity_kind,
  COALESCE(NULLIF(trim(e.target_id), ''), e.provider_event_id, '')::text AS entity_id,
  COALESCE(NULLIF(trim(e.target_name), ''), e.event_type, '')::text AS entity_name,
  s.signal_id::text AS signal_id,
  s.policy_pack_id::text AS policy_pack_id,
  s.policy_pack_version::text AS policy_pack_version,
  s.severity::text AS severity,
  s.title::text AS title,
  s.evidence::text AS evidence,
  s.output::jsonb AS output
FROM riskpolicy_event_shadow_signals s
JOIN events e
  ON e.received_at = s.event_received_at
 AND e.id = s.event_id
LEFT JOIN riskpolicy_findings f
  ON f.source = 'riskpolicy_event_shadow'
 AND f.event_received_at = s.event_received_at
 AND f.event_id = s.event_id
 AND f.signal_id = s.signal_id
WHERE (sqlc.narg(since)::timestamptz IS NULL OR s.evaluated_at >= sqlc.narg(since)::timestamptz)
  AND (sqlc.narg(until)::timestamptz IS NULL OR s.evaluated_at < sqlc.narg(until)::timestamptz)
  AND (
    f.finding_key IS NULL
    OR f.updated_at < s.evaluated_at
  )
ORDER BY s.evaluated_at ASC, s.event_received_at ASC, s.event_id ASC, s.signal_id ASC
LIMIT sqlc.arg(limit_rows)::int;

-- name: UpsertRiskpolicyFinding :exec
INSERT INTO riskpolicy_findings (
  finding_key,
  source,
  shadow,
  status,
  source_kind,
  source_name,
  entity_kind,
  entity_id,
  entity_name,
  event_received_at,
  event_id,
  signal_id,
  policy_pack_id,
  policy_pack_version,
  severity,
  title,
  evidence,
  output,
  first_seen_at,
  last_seen_at,
  resolved_at,
  updated_at
) VALUES (
  sqlc.arg(finding_key)::text,
  sqlc.arg(source)::text,
  sqlc.arg(shadow)::boolean,
  'open',
  sqlc.arg(source_kind)::text,
  sqlc.arg(source_name)::text,
  sqlc.arg(entity_kind)::text,
  sqlc.arg(entity_id)::text,
  sqlc.arg(entity_name)::text,
  sqlc.narg(event_received_at)::timestamptz,
  sqlc.narg(event_id)::uuid,
  sqlc.arg(signal_id)::text,
  sqlc.arg(policy_pack_id)::text,
  sqlc.arg(policy_pack_version)::text,
  sqlc.arg(severity)::text,
  sqlc.arg(title)::text,
  sqlc.arg(evidence)::text,
  sqlc.arg(output)::jsonb,
  now(),
  now(),
  NULL,
  now()
)
ON CONFLICT (finding_key) DO UPDATE SET
  source_kind = EXCLUDED.source_kind,
  source_name = EXCLUDED.source_name,
  entity_kind = EXCLUDED.entity_kind,
  entity_id = EXCLUDED.entity_id,
  entity_name = EXCLUDED.entity_name,
  event_received_at = EXCLUDED.event_received_at,
  event_id = EXCLUDED.event_id,
  signal_id = EXCLUDED.signal_id,
  policy_pack_id = EXCLUDED.policy_pack_id,
  policy_pack_version = EXCLUDED.policy_pack_version,
  severity = EXCLUDED.severity,
  title = EXCLUDED.title,
  evidence = EXCLUDED.evidence,
  output = EXCLUDED.output,
  last_seen_at = now(),
  updated_at = now();

-- name: CountRiskpolicyFindingsByShadowStatus :one
SELECT count(*)::bigint
FROM riskpolicy_findings
WHERE shadow = sqlc.arg(shadow)::boolean
  AND status = sqlc.arg(status)::text;

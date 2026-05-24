-- name: CreateIdentityMergeEvent :one
INSERT INTO identity_merge_events (
  source_identity_id,
  target_identity_id,
  status,
  reason,
  requested_by,
  reviewed_by,
  metadata
)
VALUES (
  sqlc.arg(source_identity_id)::bigint,
  sqlc.arg(target_identity_id)::bigint,
  COALESCE(NULLIF(trim(sqlc.arg(status)::text), ''), 'pending'),
  sqlc.arg(reason)::text,
  NULLIF(trim(sqlc.narg(requested_by)::text), ''),
  NULLIF(trim(sqlc.narg(reviewed_by)::text), ''),
  COALESCE(sqlc.narg(metadata)::jsonb, '{}'::jsonb)
)
RETURNING *;

-- name: MarkIdentityMergeEventApplied :one
UPDATE identity_merge_events
SET
  status = 'applied',
  applied_at = now()
WHERE id = sqlc.arg(id)::bigint
  AND status = 'pending'
RETURNING *;

-- name: MarkIdentityMerged :exec
UPDATE identities
SET
  resolution_state = 'merged',
  updated_at = now()
WHERE id = sqlc.arg(source_identity_id)::bigint;

-- name: MoveIdentityAccountsToIdentity :exec
UPDATE identity_accounts
SET
  identity_id = sqlc.arg(target_identity_id)::bigint,
  link_reason = CASE
    WHEN link_state = 'needs_review' THEN link_reason
    ELSE COALESCE(NULLIF(trim(sqlc.arg(link_reason)::text), ''), link_reason)
  END,
  link_state = CASE
    WHEN link_state = 'needs_review' THEN link_state
    WHEN lower(trim(sqlc.arg(link_reason)::text)) IN ('manual', 'manual_merge') THEN 'manual_confirmed'
    ELSE link_state
  END,
  reviewed_by = NULLIF(trim(sqlc.narg(reviewed_by)::text), ''),
  reviewed_at = now(),
  updated_at = now()
WHERE identity_id = sqlc.arg(source_identity_id)::bigint;

-- name: MoveIdentityEmailsToIdentity :exec
-- Rewrites every source-identity email row to the merge target, regardless of
-- lifecycle_state, so the merged shell ends up with no email rows. Active rows
-- that would collide with an existing active row on the target are demoted to
-- historical in place; their provenance (source_kind, source_name,
-- source_account_id) is preserved under the target as an audit echo.
UPDATE identity_emails source_email
SET
  identity_id = sqlc.arg(target_identity_id)::bigint,
  email_kind = CASE
    WHEN source_email.lifecycle_state = 'active'
      AND EXISTS (
        SELECT 1
        FROM identity_emails target_email
        WHERE target_email.identity_id = sqlc.arg(target_identity_id)::bigint
          AND target_email.normalized_email = source_email.normalized_email
          AND target_email.lifecycle_state = 'active'
      ) THEN 'historical'
    WHEN source_email.lifecycle_state = 'active'
      AND source_email.is_primary
      AND NOT EXISTS (
        SELECT 1
        FROM identity_emails target_primary
        WHERE target_primary.identity_id = sqlc.arg(target_identity_id)::bigint
          AND target_primary.lifecycle_state = 'active'
          AND target_primary.is_primary
      ) THEN 'primary'
    WHEN source_email.lifecycle_state = 'active' AND source_email.email_kind = 'primary' THEN 'alias'
    ELSE source_email.email_kind
  END,
  lifecycle_state = CASE
    WHEN source_email.lifecycle_state = 'active'
      AND EXISTS (
        SELECT 1
        FROM identity_emails target_email
        WHERE target_email.identity_id = sqlc.arg(target_identity_id)::bigint
          AND target_email.normalized_email = source_email.normalized_email
          AND target_email.lifecycle_state = 'active'
      ) THEN 'historical'
    ELSE source_email.lifecycle_state
  END,
  is_primary = CASE
    WHEN source_email.lifecycle_state = 'active'
      AND source_email.is_primary
      AND NOT EXISTS (
        SELECT 1
        FROM identity_emails target_email
        WHERE target_email.identity_id = sqlc.arg(target_identity_id)::bigint
          AND target_email.normalized_email = source_email.normalized_email
          AND target_email.lifecycle_state = 'active'
      )
      AND NOT EXISTS (
        SELECT 1
        FROM identity_emails target_primary
        WHERE target_primary.identity_id = sqlc.arg(target_identity_id)::bigint
          AND target_primary.lifecycle_state = 'active'
          AND target_primary.is_primary
      ) THEN TRUE
    ELSE FALSE
  END,
  reviewed_by = NULLIF(trim(sqlc.narg(reviewed_by)::text), ''),
  reviewed_at = now(),
  updated_at = now()
WHERE source_email.identity_id = sqlc.arg(source_identity_id)::bigint;

-- name: MoveIdentityAnchorsToIdentity :exec
-- Rewrites every source-identity anchor row to the merge target, regardless of
-- lifecycle_state, so the merged shell ends up with no anchor rows. Active
-- rows that would collide with an existing active anchor on the target are
-- demoted to historical in place so their provenance survives the merge.
UPDATE identity_anchors source_anchor
SET
  identity_id = sqlc.arg(target_identity_id)::bigint,
  lifecycle_state = CASE
    WHEN source_anchor.lifecycle_state = 'active'
      AND EXISTS (
        SELECT 1
        FROM identity_anchors target_anchor
        WHERE target_anchor.identity_id = sqlc.arg(target_identity_id)::bigint
          AND target_anchor.anchor_kind = source_anchor.anchor_kind
          AND target_anchor.issuer = source_anchor.issuer
          AND target_anchor.normalized_anchor_value = source_anchor.normalized_anchor_value
          AND target_anchor.lifecycle_state = 'active'
      ) THEN 'historical'
    ELSE source_anchor.lifecycle_state
  END,
  reviewed_by = NULLIF(trim(sqlc.narg(reviewed_by)::text), ''),
  reviewed_at = now(),
  updated_at = now()
WHERE source_anchor.identity_id = sqlc.arg(source_identity_id)::bigint;

-- name: UpsertIdentityMergeRedirect :one
INSERT INTO identity_merge_redirects (
  source_identity_id,
  target_identity_id,
  merge_event_id
)
VALUES (
  sqlc.arg(source_identity_id)::bigint,
  sqlc.arg(target_identity_id)::bigint,
  sqlc.arg(merge_event_id)::bigint
)
ON CONFLICT (source_identity_id) DO UPDATE SET
  target_identity_id = EXCLUDED.target_identity_id,
  merge_event_id = EXCLUDED.merge_event_id,
  merged_at = now()
RETURNING *;

-- name: GetIdentityMergeRedirect :one
SELECT *
FROM identity_merge_redirects
WHERE source_identity_id = $1;

-- name: UpsertAccountIdentityRelationship :one
INSERT INTO account_identity_relationships (
  account_id,
  identity_id,
  relationship_type,
  source_kind,
  source_name,
  confidence,
  lifecycle_state,
  last_seen_at,
  updated_at
)
VALUES (
  sqlc.arg(account_id)::bigint,
  sqlc.arg(identity_id)::bigint,
  sqlc.arg(relationship_type)::text,
  NULLIF(trim(sqlc.narg(source_kind)::text), ''),
  NULLIF(trim(sqlc.narg(source_name)::text), ''),
  sqlc.arg(confidence)::int,
  COALESCE(NULLIF(trim(sqlc.arg(lifecycle_state)::text), ''), 'active'),
  now(),
  now()
)
ON CONFLICT (account_id, identity_id, relationship_type)
WHERE lifecycle_state = 'active'
DO UPDATE SET
  source_kind = COALESCE(EXCLUDED.source_kind, account_identity_relationships.source_kind),
  source_name = COALESCE(EXCLUDED.source_name, account_identity_relationships.source_name),
  confidence = EXCLUDED.confidence,
  last_seen_at = EXCLUDED.last_seen_at,
  updated_at = EXCLUDED.updated_at
RETURNING *;

-- name: ListAccountIdentityRelationships :many
SELECT *
FROM account_identity_relationships
WHERE account_id = $1
  AND lifecycle_state = COALESCE(NULLIF(trim(sqlc.arg(lifecycle_state)::text), ''), 'active')
ORDER BY relationship_type, identity_id;

-- name: ListIdentityScopedAccountRelationships :many
SELECT
  rel.id,
  rel.account_id,
  rel.identity_id,
  rel.relationship_type,
  rel.source_kind,
  rel.source_name,
  rel.confidence,
  rel.lifecycle_state,
  rel.first_seen_at,
  rel.last_seen_at,
  rel.created_at,
  rel.updated_at,
  owner.display_name::text AS relationship_display_name,
  owner.primary_email::text AS relationship_primary_email,
  owner.kind::text AS relationship_identity_kind,
  owner.resolution_state::text AS relationship_resolution_state,
  a.source_kind::text AS account_source_kind,
  a.source_name::text AS account_source_name,
  a.external_id::text AS account_external_id,
  COALESCE(NULLIF(trim(a.display_name), ''), a.external_id)::text AS account_display_name
FROM identity_accounts ia
JOIN accounts a ON a.id = ia.account_id
JOIN account_identity_relationships rel
  ON rel.account_id = ia.account_id
JOIN identities owner ON owner.id = rel.identity_id
WHERE ia.identity_id = sqlc.arg(identity_id)::bigint
  AND a.expired_at IS NULL
  AND a.last_observed_run_id IS NOT NULL
  AND rel.lifecycle_state = COALESCE(NULLIF(trim(sqlc.arg(lifecycle_state)::text), ''), 'active')
  AND owner.resolution_state NOT IN ('merged', 'disabled')
ORDER BY
  rel.relationship_type,
  lower(COALESCE(NULLIF(trim(owner.display_name), ''), NULLIF(trim(owner.primary_email), ''), owner.id::text)),
  a.source_kind,
  a.source_name,
  a.external_id;

-- name: MoveAccountIdentityRelationshipsToIdentity :exec
-- Rewrites active account_identity_relationships pointing at the merged source
-- identity so they follow the merge to the target. Rows that would collide with
-- an existing active (account_id, identity_id=target, relationship_type) row
-- are left to RetireRemainingAccountIdentityRelationships, which historizes
-- them so the audit trail remains attached to the target.
UPDATE account_identity_relationships source_rel
SET
  identity_id = sqlc.arg(target_identity_id)::bigint,
  updated_at = now()
WHERE source_rel.identity_id = sqlc.arg(source_identity_id)::bigint
  AND source_rel.lifecycle_state = 'active'
  AND NOT EXISTS (
    SELECT 1
    FROM account_identity_relationships target_rel
    WHERE target_rel.account_id = source_rel.account_id
      AND target_rel.identity_id = sqlc.arg(target_identity_id)::bigint
      AND target_rel.relationship_type = source_rel.relationship_type
      AND target_rel.lifecycle_state = 'active'
  );

-- name: RetireRemainingAccountIdentityRelationships :exec
-- Historizes any active relationships still pointing at the merged source
-- identity (those that collided with an existing target relationship in
-- MoveAccountIdentityRelationshipsToIdentity). The rewrite to the target
-- identity preserves the provenance under the merge target rather than
-- leaving the rows attached to a merged identity.
UPDATE account_identity_relationships
SET
  identity_id = sqlc.arg(target_identity_id)::bigint,
  lifecycle_state = 'historical',
  updated_at = now()
WHERE identity_id = sqlc.arg(source_identity_id)::bigint
  AND lifecycle_state = 'active';

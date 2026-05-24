-- name: UpsertIdentityEmail :one
INSERT INTO identity_emails (
  identity_id,
  email,
  normalized_email,
  email_kind,
  verification_state,
  lifecycle_state,
  is_primary,
  source_kind,
  source_name,
  source_account_id,
  last_seen_at,
  updated_at
)
VALUES (
  sqlc.arg(identity_id)::bigint,
  sqlc.arg(email)::text,
  lower(trim(sqlc.arg(normalized_email)::text)),
  COALESCE(NULLIF(trim(sqlc.arg(email_kind)::text), ''), 'alias'),
  COALESCE(NULLIF(trim(sqlc.arg(verification_state)::text), ''), 'observed'),
  COALESCE(NULLIF(trim(sqlc.arg(lifecycle_state)::text), ''), 'active'),
  sqlc.arg(is_primary)::boolean,
  NULLIF(trim(sqlc.narg(source_kind)::text), ''),
  NULLIF(trim(sqlc.narg(source_name)::text), ''),
  sqlc.narg(source_account_id)::bigint,
  now(),
  now()
)
ON CONFLICT (identity_id, normalized_email)
WHERE lifecycle_state = 'active'
DO UPDATE SET
  email = EXCLUDED.email,
  email_kind = CASE
    WHEN identity_emails.email_kind = 'primary' THEN identity_emails.email_kind
    WHEN EXCLUDED.email_kind = 'primary' THEN EXCLUDED.email_kind
    WHEN identity_emails.email_kind = 'login' AND EXCLUDED.email_kind = 'alias' THEN identity_emails.email_kind
    ELSE EXCLUDED.email_kind
  END,
  verification_state = CASE
    WHEN CASE identity_emails.verification_state
      WHEN 'verified_authoritative' THEN 0
      WHEN 'verified_source' THEN 1
      WHEN 'manual' THEN 2
      WHEN 'observed' THEN 3
      WHEN 'inferred_legacy' THEN 4
      ELSE 5
    END <= CASE EXCLUDED.verification_state
      WHEN 'verified_authoritative' THEN 0
      WHEN 'verified_source' THEN 1
      WHEN 'manual' THEN 2
      WHEN 'observed' THEN 3
      WHEN 'inferred_legacy' THEN 4
      ELSE 5
    END THEN identity_emails.verification_state
    ELSE EXCLUDED.verification_state
  END,
  is_primary = identity_emails.is_primary OR EXCLUDED.is_primary,
  source_kind = COALESCE(EXCLUDED.source_kind, identity_emails.source_kind),
  source_name = COALESCE(EXCLUDED.source_name, identity_emails.source_name),
  source_account_id = COALESCE(EXCLUDED.source_account_id, identity_emails.source_account_id),
  last_seen_at = EXCLUDED.last_seen_at,
  updated_at = EXCLUDED.updated_at
RETURNING *;

-- name: ListIdentityEmails :many
SELECT *
FROM identity_emails
WHERE identity_id = $1
ORDER BY is_primary DESC, lifecycle_state, email_kind, normalized_email;

-- name: GetIdentityForIdentityResolution :one
SELECT *
FROM identities
WHERE id = $1;

-- name: CountActiveAccountsForIdentity :one
-- Counts source accounts currently linked to an identity that are still
-- observable from a sync run. Used by the candidate accept flow to detect
-- when accepting the candidate leaves a provisional identity with no live
-- accounts, in which case the merge into the target should be auto-applied.
SELECT count(*)::bigint
FROM identity_accounts ia
JOIN accounts a ON a.id = ia.account_id
WHERE ia.identity_id = $1
  AND a.expired_at IS NULL
  AND a.last_observed_run_id IS NOT NULL;

-- name: FindIdentitiesByNormalizedEmail :many
SELECT
  i.id,
  i.kind,
  i.display_name,
  i.primary_email,
  i.created_at,
  i.updated_at,
  ie.id AS identity_email_id,
  ie.email,
  ie.normalized_email,
  ie.email_kind,
  ie.verification_state,
  ie.lifecycle_state,
  ie.is_primary
FROM identity_emails ie
JOIN identities i ON i.id = ie.identity_id
WHERE ie.normalized_email = lower(trim(sqlc.arg(normalized_email)::text))
  AND ie.lifecycle_state = COALESCE(NULLIF(trim(sqlc.arg(lifecycle_state)::text), ''), 'active')
ORDER BY
  CASE ie.verification_state
    WHEN 'verified_authoritative' THEN 0
    WHEN 'verified_source' THEN 1
    WHEN 'manual' THEN 2
    WHEN 'observed' THEN 3
    WHEN 'inferred_legacy' THEN 4
    ELSE 5
  END,
  i.id;

-- name: FindUnambiguousIdentityByNormalizedEmail :one
WITH candidates AS (
  SELECT DISTINCT i.*
  FROM identity_emails ie
  JOIN identities i ON i.id = ie.identity_id
  WHERE ie.normalized_email = lower(trim(sqlc.arg(normalized_email)::text))
    AND ie.lifecycle_state = 'active'
    AND i.resolution_state NOT IN ('merged', 'disabled')
)
SELECT *
FROM candidates
WHERE (SELECT count(*) FROM candidates) = 1;

-- name: UpsertAccountAnchor :one
INSERT INTO account_anchors (
  account_id,
  source_kind,
  source_name,
  anchor_kind,
  issuer,
  anchor_value,
  normalized_anchor_value,
  extraction_method,
  last_seen_at,
  updated_at
)
VALUES (
  sqlc.arg(account_id)::bigint,
  sqlc.arg(source_kind)::text,
  sqlc.arg(source_name)::text,
  sqlc.arg(anchor_kind)::text,
  sqlc.arg(issuer)::text,
  sqlc.arg(anchor_value)::text,
  lower(trim(sqlc.arg(normalized_anchor_value)::text)),
  COALESCE(NULLIF(trim(sqlc.arg(extraction_method)::text), ''), 'connector'),
  now(),
  now()
)
ON CONFLICT (account_id, anchor_kind, issuer, normalized_anchor_value)
DO UPDATE SET
  anchor_value = EXCLUDED.anchor_value,
  extraction_method = EXCLUDED.extraction_method,
  last_seen_at = EXCLUDED.last_seen_at,
  updated_at = EXCLUDED.updated_at
RETURNING *;

-- name: UpsertIdentityAnchor :one
INSERT INTO identity_anchors (
  identity_id,
  anchor_kind,
  issuer,
  anchor_value,
  normalized_anchor_value,
  source_kind,
  source_name,
  source_account_id,
  trust_level,
  lifecycle_state,
  last_seen_at,
  updated_at
)
VALUES (
  sqlc.arg(identity_id)::bigint,
  sqlc.arg(anchor_kind)::text,
  sqlc.arg(issuer)::text,
  sqlc.arg(anchor_value)::text,
  lower(trim(sqlc.arg(normalized_anchor_value)::text)),
  NULLIF(trim(sqlc.narg(source_kind)::text), ''),
  NULLIF(trim(sqlc.narg(source_name)::text), ''),
  sqlc.narg(source_account_id)::bigint,
  COALESCE(NULLIF(trim(sqlc.arg(trust_level)::text), ''), 'source_observed'),
  COALESCE(NULLIF(trim(sqlc.arg(lifecycle_state)::text), ''), 'active'),
  now(),
  now()
)
ON CONFLICT (anchor_kind, issuer, normalized_anchor_value)
WHERE lifecycle_state = 'active'
DO UPDATE SET
  anchor_value = CASE
    WHEN identity_anchors.identity_id = EXCLUDED.identity_id THEN EXCLUDED.anchor_value
    ELSE identity_anchors.anchor_value
  END,
  source_kind = CASE
    WHEN identity_anchors.identity_id = EXCLUDED.identity_id THEN COALESCE(EXCLUDED.source_kind, identity_anchors.source_kind)
    ELSE identity_anchors.source_kind
  END,
  source_name = CASE
    WHEN identity_anchors.identity_id = EXCLUDED.identity_id THEN COALESCE(EXCLUDED.source_name, identity_anchors.source_name)
    ELSE identity_anchors.source_name
  END,
  source_account_id = CASE
    WHEN identity_anchors.identity_id = EXCLUDED.identity_id THEN COALESCE(EXCLUDED.source_account_id, identity_anchors.source_account_id)
    ELSE identity_anchors.source_account_id
  END,
  trust_level = CASE
    WHEN identity_anchors.identity_id <> EXCLUDED.identity_id THEN identity_anchors.trust_level
    WHEN CASE identity_anchors.trust_level
      WHEN 'authoritative' THEN 0
      WHEN 'manual' THEN 1
      WHEN 'source_observed' THEN 2
      ELSE 3
    END <= CASE EXCLUDED.trust_level
      WHEN 'authoritative' THEN 0
      WHEN 'manual' THEN 1
      WHEN 'source_observed' THEN 2
      ELSE 3
    END THEN identity_anchors.trust_level
    ELSE EXCLUDED.trust_level
  END,
  last_seen_at = CASE
    WHEN identity_anchors.identity_id = EXCLUDED.identity_id THEN EXCLUDED.last_seen_at
    ELSE identity_anchors.last_seen_at
  END,
  updated_at = CASE
    WHEN identity_anchors.identity_id = EXCLUDED.identity_id THEN EXCLUDED.updated_at
    ELSE identity_anchors.updated_at
  END
RETURNING *;

-- name: ListIdentityAnchors :many
SELECT *
FROM identity_anchors
WHERE identity_id = $1
ORDER BY lifecycle_state, trust_level, anchor_kind, issuer, normalized_anchor_value;

-- name: ListIdentityAnchorMatchesForAccount :many
SELECT
  aa.account_id,
  ia.identity_id,
  ia.anchor_kind,
  ia.issuer,
  ia.normalized_anchor_value,
  ia.trust_level
FROM account_anchors aa
JOIN identity_anchors ia
  ON ia.anchor_kind = aa.anchor_kind
 AND ia.issuer = aa.issuer
 AND ia.normalized_anchor_value = aa.normalized_anchor_value
 AND ia.lifecycle_state = 'active'
WHERE aa.account_id = $1
ORDER BY
  CASE ia.trust_level
    WHEN 'authoritative' THEN 0
    WHEN 'manual' THEN 1
    WHEN 'source_observed' THEN 2
    ELSE 3
  END,
  ia.identity_id;

-- name: EnsureProvisionalIdentityForAccount :one
WITH existing AS (
  SELECT i.*
  FROM identity_accounts ia
  JOIN identities i ON i.id = ia.identity_id
  WHERE ia.account_id = sqlc.arg(account_id)::bigint
),
created AS (
  INSERT INTO identities (kind, display_name, primary_email, resolution_state, identity_kind)
  SELECT
    COALESCE(NULLIF(trim(a.account_kind), ''), 'unknown'),
    COALESCE(a.display_name, ''),
    lower(trim(COALESCE(a.email, ''))),
    'provisional',
    CASE
      WHEN a.account_kind IN ('human', 'service', 'bot') THEN a.account_kind
      ELSE 'unknown'
    END
  FROM accounts a
  WHERE a.id = sqlc.arg(account_id)::bigint
    AND NOT EXISTS (SELECT 1 FROM existing)
  RETURNING *
)
SELECT *
FROM created
UNION ALL
SELECT *
FROM existing
LIMIT 1;

-- name: UpsertIdentityMatchCandidate :one
WITH existing_rejected AS (
  SELECT *
  FROM identity_match_candidates
  WHERE account_id = sqlc.arg(account_id)::bigint
    AND candidate_identity_id = sqlc.arg(candidate_identity_id)::bigint
    AND resolver_fingerprint = sqlc.arg(resolver_fingerprint)::text
    AND status = 'rejected'
),
upserted AS (
INSERT INTO identity_match_candidates (
  account_id,
  candidate_identity_id,
  provisional_identity_id,
  status,
  confidence_band,
  score,
  match_reason,
  ambiguity_key,
  resolver_version,
  resolver_fingerprint,
  updated_at
)
SELECT
  sqlc.arg(account_id)::bigint,
  sqlc.arg(candidate_identity_id)::bigint,
  sqlc.narg(provisional_identity_id)::bigint,
  'pending',
  sqlc.arg(confidence_band)::text,
  sqlc.arg(score)::int,
  sqlc.arg(match_reason)::text,
  NULLIF(trim(sqlc.narg(ambiguity_key)::text), ''),
  sqlc.arg(resolver_version)::text,
  sqlc.arg(resolver_fingerprint)::text,
  now()
WHERE NOT EXISTS (SELECT 1 FROM existing_rejected)
ON CONFLICT (account_id, candidate_identity_id, resolver_fingerprint)
WHERE status = 'pending'
DO UPDATE SET
  provisional_identity_id = COALESCE(EXCLUDED.provisional_identity_id, identity_match_candidates.provisional_identity_id),
  confidence_band = EXCLUDED.confidence_band,
  score = EXCLUDED.score,
  match_reason = EXCLUDED.match_reason,
  ambiguity_key = COALESCE(EXCLUDED.ambiguity_key, identity_match_candidates.ambiguity_key),
  resolver_version = EXCLUDED.resolver_version,
  updated_at = EXCLUDED.updated_at
RETURNING *
)
SELECT *
FROM upserted
UNION ALL
SELECT *
FROM existing_rejected
LIMIT 1;

-- name: CountIdentityMatchCandidatesByStatus :one
SELECT count(*)
FROM identity_match_candidates
WHERE status = COALESCE(NULLIF(trim(sqlc.arg(status)::text), ''), 'pending');

-- name: CountIdentityMatchCandidatesByFilters :one
SELECT count(*)
FROM identity_match_candidates
WHERE status = COALESCE(NULLIF(trim(sqlc.arg(status)::text), ''), 'pending')
  AND (
    COALESCE(NULLIF(trim(sqlc.arg(confidence_band)::text), ''), '') = ''
    OR confidence_band = trim(sqlc.arg(confidence_band)::text)
  )
  AND (
    COALESCE(NULLIF(trim(sqlc.arg(match_reason)::text), ''), '') = ''
    OR match_reason = trim(sqlc.arg(match_reason)::text)
  );

-- name: ListIdentityMatchCandidateDetailsByStatus :many
SELECT
  imc.id,
  imc.account_id,
  imc.candidate_identity_id,
  imc.provisional_identity_id,
  imc.status,
  imc.confidence_band,
  imc.score,
  imc.match_reason,
  imc.ambiguity_key,
  imc.resolver_version,
  imc.resolver_fingerprint,
  imc.created_at,
  imc.updated_at,
  a.source_kind AS account_source_kind,
  a.source_name AS account_source_name,
  a.external_id AS account_external_id,
  a.email AS account_email,
  a.display_name AS account_display_name,
  a.account_kind,
  a.entity_category,
  link.identity_id AS current_identity_id,
  link.link_state AS current_link_state,
  link.link_reason AS current_link_reason,
  current_identity.display_name AS current_identity_display_name,
  current_identity.primary_email AS current_identity_primary_email,
  candidate.display_name AS candidate_display_name,
  candidate.primary_email AS candidate_primary_email,
  candidate.kind AS candidate_kind,
  candidate.resolution_state AS candidate_resolution_state,
  candidate.identity_kind AS candidate_identity_kind,
  provisional.display_name AS provisional_display_name,
  provisional.primary_email AS provisional_primary_email,
  provisional.resolution_state AS provisional_resolution_state,
  (
    SELECT count(*)::bigint
    FROM identity_link_evidence evidence
    WHERE evidence.candidate_id = imc.id
  ) AS evidence_count
FROM identity_match_candidates imc
JOIN accounts a ON a.id = imc.account_id
JOIN identities candidate ON candidate.id = imc.candidate_identity_id
LEFT JOIN identity_accounts link ON link.account_id = imc.account_id
LEFT JOIN identities current_identity ON current_identity.id = link.identity_id
LEFT JOIN identities provisional ON provisional.id = imc.provisional_identity_id
WHERE imc.status = COALESCE(NULLIF(trim(sqlc.arg(status)::text), ''), 'pending')
ORDER BY imc.created_at ASC, imc.id ASC
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: ListIdentityMatchCandidateDetailsByFilters :many
SELECT
  imc.id,
  imc.account_id,
  imc.candidate_identity_id,
  imc.provisional_identity_id,
  imc.status,
  imc.confidence_band,
  imc.score,
  imc.match_reason,
  imc.ambiguity_key,
  imc.resolver_version,
  imc.resolver_fingerprint,
  imc.created_at,
  imc.updated_at,
  a.source_kind AS account_source_kind,
  a.source_name AS account_source_name,
  a.external_id AS account_external_id,
  a.email AS account_email,
  a.display_name AS account_display_name,
  a.account_kind,
  a.entity_category,
  link.identity_id AS current_identity_id,
  link.link_state AS current_link_state,
  link.link_reason AS current_link_reason,
  current_identity.display_name AS current_identity_display_name,
  current_identity.primary_email AS current_identity_primary_email,
  candidate.display_name AS candidate_display_name,
  candidate.primary_email AS candidate_primary_email,
  candidate.kind AS candidate_kind,
  candidate.resolution_state AS candidate_resolution_state,
  candidate.identity_kind AS candidate_identity_kind,
  provisional.display_name AS provisional_display_name,
  provisional.primary_email AS provisional_primary_email,
  provisional.resolution_state AS provisional_resolution_state,
  (
    SELECT count(*)::bigint
    FROM identity_link_evidence evidence
    WHERE evidence.candidate_id = imc.id
  ) AS evidence_count,
  (
    SELECT count(*)::bigint
    FROM account_identity_relationships rel
    WHERE rel.account_id = imc.account_id
      AND rel.lifecycle_state = 'active'
  ) AS relationship_count
FROM identity_match_candidates imc
JOIN accounts a ON a.id = imc.account_id
JOIN identities candidate ON candidate.id = imc.candidate_identity_id
LEFT JOIN identity_accounts link ON link.account_id = imc.account_id
LEFT JOIN identities current_identity ON current_identity.id = link.identity_id
LEFT JOIN identities provisional ON provisional.id = imc.provisional_identity_id
WHERE imc.status = COALESCE(NULLIF(trim(sqlc.arg(status)::text), ''), 'pending')
  AND (
    COALESCE(NULLIF(trim(sqlc.arg(confidence_band)::text), ''), '') = ''
    OR imc.confidence_band = trim(sqlc.arg(confidence_band)::text)
  )
  AND (
    COALESCE(NULLIF(trim(sqlc.arg(match_reason)::text), ''), '') = ''
    OR imc.match_reason = trim(sqlc.arg(match_reason)::text)
  )
ORDER BY imc.created_at ASC, imc.id ASC
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: GetIdentityMatchCandidateDetailByID :one
SELECT
  imc.id,
  imc.account_id,
  imc.candidate_identity_id,
  imc.provisional_identity_id,
  imc.status,
  imc.confidence_band,
  imc.score,
  imc.match_reason,
  imc.ambiguity_key,
  imc.resolver_version,
  imc.resolver_fingerprint,
  imc.created_at,
  imc.updated_at,
  a.source_kind AS account_source_kind,
  a.source_name AS account_source_name,
  a.external_id AS account_external_id,
  a.email AS account_email,
  a.display_name AS account_display_name,
  a.account_kind,
  a.entity_category,
  link.identity_id AS current_identity_id,
  link.link_state AS current_link_state,
  link.link_reason AS current_link_reason,
  current_identity.display_name AS current_identity_display_name,
  current_identity.primary_email AS current_identity_primary_email,
  candidate.display_name AS candidate_display_name,
  candidate.primary_email AS candidate_primary_email,
  candidate.kind AS candidate_kind,
  candidate.resolution_state AS candidate_resolution_state,
  candidate.identity_kind AS candidate_identity_kind,
  provisional.display_name AS provisional_display_name,
  provisional.primary_email AS provisional_primary_email,
  provisional.resolution_state AS provisional_resolution_state,
  (
    SELECT count(*)::bigint
    FROM identity_link_evidence evidence
    WHERE evidence.candidate_id = imc.id
  ) AS evidence_count
FROM identity_match_candidates imc
JOIN accounts a ON a.id = imc.account_id
JOIN identities candidate ON candidate.id = imc.candidate_identity_id
LEFT JOIN identity_accounts link ON link.account_id = imc.account_id
LEFT JOIN identities current_identity ON current_identity.id = link.identity_id
LEFT JOIN identities provisional ON provisional.id = imc.provisional_identity_id
WHERE imc.id = $1;

-- name: ListPendingIdentityMatchCandidates :many
SELECT *
FROM identity_match_candidates
WHERE status = 'pending'
ORDER BY created_at ASC, id ASC
LIMIT sqlc.arg(page_limit)::int
OFFSET sqlc.arg(page_offset)::int;

-- name: GetIdentityMatchCandidate :one
SELECT *
FROM identity_match_candidates
WHERE id = $1;

-- name: GetIdentityMatchCandidateForUpdate :one
SELECT *
FROM identity_match_candidates
WHERE id = $1
FOR UPDATE;

-- name: GetAccountForIdentityResolutionUpdate :one
SELECT *
FROM accounts
WHERE id = $1
FOR UPDATE;

-- name: GetIdentityForIdentityResolutionUpdate :one
SELECT *
FROM identities
WHERE id = $1
FOR UPDATE;

-- name: UpdateAccountIdentityResolutionClassification :one
UPDATE accounts
SET
  account_kind = CASE
    WHEN lower(trim(sqlc.arg(account_kind)::text)) IN ('human', 'service', 'bot', 'unknown')
      THEN lower(trim(sqlc.arg(account_kind)::text))
    ELSE accounts.account_kind
  END,
  entity_category = CASE
    WHEN lower(trim(sqlc.arg(entity_category)::text)) IN ('user', 'group', 'service_principal', 'service_account', 'team', 'role', 'auth_role', 'entity', 'unknown')
      THEN lower(trim(sqlc.arg(entity_category)::text))
    ELSE accounts.entity_category
  END,
  updated_at = now()
WHERE id = sqlc.arg(account_id)::bigint
RETURNING *;

-- name: UpdateIdentityResolutionClassification :one
UPDATE identities
SET
  kind = CASE
    WHEN lower(trim(sqlc.arg(kind)::text)) IN ('human', 'service', 'bot', 'unknown')
      THEN lower(trim(sqlc.arg(kind)::text))
    ELSE identities.kind
  END,
  identity_kind = CASE
    WHEN lower(trim(sqlc.arg(identity_kind)::text)) IN ('human', 'service', 'shared', 'application', 'bot', 'unknown')
      THEN lower(trim(sqlc.arg(identity_kind)::text))
    ELSE identities.identity_kind
  END,
  resolution_state = COALESCE(NULLIF(trim(sqlc.arg(resolution_state)::text), ''), identities.resolution_state),
  updated_at = now()
WHERE id = sqlc.arg(identity_id)::bigint
RETURNING *;

-- name: AcceptIdentityMatchCandidate :exec
UPDATE identity_match_candidates
SET
  status = 'accepted',
  reviewed_by = NULLIF(trim(sqlc.narg(reviewed_by)::text), ''),
  reviewed_at = now(),
  review_note = COALESCE(sqlc.narg(review_note)::text, review_note),
  updated_at = now()
WHERE id = sqlc.arg(id)::bigint
  AND status = 'pending';

-- name: RejectIdentityMatchCandidate :exec
UPDATE identity_match_candidates
SET
  status = 'rejected',
  reviewed_by = NULLIF(trim(sqlc.narg(reviewed_by)::text), ''),
  reviewed_at = now(),
  review_note = COALESCE(sqlc.narg(review_note)::text, review_note),
  updated_at = now()
WHERE id = sqlc.arg(id)::bigint
  AND status = 'pending';

-- name: SupersedeCompetingIdentityMatchCandidates :exec
UPDATE identity_match_candidates
SET
  status = 'superseded',
  updated_at = now()
WHERE account_id = sqlc.arg(account_id)::bigint
  AND status = 'pending'
  AND id <> sqlc.arg(accepted_candidate_id)::bigint;

-- name: RejectPendingIdentityMatchCandidatesForAccount :exec
UPDATE identity_match_candidates
SET
  status = 'rejected',
  reviewed_by = NULLIF(trim(sqlc.narg(reviewed_by)::text), ''),
  reviewed_at = now(),
  review_note = COALESCE(sqlc.narg(review_note)::text, review_note),
  updated_at = now()
WHERE account_id = sqlc.arg(account_id)::bigint
  AND status = 'pending';

-- name: UpsertCandidateLinkEvidence :one
-- Records evidence attached to a pending review candidate. On replay the
-- existing row's observed_at and metadata are refreshed; duplicate evidence
-- rows are prevented by identity_link_evidence_candidate_unique.
INSERT INTO identity_link_evidence (
  account_id,
  identity_id,
  candidate_id,
  evidence_type,
  evidence_key,
  account_value,
  identity_value,
  source_kind,
  source_name,
  strength,
  is_positive,
  metadata,
  observed_at
)
VALUES (
  sqlc.arg(account_id)::bigint,
  sqlc.narg(identity_id)::bigint,
  sqlc.arg(candidate_id)::bigint,
  sqlc.arg(evidence_type)::text,
  sqlc.arg(evidence_key)::text,
  sqlc.narg(account_value)::text,
  sqlc.narg(identity_value)::text,
  sqlc.narg(source_kind)::text,
  sqlc.narg(source_name)::text,
  sqlc.arg(strength)::int,
  sqlc.arg(is_positive)::boolean,
  COALESCE(sqlc.narg(metadata)::jsonb, '{}'::jsonb),
  now()
)
ON CONFLICT (account_id, candidate_id, evidence_type, evidence_key)
  WHERE candidate_id IS NOT NULL
DO UPDATE SET
  identity_id   = COALESCE(EXCLUDED.identity_id, identity_link_evidence.identity_id),
  account_value = COALESCE(EXCLUDED.account_value, identity_link_evidence.account_value),
  identity_value = COALESCE(EXCLUDED.identity_value, identity_link_evidence.identity_value),
  source_kind   = COALESCE(EXCLUDED.source_kind, identity_link_evidence.source_kind),
  source_name   = COALESCE(EXCLUDED.source_name, identity_link_evidence.source_name),
  strength      = EXCLUDED.strength,
  is_positive   = EXCLUDED.is_positive,
  metadata      = EXCLUDED.metadata,
  observed_at   = EXCLUDED.observed_at
RETURNING *;

-- name: UpsertIdentityLinkEvidence :one
-- Records evidence attached to an accepted account-to-identity link (no
-- candidate row). Duplicate evidence rows are prevented by
-- identity_link_evidence_identity_unique.
INSERT INTO identity_link_evidence (
  account_id,
  identity_id,
  candidate_id,
  evidence_type,
  evidence_key,
  account_value,
  identity_value,
  source_kind,
  source_name,
  strength,
  is_positive,
  metadata,
  observed_at
)
VALUES (
  sqlc.arg(account_id)::bigint,
  sqlc.arg(identity_id)::bigint,
  NULL,
  sqlc.arg(evidence_type)::text,
  sqlc.arg(evidence_key)::text,
  sqlc.narg(account_value)::text,
  sqlc.narg(identity_value)::text,
  sqlc.narg(source_kind)::text,
  sqlc.narg(source_name)::text,
  sqlc.arg(strength)::int,
  sqlc.arg(is_positive)::boolean,
  COALESCE(sqlc.narg(metadata)::jsonb, '{}'::jsonb),
  now()
)
ON CONFLICT (account_id, identity_id, evidence_type, evidence_key)
  WHERE candidate_id IS NULL AND identity_id IS NOT NULL
DO UPDATE SET
  account_value = COALESCE(EXCLUDED.account_value, identity_link_evidence.account_value),
  identity_value = COALESCE(EXCLUDED.identity_value, identity_link_evidence.identity_value),
  source_kind   = COALESCE(EXCLUDED.source_kind, identity_link_evidence.source_kind),
  source_name   = COALESCE(EXCLUDED.source_name, identity_link_evidence.source_name),
  strength      = EXCLUDED.strength,
  is_positive   = EXCLUDED.is_positive,
  metadata      = EXCLUDED.metadata,
  observed_at   = EXCLUDED.observed_at
RETURNING *;

-- name: ListIdentityLinkEvidenceForCandidate :many
SELECT *
FROM identity_link_evidence
WHERE candidate_id = $1
ORDER BY created_at ASC, id ASC;

-- name: ListIdentityLinkEvidenceForCandidateIDs :many
SELECT *
FROM identity_link_evidence
WHERE candidate_id = ANY(sqlc.arg(candidate_ids)::bigint[])
ORDER BY candidate_id ASC, created_at ASC, id ASC;

-- Identity link evidence: collapse any pre-existing duplicate observations
-- before installing the partial UNIQUE indexes that the upsert paths rely on.
-- Within each (account_id, evidence_type, evidence_key) bucket scoped to a
-- single candidate (or a single identity, for accepted-link evidence), the
-- most recent observation wins.
DELETE FROM identity_link_evidence le
USING identity_link_evidence newer
WHERE le.account_id = newer.account_id
  AND le.evidence_type = newer.evidence_type
  AND le.evidence_key = newer.evidence_key
  AND le.candidate_id IS NOT NULL
  AND le.candidate_id = newer.candidate_id
  AND (newer.observed_at, newer.id) > (le.observed_at, le.id);

DELETE FROM identity_link_evidence le
USING identity_link_evidence newer
WHERE le.account_id = newer.account_id
  AND le.evidence_type = newer.evidence_type
  AND le.evidence_key = newer.evidence_key
  AND le.candidate_id IS NULL
  AND newer.candidate_id IS NULL
  AND le.identity_id IS NOT NULL
  AND le.identity_id = newer.identity_id
  AND (newer.observed_at, newer.id) > (le.observed_at, le.id);

CREATE UNIQUE INDEX IF NOT EXISTS identity_link_evidence_candidate_unique
  ON identity_link_evidence(account_id, candidate_id, evidence_type, evidence_key)
  WHERE candidate_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS identity_link_evidence_identity_unique
  ON identity_link_evidence(account_id, identity_id, evidence_type, evidence_key)
  WHERE candidate_id IS NULL AND identity_id IS NOT NULL;

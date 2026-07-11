-- name: UpsertSaaSAppReviewGovernance :one
INSERT INTO governance_subject_overrides (
  subject_kind,
  subject_id,
  governance_state,
  owner_identity_id,
  ticket_ref,
  notes,
  review_disposition,
  review_owner_identity_id,
  follow_up_due_date,
  replacement_saas_app_id,
  updated_by_auth_user_id,
  updated_at
)
VALUES (
  'saas_app',
  sqlc.arg(saas_app_id)::bigint,
  CASE sqlc.arg(review_disposition)::text
    WHEN 'under_review' THEN 'in_review'
    WHEN 'sanctioned' THEN 'approved'
    WHEN 'tolerated' THEN 'approved'
    WHEN 'replace' THEN 'action_required'
    ELSE 'unreviewed'
  END,
  sqlc.narg(owner_identity_id)::bigint,
  sqlc.arg(ticket_ref)::text,
  sqlc.arg(notes)::text,
  sqlc.arg(review_disposition)::text,
  sqlc.narg(review_owner_identity_id)::bigint,
  sqlc.narg(follow_up_due_date)::date,
  sqlc.narg(replacement_saas_app_id)::bigint,
  sqlc.narg(updated_by_auth_user_id)::bigint,
  now()
)
ON CONFLICT (subject_kind, subject_id) DO UPDATE SET
  governance_state = EXCLUDED.governance_state,
  owner_identity_id = EXCLUDED.owner_identity_id,
  ticket_ref = EXCLUDED.ticket_ref,
  notes = EXCLUDED.notes,
  review_disposition = EXCLUDED.review_disposition,
  review_owner_identity_id = EXCLUDED.review_owner_identity_id,
  follow_up_due_date = EXCLUDED.follow_up_due_date,
  replacement_saas_app_id = EXCLUDED.replacement_saas_app_id,
  updated_by_auth_user_id = EXCLUDED.updated_by_auth_user_id,
  updated_at = now()
RETURNING *;

-- name: InsertSaaSAppReviewDecision :exec
INSERT INTO saas_app_review_decisions (
  saas_app_id,
  owner_identity_id,
  review_owner_identity_id,
  review_disposition,
  ticket_ref,
  notes,
  follow_up_due_date,
  replacement_saas_app_id,
  changed_by_auth_user_id,
  changed_at
)
VALUES (
  sqlc.arg(saas_app_id)::bigint,
  sqlc.narg(owner_identity_id)::bigint,
  sqlc.narg(review_owner_identity_id)::bigint,
  sqlc.arg(review_disposition)::text,
  sqlc.arg(ticket_ref)::text,
  sqlc.arg(notes)::text,
  sqlc.narg(follow_up_due_date)::date,
  sqlc.narg(replacement_saas_app_id)::bigint,
  sqlc.narg(changed_by_auth_user_id)::bigint,
  now()
);

-- name: InsertSaaSAppReviewDecisionIfChanged :execrows
INSERT INTO saas_app_review_decisions (
  saas_app_id,
  owner_identity_id,
  review_owner_identity_id,
  review_disposition,
  ticket_ref,
  notes,
  follow_up_due_date,
  replacement_saas_app_id,
  changed_by_auth_user_id,
  changed_at
)
SELECT
  sqlc.arg(saas_app_id)::bigint,
  sqlc.narg(owner_identity_id)::bigint,
  sqlc.narg(review_owner_identity_id)::bigint,
  sqlc.arg(review_disposition)::text,
  sqlc.arg(ticket_ref)::text,
  sqlc.arg(notes)::text,
  sqlc.narg(follow_up_due_date)::date,
  sqlc.narg(replacement_saas_app_id)::bigint,
  sqlc.narg(changed_by_auth_user_id)::bigint,
  now()
WHERE NOT EXISTS (
  SELECT 1
  FROM (
    SELECT
      d.owner_identity_id,
      d.review_owner_identity_id,
      d.review_disposition,
      d.ticket_ref,
      d.notes,
      d.follow_up_due_date,
      d.replacement_saas_app_id,
      d.changed_by_auth_user_id
    FROM saas_app_review_decisions d
    WHERE d.saas_app_id = sqlc.arg(saas_app_id)::bigint
    ORDER BY d.changed_at DESC, d.id DESC
    LIMIT 1
  ) latest
  WHERE latest.owner_identity_id IS NOT DISTINCT FROM sqlc.narg(owner_identity_id)::bigint
    AND latest.review_owner_identity_id IS NOT DISTINCT FROM sqlc.narg(review_owner_identity_id)::bigint
    AND latest.review_disposition = sqlc.arg(review_disposition)::text
    AND latest.ticket_ref = sqlc.arg(ticket_ref)::text
    AND latest.notes = sqlc.arg(notes)::text
    AND latest.follow_up_due_date IS NOT DISTINCT FROM sqlc.narg(follow_up_due_date)::date
    AND latest.replacement_saas_app_id IS NOT DISTINCT FROM sqlc.narg(replacement_saas_app_id)::bigint
    AND latest.changed_by_auth_user_id IS NOT DISTINCT FROM sqlc.narg(changed_by_auth_user_id)::bigint
);

-- name: ListSaaSAppReviewDecisionsBySaaSAppID :many
SELECT
  d.id::bigint AS id,
  d.changed_at::timestamptz AS changed_at,
  COALESCE(changed_by.email, '')::text AS changed_by_auth_user_email,
  COALESCE(owner.display_name, '')::text AS owner_display_name,
  COALESCE(owner.primary_email, '')::text AS owner_primary_email,
  COALESCE(review_owner.display_name, '')::text AS review_owner_display_name,
  COALESCE(review_owner.primary_email, '')::text AS review_owner_primary_email,
  d.review_disposition::text AS review_disposition,
  d.ticket_ref::text AS ticket_ref,
  d.notes::text AS notes,
  d.follow_up_due_date::date AS follow_up_due_date,
  COALESCE(d.replacement_saas_app_id, 0)::bigint AS replacement_saas_app_id,
  COALESCE(NULLIF(trim(replacement.display_name), ''), replacement.canonical_key, '')::text AS replacement_display_name,
  COALESCE(replacement.primary_domain, '')::text AS replacement_primary_domain
FROM saas_app_review_decisions d
LEFT JOIN auth_users changed_by
  ON changed_by.id = d.changed_by_auth_user_id
LEFT JOIN identities owner
  ON owner.id = d.owner_identity_id
LEFT JOIN identities review_owner
  ON review_owner.id = d.review_owner_identity_id
LEFT JOIN saas_apps replacement
  ON replacement.id = d.replacement_saas_app_id
WHERE d.saas_app_id = sqlc.arg(saas_app_id)::bigint
ORDER BY d.changed_at DESC, d.id DESC
LIMIT sqlc.arg(limit_rows)::int;

-- name: SearchManagedReplacementSaaSApps :many
SELECT
  pr.id::bigint AS id,
  COALESCE(NULLIF(trim(pr.display_name), ''), pr.canonical_key)::text AS display_name,
  COALESCE(pr.primary_domain, '')::text AS primary_domain,
  COALESCE(pr.vendor_name, '')::text AS vendor_name,
  pr.managed_state::text AS managed_state,
  pr.risk_level::text AS risk_level
FROM discovery_app_read_models_v pr
WHERE pr.id <> sqlc.arg(exclude_id)::bigint
  AND pr.managed_state = 'managed'
  AND EXISTS (
    SELECT 1
    FROM saas_app_sources sas
    JOIN connector_source_state css
      ON lower(trim(css.source_kind)) = lower(trim(sas.source_kind))
     AND lower(trim(css.source_name)) = lower(trim(sas.source_name))
    WHERE sas.saas_app_id = pr.id
      AND sas.expired_at IS NULL
      AND sas.last_observed_run_id IS NOT NULL
      AND css.configured
      AND css.discovery_enabled
  )
  AND (
    sqlc.arg(query)::text = ''
    OR pr.display_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR pr.primary_domain ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR pr.vendor_name ILIKE ('%' || sqlc.arg(query)::text || '%')
    OR pr.canonical_key ILIKE ('%' || sqlc.arg(query)::text || '%')
  )
ORDER BY
  CASE
    WHEN lower(COALESCE(NULLIF(trim(pr.display_name), ''), pr.canonical_key)) = lower(trim(sqlc.arg(query)::text))
      OR lower(COALESCE(pr.primary_domain, '')) = lower(trim(sqlc.arg(query)::text))
      OR lower(COALESCE(pr.vendor_name, '')) = lower(trim(sqlc.arg(query)::text))
      OR lower(pr.canonical_key) = lower(trim(sqlc.arg(query)::text))
    THEN 0
    WHEN lower(COALESCE(NULLIF(trim(pr.display_name), ''), pr.canonical_key)) LIKE lower(trim(sqlc.arg(query)::text)) || '%'
      OR lower(COALESCE(pr.primary_domain, '')) LIKE lower(trim(sqlc.arg(query)::text)) || '%'
      OR lower(COALESCE(pr.vendor_name, '')) LIKE lower(trim(sqlc.arg(query)::text)) || '%'
      OR lower(pr.canonical_key) LIKE lower(trim(sqlc.arg(query)::text)) || '%'
    THEN 1
    ELSE 2
  END ASC,
  pr.risk_score DESC,
  pr.last_seen_at DESC,
  lower(COALESCE(NULLIF(trim(pr.display_name), ''), pr.canonical_key)) ASC,
  pr.id ASC
LIMIT sqlc.arg(limit_rows)::int;

-- name: GetSaaSAppReplacementCandidateByID :one
SELECT
  pr.id::bigint AS id,
  COALESCE(NULLIF(trim(pr.display_name), ''), pr.canonical_key)::text AS display_name,
  COALESCE(pr.primary_domain, '')::text AS primary_domain,
  COALESCE(pr.vendor_name, '')::text AS vendor_name,
  pr.managed_state::text AS managed_state,
  pr.risk_level::text AS risk_level
FROM discovery_app_read_models_v pr
WHERE pr.id = sqlc.arg(id)::bigint
  AND pr.managed_state = 'managed'
  AND EXISTS (
    SELECT 1
    FROM saas_app_sources sas
    JOIN connector_source_state css
      ON lower(trim(css.source_kind)) = lower(trim(sas.source_kind))
     AND lower(trim(css.source_name)) = lower(trim(sas.source_name))
    WHERE sas.saas_app_id = pr.id
      AND sas.expired_at IS NULL
      AND sas.last_observed_run_id IS NOT NULL
      AND css.configured
      AND css.discovery_enabled
  );

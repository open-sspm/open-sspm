CREATE OR REPLACE FUNCTION credential_artifact_risk_level(
  status text,
  credential_kind text,
  expires_at_source timestamptz,
  last_used_at_source timestamptz,
  created_by_external_id text,
  approved_by_external_id text,
  evaluated_at timestamptz
)
RETURNS text
LANGUAGE sql
STABLE
AS $$
SELECT CASE
  WHEN expires_at_source IS NOT NULL
    AND expires_at_source < evaluated_at
    AND lower(COALESCE(NULLIF(trim(status), ''), 'active')) IN ('active', 'approved', 'pending_approval')
    THEN 'critical'
  WHEN expires_at_source IS NOT NULL
    AND expires_at_source < evaluated_at
    THEN 'high'
  WHEN lower(trim(COALESCE(credential_kind, ''))) IN ('entra_client_secret', 'github_deploy_key', 'github_pat_request', 'github_pat_fine_grained')
    AND trim(COALESCE(created_by_external_id, '')) = ''
    AND trim(COALESCE(approved_by_external_id, '')) = ''
    THEN 'critical'
  WHEN expires_at_source IS NOT NULL
    AND expires_at_source >= evaluated_at
    AND expires_at_source <= evaluated_at + make_interval(days => 7)
    THEN 'high'
  WHEN trim(COALESCE(created_by_external_id, '')) = ''
    THEN 'high'
  WHEN last_used_at_source IS NOT NULL
    AND last_used_at_source <= evaluated_at - make_interval(days => 90)
    THEN 'high'
  WHEN expires_at_source IS NOT NULL
    AND expires_at_source >= evaluated_at
    AND expires_at_source <= evaluated_at + make_interval(days => 30)
    THEN 'medium'
  ELSE 'low'
END;
$$;

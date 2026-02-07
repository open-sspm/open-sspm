-- Remove only the legacy Okta v1 ruleset now that open-sspm is v2-only.
-- Cascades remove related rules, overrides, results, and attestations.
DELETE FROM rulesets
WHERE key = 'cis.okta.idaas_stig.v1';

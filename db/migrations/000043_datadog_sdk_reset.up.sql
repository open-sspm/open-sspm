-- Datadog account rows are preserved across the SDK cutover.
-- The first SDK-backed sync updates existing rows in place via external_id-based upserts.
SELECT 1;

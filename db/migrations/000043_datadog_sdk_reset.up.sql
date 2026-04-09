-- Intentional no-op marker for the Datadog SDK cutover.
-- Datadog account rows remain stable because SDK-backed syncs still upsert by external_id.
-- The first SDK-backed sync may add service-account entitlements that the old integration silently missed.
SELECT 1;

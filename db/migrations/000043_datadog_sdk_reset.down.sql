-- Intentional no-op rollback marker for the Datadog SDK cutover.
-- Rolling back this migration preserves the marker only; it does not restore pre-SDK Datadog sync behavior.
-- Datadog account rows and any newly observed service-account entitlements remain data-driven sync state.
SELECT 1;

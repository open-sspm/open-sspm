ALTER TABLE saas_app_sources
  DROP CONSTRAINT IF EXISTS saas_app_sources_seen_in_run_id_fkey,
  DROP CONSTRAINT IF EXISTS saas_app_sources_last_observed_run_id_fkey,
  DROP CONSTRAINT IF EXISTS saas_app_sources_expired_run_id_fkey,
  ADD CONSTRAINT saas_app_sources_seen_in_run_id_fkey
    FOREIGN KEY (seen_in_run_id) REFERENCES sync_runs(id) ON DELETE SET NULL,
  ADD CONSTRAINT saas_app_sources_last_observed_run_id_fkey
    FOREIGN KEY (last_observed_run_id) REFERENCES sync_runs(id) ON DELETE SET NULL,
  ADD CONSTRAINT saas_app_sources_expired_run_id_fkey
    FOREIGN KEY (expired_run_id) REFERENCES sync_runs(id) ON DELETE SET NULL;

ALTER TABLE saas_app_events
  DROP CONSTRAINT IF EXISTS saas_app_events_seen_in_run_id_fkey,
  DROP CONSTRAINT IF EXISTS saas_app_events_last_observed_run_id_fkey,
  DROP CONSTRAINT IF EXISTS saas_app_events_expired_run_id_fkey,
  ADD CONSTRAINT saas_app_events_seen_in_run_id_fkey
    FOREIGN KEY (seen_in_run_id) REFERENCES sync_runs(id) ON DELETE SET NULL,
  ADD CONSTRAINT saas_app_events_last_observed_run_id_fkey
    FOREIGN KEY (last_observed_run_id) REFERENCES sync_runs(id) ON DELETE SET NULL,
  ADD CONSTRAINT saas_app_events_expired_run_id_fkey
    FOREIGN KEY (expired_run_id) REFERENCES sync_runs(id) ON DELETE SET NULL;

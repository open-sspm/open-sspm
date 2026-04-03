UPDATE sync_runs
SET
  status = 'canceled',
  finished_at = started_at,
  message = 'reclaimed stale running sync run during deployment cleanup',
  error_kind = 'stale_reclaimed'
WHERE status = 'running'
  AND started_at < now() - interval '72 hours';

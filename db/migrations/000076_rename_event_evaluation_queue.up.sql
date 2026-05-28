ALTER TABLE IF EXISTS riskpolicy_event_queue
  RENAME TO event_evaluation_queue;

ALTER TABLE IF EXISTS event_evaluation_queue
  RENAME CONSTRAINT riskpolicy_event_queue_status_check TO event_evaluation_queue_status_check;

ALTER INDEX IF EXISTS idx_riskpolicy_event_queue_claim
  RENAME TO idx_event_evaluation_queue_claim;

ALTER INDEX IF EXISTS idx_riskpolicy_event_queue_lease
  RENAME TO idx_event_evaluation_queue_lease;

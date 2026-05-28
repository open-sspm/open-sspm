DELETE FROM finding_events
WHERE finding_key LIKE 'riskpolicy_event:%';

DELETE FROM findings
WHERE finding_key LIKE 'riskpolicy_event:%';

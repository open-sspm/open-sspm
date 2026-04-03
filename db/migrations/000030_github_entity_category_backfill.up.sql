-- Repair legacy GitHub account rows that predate entity_category writes.
UPDATE accounts
SET entity_category = CASE
  WHEN lower(trim(external_id)) LIKE 'team:%'
    OR NULLIF(trim(COALESCE(raw_json ->> 'slug', '')), '') IS NOT NULL
    THEN 'team'
  ELSE 'user'
END
WHERE lower(trim(source_kind)) = 'github'
  AND lower(trim(entity_category)) = 'unknown';

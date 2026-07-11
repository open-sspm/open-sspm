ALTER TABLE rules
  ADD COLUMN required_data JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN expected_params JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN rule_version TEXT NOT NULL DEFAULT '';

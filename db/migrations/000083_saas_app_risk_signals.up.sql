ALTER TABLE saas_app_risk_read_models
  ADD COLUMN IF NOT EXISTS risk_signals_json JSONB NOT NULL DEFAULT '[]'::jsonb;

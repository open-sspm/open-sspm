-- Rules engine schema for Findings (benchmarks/rules/results/attestations)

CREATE TABLE IF NOT EXISTS rulesets (
  id BIGSERIAL PRIMARY KEY,
  key TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL DEFAULT '',
  description TEXT NOT NULL DEFAULT '',
  source TEXT NOT NULL DEFAULT '',
  source_version TEXT NOT NULL DEFAULT '',
  source_date DATE,
  scope_kind TEXT NOT NULL,
  connector_kind TEXT,
  status TEXT NOT NULL DEFAULT 'active',
  definition_hash TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (scope_kind IN ('connector_instance', 'global')),
  CHECK (status IN ('active', 'deprecated')),
  CHECK (
    (scope_kind = 'global' AND connector_kind IS NULL)
    OR (scope_kind = 'connector_instance' AND connector_kind IS NOT NULL AND connector_kind <> '')
  )
);

CREATE TABLE IF NOT EXISTS rules (
  id BIGSERIAL PRIMARY KEY,
  ruleset_id BIGINT NOT NULL REFERENCES rulesets(id) ON DELETE CASCADE,
  key TEXT NOT NULL,
  title TEXT NOT NULL DEFAULT '',
  summary TEXT NOT NULL DEFAULT '',
  category TEXT NOT NULL DEFAULT '',
  severity TEXT NOT NULL DEFAULT '',
  monitoring_status TEXT NOT NULL DEFAULT 'unsupported',
  monitoring_reason TEXT NOT NULL DEFAULT '',
  required_data JSONB NOT NULL DEFAULT '[]'::jsonb,
  expected_params JSONB NOT NULL DEFAULT '{}'::jsonb,
  rule_version TEXT NOT NULL DEFAULT '',
  is_active BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (ruleset_id, key),
  CHECK (monitoring_status IN ('automated', 'partial', 'manual', 'unsupported'))
);

CREATE INDEX IF NOT EXISTS idx_rules_ruleset_id ON rules (ruleset_id);
CREATE INDEX IF NOT EXISTS idx_rules_key ON rules (key);

CREATE TABLE IF NOT EXISTS ruleset_overrides (
  id BIGSERIAL PRIMARY KEY,
  ruleset_id BIGINT NOT NULL REFERENCES rulesets(id) ON DELETE CASCADE,
  scope_kind TEXT NOT NULL,
  source_kind TEXT NOT NULL DEFAULT '',
  source_name TEXT NOT NULL DEFAULT '',
  enabled BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (ruleset_id, scope_kind, source_kind, source_name),
  CHECK (scope_kind IN ('connector_instance', 'global'))
);

CREATE INDEX IF NOT EXISTS idx_ruleset_overrides_ruleset_id ON ruleset_overrides (ruleset_id);

CREATE TABLE IF NOT EXISTS rule_overrides (
  id BIGSERIAL PRIMARY KEY,
  rule_id BIGINT NOT NULL REFERENCES rules(id) ON DELETE CASCADE,
  scope_kind TEXT NOT NULL,
  source_kind TEXT NOT NULL DEFAULT '',
  source_name TEXT NOT NULL DEFAULT '',
  params JSONB NOT NULL DEFAULT '{}'::jsonb,
  enabled BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (rule_id, scope_kind, source_kind, source_name),
  CHECK (scope_kind IN ('connector_instance', 'global'))
);

CREATE INDEX IF NOT EXISTS idx_rule_overrides_rule_id ON rule_overrides (rule_id);

CREATE TABLE IF NOT EXISTS rule_results_current (
  id BIGSERIAL PRIMARY KEY,
  rule_id BIGINT NOT NULL REFERENCES rules(id) ON DELETE CASCADE,
  scope_kind TEXT NOT NULL,
  source_kind TEXT NOT NULL DEFAULT '',
  source_name TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL,
  evaluated_at TIMESTAMPTZ,
  sync_run_id BIGINT REFERENCES sync_runs(id),
  evidence_summary TEXT NOT NULL DEFAULT '',
  evidence_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  affected_resource_ids TEXT[] NOT NULL DEFAULT '{}'::text[],
  error_kind TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (rule_id, scope_kind, source_kind, source_name),
  CHECK (scope_kind IN ('connector_instance', 'global')),
  CHECK (status IN ('pass', 'fail', 'unknown', 'not_applicable', 'error'))
);

CREATE INDEX IF NOT EXISTS idx_rule_results_current_scope
  ON rule_results_current (scope_kind, source_kind, source_name);

CREATE TABLE IF NOT EXISTS rule_evaluations (
  id BIGSERIAL PRIMARY KEY,
  rule_id BIGINT NOT NULL REFERENCES rules(id) ON DELETE CASCADE,
  scope_kind TEXT NOT NULL,
  source_kind TEXT NOT NULL DEFAULT '',
  source_name TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL,
  sync_run_id BIGINT REFERENCES sync_runs(id),
  evaluated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  evidence_summary TEXT NOT NULL DEFAULT '',
  evidence_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  affected_resource_ids TEXT[] NOT NULL DEFAULT '{}'::text[],
  error_kind TEXT NOT NULL DEFAULT '',
  CHECK (scope_kind IN ('connector_instance', 'global')),
  CHECK (status IN ('pass', 'fail', 'unknown', 'not_applicable', 'error'))
);

CREATE INDEX IF NOT EXISTS idx_rule_evaluations_rule_scope_evaluated_at_desc
  ON rule_evaluations (rule_id, scope_kind, source_kind, source_name, evaluated_at DESC);

CREATE TABLE IF NOT EXISTS rule_attestations (
  id BIGSERIAL PRIMARY KEY,
  rule_id BIGINT NOT NULL REFERENCES rules(id) ON DELETE CASCADE,
  scope_kind TEXT NOT NULL,
  source_kind TEXT NOT NULL DEFAULT '',
  source_name TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL,
  notes TEXT NOT NULL DEFAULT '',
  expires_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (rule_id, scope_kind, source_kind, source_name),
  CHECK (scope_kind IN ('connector_instance', 'global')),
  CHECK (status IN ('pass', 'fail', 'not_applicable'))
);

CREATE INDEX IF NOT EXISTS idx_rule_attestations_rule_id ON rule_attestations (rule_id);


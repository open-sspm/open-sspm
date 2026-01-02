-- Okta app assignments and group mappings

CREATE TABLE IF NOT EXISTS okta_groups (
  id BIGSERIAL PRIMARY KEY,
  external_id TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL DEFAULT '',
  type TEXT NOT NULL DEFAULT '',
  raw_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_okta_groups_name ON okta_groups (name);

CREATE TABLE IF NOT EXISTS okta_user_groups (
  id BIGSERIAL PRIMARY KEY,
  idp_user_id BIGINT NOT NULL REFERENCES idp_users(id) ON DELETE CASCADE,
  okta_group_id BIGINT NOT NULL REFERENCES okta_groups(id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (idp_user_id, okta_group_id)
);

CREATE INDEX IF NOT EXISTS idx_okta_user_groups_user ON okta_user_groups (idp_user_id);
CREATE INDEX IF NOT EXISTS idx_okta_user_groups_group ON okta_user_groups (okta_group_id);

CREATE TABLE IF NOT EXISTS okta_apps (
  id BIGSERIAL PRIMARY KEY,
  external_id TEXT NOT NULL UNIQUE,
  label TEXT NOT NULL DEFAULT '',
  name TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT '',
  sign_on_mode TEXT NOT NULL DEFAULT '',
  raw_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_okta_apps_label ON okta_apps (label);

CREATE TABLE IF NOT EXISTS okta_user_app_assignments (
  id BIGSERIAL PRIMARY KEY,
  idp_user_id BIGINT NOT NULL REFERENCES idp_users(id) ON DELETE CASCADE,
  okta_app_id BIGINT NOT NULL REFERENCES okta_apps(id) ON DELETE CASCADE,
  scope TEXT NOT NULL DEFAULT '',
  profile_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (idp_user_id, okta_app_id)
);

CREATE INDEX IF NOT EXISTS idx_okta_user_app_assignments_user ON okta_user_app_assignments (idp_user_id);
CREATE INDEX IF NOT EXISTS idx_okta_user_app_assignments_app ON okta_user_app_assignments (okta_app_id);

CREATE TABLE IF NOT EXISTS okta_app_group_assignments (
  id BIGSERIAL PRIMARY KEY,
  okta_app_id BIGINT NOT NULL REFERENCES okta_apps(id) ON DELETE CASCADE,
  okta_group_id BIGINT NOT NULL REFERENCES okta_groups(id) ON DELETE CASCADE,
  priority INTEGER NOT NULL DEFAULT 0,
  profile_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (okta_app_id, okta_group_id)
);

CREATE INDEX IF NOT EXISTS idx_okta_app_group_assignments_app ON okta_app_group_assignments (okta_app_id);
CREATE INDEX IF NOT EXISTS idx_okta_app_group_assignments_group ON okta_app_group_assignments (okta_group_id);

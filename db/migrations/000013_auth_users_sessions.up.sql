-- UI authentication users + server-side sessions

CREATE TABLE IF NOT EXISTS auth_users (
  id BIGSERIAL PRIMARY KEY,
  email TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT 'viewer',
  is_active BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_login_at TIMESTAMPTZ,
  last_login_ip TEXT NOT NULL DEFAULT '',
  CONSTRAINT auth_users_email_nonempty CHECK (email <> ''),
  CONSTRAINT auth_users_role_check CHECK (role IN ('admin', 'viewer'))
);

-- alexedwards/scs session store schema
CREATE TABLE IF NOT EXISTS sessions (
  token TEXT PRIMARY KEY,
  data BYTEA NOT NULL,
  expiry TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS sessions_expiry_idx ON sessions (expiry);


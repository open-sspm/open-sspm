CREATE TABLE IF NOT EXISTS connector_secrets (
  kind TEXT NOT NULL REFERENCES connector_configs(kind) ON DELETE CASCADE,
  secret_name TEXT NOT NULL,
  ciphertext BYTEA NOT NULL,
  nonce BYTEA NOT NULL,
  version SMALLINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (kind, secret_name),
  CHECK (octet_length(nonce) = 12),
  CHECK (version > 0)
);

CREATE INDEX IF NOT EXISTS connector_secrets_kind_idx
ON connector_secrets (kind);

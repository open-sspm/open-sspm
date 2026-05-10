package config

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadWithOptions_InvalidSyncIntervalReturnsError(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("SYNC_INTERVAL", "not-a-duration")

	_, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err == nil {
		t.Fatalf("expected invalid duration error")
	}
}

func TestLoadWithOptions_RejectsNonPositiveConnectorInterval(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("SYNC_OKTA_INTERVAL", "0s")

	_, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err == nil {
		t.Fatalf("expected non-positive interval error")
	}
}

func TestLoadWithOptions_ParsesTrustedProxyCIDRs(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("TRUSTED_PROXY_CIDRS", "35.191.0.0/16, 130.211.0.0/22")

	cfg, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err != nil {
		t.Fatalf("LoadWithOptions() error = %v", err)
	}
	if got, want := len(cfg.TrustedProxyCIDRs), 2; got != want {
		t.Fatalf("len(TrustedProxyCIDRs) = %d, want %d", got, want)
	}
}

func TestLoadWithOptions_RejectsInvalidTrustedProxyCIDRs(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("TRUSTED_PROXY_CIDRS", "not-a-cidr")

	_, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err == nil {
		t.Fatalf("expected invalid cidr error")
	}
}

func TestLoadWithOptions_LoadsConnectorSecretKeyFromEnv(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("CONNECTOR_SECRET_KEY_FILE", "")
	t.Setenv("CONNECTOR_SECRET_KEY", base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef")))

	cfg, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err != nil {
		t.Fatalf("LoadWithOptions() error = %v", err)
	}
	if got := string(cfg.ConnectorSecretKey); got != "0123456789abcdef0123456789abcdef" {
		t.Fatalf("ConnectorSecretKey = %q, want %q", got, "0123456789abcdef0123456789abcdef")
	}
}

func TestLoadWithOptions_LoadsConnectorSecretKeyFromFile(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("CONNECTOR_SECRET_KEY", "")
	path := filepath.Join(t.TempDir(), "connector-key")
	if err := os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	t.Setenv("CONNECTOR_SECRET_KEY_FILE", path)

	cfg, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err != nil {
		t.Fatalf("LoadWithOptions() error = %v", err)
	}
	if got := string(cfg.ConnectorSecretKey); got != "0123456789abcdef0123456789abcdef" {
		t.Fatalf("ConnectorSecretKey = %q, want %q", got, "0123456789abcdef0123456789abcdef")
	}
}

func TestLoadWithOptions_RejectsConflictingConnectorSecretKeySources(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("CONNECTOR_SECRET_KEY", base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef")))
	t.Setenv("CONNECTOR_SECRET_KEY_FILE", filepath.Join(t.TempDir(), "connector-key"))

	_, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err == nil {
		t.Fatalf("expected conflicting connector secret key sources error")
	}
}

func TestLoadWithOptions_RejectsInvalidConnectorSecretKey(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("CONNECTOR_SECRET_KEY_FILE", "")
	t.Setenv("CONNECTOR_SECRET_KEY", "not-base64")

	_, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err == nil {
		t.Fatalf("expected invalid connector secret key error")
	}
}

func TestLoadWithOptions_RejectsInvalidStartupReadModelRebuildMode(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("STARTUP_READ_MODEL_REBUILD_MODE", "sometimes")

	_, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err == nil {
		t.Fatalf("expected invalid startup read model rebuild mode error")
	}
}

func TestLoadWithOptions_RejectsInvalidQueueBackend(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("QUEUE_BACKEND", "kafka")

	_, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err == nil {
		t.Fatalf("expected invalid queue backend error")
	}
}

func TestLoadWithOptions_NormalizesSMTPFromAddress(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("SMTP_ENABLED", "1")
	t.Setenv("SMTP_HOST", "smtp.example.com")
	t.Setenv("SMTP_FROM_ADDRESS", "Open SSPM <noreply@example.com>")
	t.Setenv("SMTP_FROM_NAME", "")

	cfg, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err != nil {
		t.Fatalf("LoadWithOptions() error = %v", err)
	}
	if got, want := cfg.SMTP.FromAddress, "noreply@example.com"; got != want {
		t.Fatalf("SMTP.FromAddress = %q, want %q", got, want)
	}
	if got, want := cfg.SMTP.FromName, "Open SSPM"; got != want {
		t.Fatalf("SMTP.FromName = %q, want %q", got, want)
	}
}

func TestLoadWithOptions_RejectsSMTPFromNameWithHeaderBreak(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("SMTP_ENABLED", "1")
	t.Setenv("SMTP_HOST", "smtp.example.com")
	t.Setenv("SMTP_FROM_ADDRESS", "noreply@example.com")
	t.Setenv("SMTP_FROM_NAME", "Open\r\nSSPM")

	_, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err == nil {
		t.Fatalf("expected SMTP from name validation error")
	}
}

func TestLoadWithOptions_PreservesSMTPPasswordBytes(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("SMTP_ENABLED", "1")
	t.Setenv("SMTP_HOST", "smtp.example.com")
	t.Setenv("SMTP_FROM_ADDRESS", "noreply@example.com")
	t.Setenv("SMTP_USERNAME", "mailer")
	t.Setenv("SMTP_PASSWORD", "secret\r\n")

	cfg, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err != nil {
		t.Fatalf("LoadWithOptions() error = %v", err)
	}
	if got, want := cfg.SMTP.Password, "secret\r\n"; got != want {
		t.Fatalf("SMTP.Password = %q, want %q", got, want)
	}
}

func TestLoadWithOptions_SMTPEnabledRequiresHost(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("SMTP_ENABLED", "1")
	t.Setenv("SMTP_HOST", "")
	t.Setenv("SMTP_FROM_ADDRESS", "noreply@example.com")

	_, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err == nil {
		t.Fatalf("expected SMTP host validation error")
	}
}

func TestLoadWithOptions_SMTPEnabledRejectsInvalidTLSMode(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("SMTP_ENABLED", "1")
	t.Setenv("SMTP_HOST", "smtp.example.com")
	t.Setenv("SMTP_FROM_ADDRESS", "noreply@example.com")
	t.Setenv("SMTP_TLS_MODE", "invalid")

	_, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err == nil {
		t.Fatalf("expected SMTP TLS mode validation error")
	}
}

func TestLoadWithOptions_SMTPEnabledRejectsPartialAuth(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("SMTP_ENABLED", "1")
	t.Setenv("SMTP_HOST", "smtp.example.com")
	t.Setenv("SMTP_FROM_ADDRESS", "noreply@example.com")
	t.Setenv("SMTP_USERNAME", "mailer")
	t.Setenv("SMTP_PASSWORD", "")

	_, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err == nil {
		t.Fatalf("expected SMTP auth validation error")
	}
}

func TestLoadWithOptions_SMTPEnabledRejectsPlainAuthForRemoteHost(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("SMTP_ENABLED", "1")
	t.Setenv("SMTP_HOST", "smtp.example.com")
	t.Setenv("SMTP_FROM_ADDRESS", "noreply@example.com")
	t.Setenv("SMTP_USERNAME", "mailer")
	t.Setenv("SMTP_PASSWORD", "secret")
	t.Setenv("SMTP_TLS_MODE", SMTPTLSModePlain)

	_, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err == nil {
		t.Fatalf("expected plain SMTP auth validation error")
	}
}

func TestLoadWithOptions_SMTPEnabledAllowsPlainAuthOnLocalhost(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("SMTP_ENABLED", "1")
	t.Setenv("SMTP_HOST", "localhost")
	t.Setenv("SMTP_FROM_ADDRESS", "noreply@example.com")
	t.Setenv("SMTP_USERNAME", "mailer")
	t.Setenv("SMTP_PASSWORD", "secret")
	t.Setenv("SMTP_TLS_MODE", SMTPTLSModePlain)

	cfg, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err != nil {
		t.Fatalf("LoadWithOptions() error = %v", err)
	}
	if got, want := cfg.SMTP.TLSMode, SMTPTLSModePlain; got != want {
		t.Fatalf("SMTP.TLSMode = %q, want %q", got, want)
	}
}

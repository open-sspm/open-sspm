package config

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

func TestLoadWithOptions_EventInboxDefaultsToEnabled(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("SYNC_DISCOVERY_ENABLED", "0")
	t.Setenv("EVENT_INBOX_ENABLED", "")

	cfg, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err != nil {
		t.Fatalf("LoadWithOptions() error = %v", err)
	}
	if cfg.SyncDiscoveryEnabled {
		t.Fatalf("SyncDiscoveryEnabled = true, want false")
	}
	if !cfg.EventInboxEnabled {
		t.Fatalf("EventInboxEnabled = false, want true by default")
	}
}

func TestLoadWithOptions_EventInboxFlagCanDisableIngest(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("EVENT_INBOX_ENABLED", "0")

	cfg, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err != nil {
		t.Fatalf("LoadWithOptions() error = %v", err)
	}
	if cfg.EventInboxEnabled {
		t.Fatalf("EventInboxEnabled = true, want explicit false")
	}
}

func TestLoadWithOptions_RejectsInvalidEventInboxEnabledFlag(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("EVENT_INBOX_ENABLED", "true")

	_, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err == nil {
		t.Fatal("expected invalid EVENT_INBOX_ENABLED error")
	}
	if !strings.Contains(err.Error(), "EVENT_INBOX_ENABLED must be 0 or 1") {
		t.Fatalf("error = %v, want EVENT_INBOX_ENABLED guidance", err)
	}
}

func TestLoadWithOptions_LoadsEventInboxConfig(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("EVENT_INBOX_BATCH_SIZE", "42")
	t.Setenv("EVENT_INBOX_POLL_INTERVAL", "3s")
	t.Setenv("EVENT_INBOX_CLEANUP_INTERVAL", "4m")
	t.Setenv("EVENT_INBOX_RETRY_DELAY", "5s")
	t.Setenv("EVENT_INBOX_RETRY_MAX_DELAY", "30s")
	t.Setenv("EVENT_INBOX_STALE_PROCESSING_TIMEOUT", "6m")
	t.Setenv("EVENT_INBOX_MAX_ATTEMPTS", "7")
	t.Setenv("EVENT_INBOX_PROCESSED_RETENTION_DAYS", "8")
	t.Setenv("EVENT_INBOX_DEAD_LETTER_RETENTION_DAYS", "9")

	cfg, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err != nil {
		t.Fatalf("LoadWithOptions() error = %v", err)
	}
	if got, want := cfg.EventInbox.BatchSize, int32(42); got != want {
		t.Fatalf("BatchSize = %d, want %d", got, want)
	}
	if got, want := cfg.EventInbox.MaxAttempts, int32(7); got != want {
		t.Fatalf("MaxAttempts = %d, want %d", got, want)
	}
	if got, want := cfg.EventInbox.DeadLetterRetentionDays, int32(9); got != want {
		t.Fatalf("DeadLetterRetentionDays = %d, want %d", got, want)
	}
}

func TestLoadWithOptions_LoadsRealtimeWorkerConfig(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("SYNC_TAIL_INTERVAL", "45s")
	t.Setenv("EVENT_EVALUATOR_WORKER_POLL_INTERVAL", "2s")
	t.Setenv("EVENT_EVALUATOR_WORKER_BATCH_SIZE", "37")
	t.Setenv("EVENT_EVALUATOR_WORKER_MAX_ATTEMPTS", "9")
	t.Setenv("EVENT_PARTITION_MAINTENANCE_INTERVAL", "6h")
	t.Setenv("EVENT_PARTITION_FUTURE_DAYS", "5")
	t.Setenv("EVENT_RETENTION_DAYS", "120")

	cfg, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err != nil {
		t.Fatalf("LoadWithOptions() error = %v", err)
	}
	if got, want := cfg.SyncTailInterval, 45*time.Second; got != want {
		t.Fatalf("SyncTailInterval = %s, want %s", got, want)
	}
	if got, want := cfg.EventEvaluatorWorker.PollInterval, 2*time.Second; got != want {
		t.Fatalf("EventEvaluatorWorker.PollInterval = %s, want %s", got, want)
	}
	if got, want := cfg.EventEvaluatorWorker.BatchSize, int32(37); got != want {
		t.Fatalf("EventEvaluatorWorker.BatchSize = %d, want %d", got, want)
	}
	if got, want := cfg.EventEvaluatorWorker.MaxAttempts, int32(9); got != want {
		t.Fatalf("EventEvaluatorWorker.MaxAttempts = %d, want %d", got, want)
	}
	if got, want := cfg.EventPartitions.MaintenanceInterval, 6*time.Hour; got != want {
		t.Fatalf("EventPartitions.MaintenanceInterval = %s, want %s", got, want)
	}
	if got, want := cfg.EventPartitions.FutureDays, int32(5); got != want {
		t.Fatalf("EventPartitions.FutureDays = %d, want %d", got, want)
	}
	if got, want := cfg.EventPartitions.RetentionDays, int32(120); got != want {
		t.Fatalf("EventPartitions.RetentionDays = %d, want %d", got, want)
	}
}

func TestLoadWithOptions_RejectsInvalidRealtimeWorkerConfig(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("EVENT_EVALUATOR_WORKER_BATCH_SIZE", "0")

	_, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err == nil {
		t.Fatalf("expected invalid evaluator worker batch size error")
	}
	if !strings.Contains(err.Error(), "EVENT_EVALUATOR_WORKER_BATCH_SIZE") {
		t.Fatalf("error = %v, want evaluator batch size guidance", err)
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

package config

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadWithOptions_DefaultSyncDiscoveryInterval(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("SYNC_DISCOVERY_INTERVAL", "")

	cfg, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err != nil {
		t.Fatalf("LoadWithOptions() error = %v", err)
	}
	if cfg.SyncDiscoveryInterval != defaultSyncDiscoveryInterval {
		t.Fatalf("SyncDiscoveryInterval = %s, want %s", cfg.SyncDiscoveryInterval, defaultSyncDiscoveryInterval)
	}
}

func TestLoadWithOptions_ParsesSyncDiscoveryInterval(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("SYNC_DISCOVERY_INTERVAL", "27m")

	cfg, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err != nil {
		t.Fatalf("LoadWithOptions() error = %v", err)
	}
	if cfg.SyncDiscoveryInterval.String() != "27m0s" {
		t.Fatalf("SyncDiscoveryInterval = %s, want %s", cfg.SyncDiscoveryInterval, "27m0s")
	}
}

func TestLoadWithOptions_DisablesDiscoveryLaneFromEnv(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("SYNC_DISCOVERY_ENABLED", "0")

	cfg, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err != nil {
		t.Fatalf("LoadWithOptions() error = %v", err)
	}
	if cfg.SyncDiscoveryEnabled {
		t.Fatalf("SyncDiscoveryEnabled = true, want false")
	}
}

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
	t.Setenv("CONNECTOR_SECRET_KEY", "not-base64")

	_, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err == nil {
		t.Fatalf("expected invalid connector secret key error")
	}
}

func TestLoadWithOptions_DefaultStartupReadModelRebuildMode(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("STARTUP_READ_MODEL_REBUILD_MODE", "")

	cfg, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err != nil {
		t.Fatalf("LoadWithOptions() error = %v", err)
	}
	if cfg.StartupReadModelRebuildMode != StartupReadModelRebuildAuto {
		t.Fatalf("StartupReadModelRebuildMode = %q, want %q", cfg.StartupReadModelRebuildMode, StartupReadModelRebuildAuto)
	}
}

func TestLoadWithOptions_ParsesStartupReadModelRebuildMode(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("STARTUP_READ_MODEL_REBUILD_MODE", StartupReadModelRebuildAlways)

	cfg, err := LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
	if err != nil {
		t.Fatalf("LoadWithOptions() error = %v", err)
	}
	if cfg.StartupReadModelRebuildMode != StartupReadModelRebuildAlways {
		t.Fatalf("StartupReadModelRebuildMode = %q, want %q", cfg.StartupReadModelRebuildMode, StartupReadModelRebuildAlways)
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

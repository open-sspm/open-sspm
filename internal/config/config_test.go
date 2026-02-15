package config

import "testing"

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

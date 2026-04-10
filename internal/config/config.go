package config

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

const (
	defaultHTTPAddr              = ":8080"
	defaultMetricsAddr           = ""
	defaultSyncInterval          = 15 * time.Minute
	defaultSyncDiscoveryInterval = 15 * time.Minute
	defaultStartupReadModelMode  = StartupReadModelRebuildAuto

	defaultSyncOktaWorkers    = 3
	defaultSyncGitHubWorkers  = 6
	defaultSyncDatadogWorkers = 3

	defaultSyncLockMode              = "lease"
	defaultSyncLockTTL               = 60 * time.Second
	defaultSyncLockHeartbeatInterval = 15 * time.Second
	defaultSyncLockHeartbeatTimeout  = 15 * time.Second
)

const (
	StartupReadModelRebuildAuto   = "auto"
	StartupReadModelRebuildAlways = "always"
)

type Config struct {
	DatabaseURL                 string
	ConnectorSecretKey          []byte
	HTTPAddr                    string
	MetricsAddr                 string
	StaticDir                   string
	AuthCookieSecure            bool
	TrustedProxyCIDRs           []string
	DevSeedAdmin                bool
	SyncDiscoveryEnabled        bool
	SyncInterval                time.Duration
	SyncDiscoveryInterval       time.Duration
	SyncOktaInterval            time.Duration
	SyncEntraInterval           time.Duration
	SyncGoogleWorkspaceInterval time.Duration
	SyncGitHubInterval          time.Duration
	SyncDatadogInterval         time.Duration
	SyncAWSInterval             time.Duration
	SyncFailureBackoffMax       time.Duration
	SyncOktaWorkers             int
	SyncGitHubWorkers           int
	SyncDatadogWorkers          int
	ResyncEnabled               bool
	ResyncMode                  string
	GlobalEvalMode              string
	SyncLockMode                string
	SyncLockTTL                 time.Duration
	SyncLockHeartbeatInterval   time.Duration
	SyncLockHeartbeatTimeout    time.Duration
	SyncLockInstanceID          string
	StartupReadModelRebuildMode string
}

type LoadOptions struct {
	RequireDatabaseURL bool
}

func Load() (Config, error) {
	return LoadWithOptions(LoadOptions{RequireDatabaseURL: true})
}

func LoadWithOptions(opts LoadOptions) (Config, error) {
	if err := godotenv.Load(); err != nil {
		if _, ok := errors.AsType[*os.PathError](err); !ok {
			return Config{}, err
		}
	}

	cfg := Config{
		DatabaseURL:           os.Getenv("DATABASE_URL"),
		HTTPAddr:              getenvDefault("HTTP_ADDR", defaultHTTPAddr),
		MetricsAddr:           defaultMetricsAddr,
		StaticDir:             strings.TrimSpace(os.Getenv("STATIC_DIR")),
		AuthCookieSecure:      getenvBoolDefault("AUTH_COOKIE_SECURE", false),
		TrustedProxyCIDRs:     splitCommaSeparated(os.Getenv("TRUSTED_PROXY_CIDRS")),
		DevSeedAdmin:          getenvBoolDefault("DEV_SEED_ADMIN", false),
		SyncDiscoveryEnabled:  getenvBoolDefault("SYNC_DISCOVERY_ENABLED", true),
		SyncInterval:          defaultSyncInterval,
		SyncDiscoveryInterval: defaultSyncDiscoveryInterval,
		SyncOktaWorkers:       getenvIntDefault("SYNC_OKTA_WORKERS", defaultSyncOktaWorkers),
		SyncGitHubWorkers:     getenvIntDefault("SYNC_GITHUB_WORKERS", defaultSyncGitHubWorkers),
		SyncDatadogWorkers:    getenvIntDefault("SYNC_DATADOG_WORKERS", defaultSyncDatadogWorkers),
		ResyncEnabled:         getenvBoolDefault("RESYNC_ENABLED", true),
		ResyncMode:            getenvDefault("RESYNC_MODE", "signal"),
		GlobalEvalMode:        strings.ToLower(strings.TrimSpace(getenvDefault("GLOBAL_EVAL_MODE", "best_effort"))),
		SyncLockMode:          strings.ToLower(strings.TrimSpace(getenvDefault("SYNC_LOCK_MODE", defaultSyncLockMode))),
		StartupReadModelRebuildMode: strings.ToLower(strings.TrimSpace(
			getenvDefault("STARTUP_READ_MODEL_REBUILD_MODE", defaultStartupReadModelMode),
		)),
		SyncLockTTL:               defaultSyncLockTTL,
		SyncLockHeartbeatInterval: defaultSyncLockHeartbeatInterval,
		SyncLockHeartbeatTimeout:  defaultSyncLockHeartbeatTimeout,
		SyncLockInstanceID:        strings.TrimSpace(os.Getenv("SYNC_LOCK_INSTANCE_ID")),
	}

	cfg.MetricsAddr = loadMetricsAddr(cfg.MetricsAddr)
	if err := applyDurationEnvOverrides(&cfg); err != nil {
		return cfg, err
	}
	connectorSecretKey, err := loadConnectorSecretKey()
	if err != nil {
		return cfg, err
	}
	cfg.ConnectorSecretKey = connectorSecretKey

	if err := validate(cfg, opts); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func loadMetricsAddr(def string) string {
	// Metrics are disabled by default in the Go binary (empty address). Some deployment methods (e.g. Helm)
	// may choose a safer non-empty default (like 127.0.0.1:9090) for defense in depth.
	//
	// Set METRICS_ADDR to:
	// - "127.0.0.1:9090" to bind localhost only (not reachable via pod IP / Service)
	// - ":9090" to bind all interfaces (0.0.0.0) inside the container/pod; restrict access via NetworkPolicy / mTLS
	// Metrics may include sensitive identifiers; use "off"/"disabled"/"false" to force-disable.
	addr := def
	if v, ok := os.LookupEnv("METRICS_ADDR"); ok {
		addr = strings.TrimSpace(v)
	}
	switch strings.ToLower(strings.TrimSpace(addr)) {
	case "off", "disabled", "false":
		return ""
	default:
		return addr
	}
}

func applyDurationEnvOverrides(cfg *Config) error {
	overrides := []struct {
		key             string
		target          *time.Duration
		requirePositive bool
	}{
		{key: "SYNC_INTERVAL", target: &cfg.SyncInterval},
		{key: "SYNC_DISCOVERY_INTERVAL", target: &cfg.SyncDiscoveryInterval},
		{key: "SYNC_OKTA_INTERVAL", target: &cfg.SyncOktaInterval, requirePositive: true},
		{key: "SYNC_ENTRA_INTERVAL", target: &cfg.SyncEntraInterval, requirePositive: true},
		{key: "SYNC_GOOGLE_WORKSPACE_INTERVAL", target: &cfg.SyncGoogleWorkspaceInterval, requirePositive: true},
		{key: "SYNC_GITHUB_INTERVAL", target: &cfg.SyncGitHubInterval, requirePositive: true},
		{key: "SYNC_DATADOG_INTERVAL", target: &cfg.SyncDatadogInterval, requirePositive: true},
		{key: "SYNC_AWS_INTERVAL", target: &cfg.SyncAWSInterval, requirePositive: true},
		{key: "SYNC_FAILURE_BACKOFF_MAX", target: &cfg.SyncFailureBackoffMax, requirePositive: true},
		{key: "SYNC_LOCK_TTL", target: &cfg.SyncLockTTL, requirePositive: true},
		{key: "SYNC_LOCK_HEARTBEAT_INTERVAL", target: &cfg.SyncLockHeartbeatInterval, requirePositive: true},
		{key: "SYNC_LOCK_HEARTBEAT_TIMEOUT", target: &cfg.SyncLockHeartbeatTimeout, requirePositive: true},
	}
	for _, override := range overrides {
		if err := applyDurationEnvOverride(override.target, override.key, override.requirePositive); err != nil {
			return err
		}
	}
	return nil
}

func applyDurationEnvOverride(target *time.Duration, key string, requirePositive bool) error {
	d, ok, err := parseDurationEnv(key, requirePositive)
	if err != nil {
		return err
	}
	if ok {
		*target = d
	}
	return nil
}

func validate(cfg Config, opts LoadOptions) error {
	if opts.RequireDatabaseURL && cfg.DatabaseURL == "" {
		return errors.New("DATABASE_URL is required")
	}
	for _, cidr := range cfg.TrustedProxyCIDRs {
		if _, _, err := net.ParseCIDR(cidr); err != nil {
			return fmt.Errorf("TRUSTED_PROXY_CIDRS contains invalid CIDR %q: %w", cidr, err)
		}
	}
	switch cfg.StartupReadModelRebuildMode {
	case StartupReadModelRebuildAuto, StartupReadModelRebuildAlways:
	default:
		return fmt.Errorf(
			"STARTUP_READ_MODEL_REBUILD_MODE must be %q or %q",
			StartupReadModelRebuildAuto,
			StartupReadModelRebuildAlways,
		)
	}

	return nil
}

func loadConnectorSecretKey() ([]byte, error) {
	raw := strings.TrimSpace(os.Getenv("CONNECTOR_SECRET_KEY"))
	path := strings.TrimSpace(os.Getenv("CONNECTOR_SECRET_KEY_FILE"))
	if raw != "" && path != "" {
		return nil, errors.New("CONNECTOR_SECRET_KEY and CONNECTOR_SECRET_KEY_FILE cannot both be set")
	}
	if path != "" {
		contents, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read CONNECTOR_SECRET_KEY_FILE: %w", err)
		}
		raw = strings.TrimSpace(string(contents))
	}
	if raw == "" {
		return nil, nil
	}
	key, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, errors.New("connector secret key must be base64 encoded")
	}
	if len(key) != 32 {
		return nil, errors.New("connector secret key must decode to 32 bytes")
	}
	return key, nil
}

func getenvDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvIntDefault(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return def
	}
	return n
}

func getenvBoolDefault(key string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	switch v {
	case "1":
		return true
	case "0":
		return false
	default:
		return def
	}
}

func splitCommaSeparated(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}

func parseDurationEnv(key string, requirePositive bool) (time.Duration, bool, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return 0, false, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, false, fmt.Errorf("%s must be a duration: %w", key, err)
	}
	if requirePositive && d <= 0 {
		return 0, false, fmt.Errorf("%s must be greater than zero", key)
	}
	return d, true, nil
}

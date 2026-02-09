package config

import (
	"errors"
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

	defaultSyncOktaWorkers    = 3
	defaultSyncGitHubWorkers  = 6
	defaultSyncDatadogWorkers = 3

	defaultSyncLockMode              = "lease"
	defaultSyncLockTTL               = 60 * time.Second
	defaultSyncLockHeartbeatInterval = 15 * time.Second
	defaultSyncLockHeartbeatTimeout  = 15 * time.Second
)

type Config struct {
	DatabaseURL               string
	HTTPAddr                  string
	MetricsAddr               string
	StaticDir                 string
	AuthCookieSecure          bool
	DevSeedAdmin              bool
	SyncInterval              time.Duration
	SyncDiscoveryInterval     time.Duration
	SyncOktaInterval          time.Duration
	SyncEntraInterval         time.Duration
	SyncGitHubInterval        time.Duration
	SyncDatadogInterval       time.Duration
	SyncAWSInterval           time.Duration
	SyncFailureBackoffMax     time.Duration
	SyncOktaWorkers           int
	SyncGitHubWorkers         int
	SyncDatadogWorkers        int
	ResyncEnabled             bool
	ResyncMode                string
	GlobalEvalMode            string
	SyncLockMode              string
	SyncLockTTL               time.Duration
	SyncLockHeartbeatInterval time.Duration
	SyncLockHeartbeatTimeout  time.Duration
	SyncLockInstanceID        string
}

type LoadOptions struct {
	RequireDatabaseURL bool
}

func Load() (Config, error) {
	return LoadWithOptions(LoadOptions{RequireDatabaseURL: true})
}

func LoadOptionalDB() (Config, error) {
	return LoadWithOptions(LoadOptions{RequireDatabaseURL: false})
}

func LoadWithOptions(opts LoadOptions) (Config, error) {
	if err := godotenv.Load(); err != nil {
		var pathErr *os.PathError
		if !errors.As(err, &pathErr) {
			return Config{}, err
		}
	}

	cfg := Config{
		DatabaseURL:               os.Getenv("DATABASE_URL"),
		HTTPAddr:                  getenvDefault("HTTP_ADDR", defaultHTTPAddr),
		MetricsAddr:               defaultMetricsAddr,
		StaticDir:                 strings.TrimSpace(os.Getenv("STATIC_DIR")),
		AuthCookieSecure:          getenvBoolDefault("AUTH_COOKIE_SECURE", false),
		DevSeedAdmin:              getenvBoolDefault("DEV_SEED_ADMIN", false),
		SyncInterval:              defaultSyncInterval,
		SyncDiscoveryInterval:     defaultSyncDiscoveryInterval,
		SyncOktaWorkers:           getenvIntDefault("SYNC_OKTA_WORKERS", defaultSyncOktaWorkers),
		SyncGitHubWorkers:         getenvIntDefault("SYNC_GITHUB_WORKERS", defaultSyncGitHubWorkers),
		SyncDatadogWorkers:        getenvIntDefault("SYNC_DATADOG_WORKERS", defaultSyncDatadogWorkers),
		ResyncEnabled:             getenvBoolDefault("RESYNC_ENABLED", true),
		ResyncMode:                getenvDefault("RESYNC_MODE", "signal"),
		GlobalEvalMode:            strings.ToLower(strings.TrimSpace(getenvDefault("GLOBAL_EVAL_MODE", "best_effort"))),
		SyncLockMode:              strings.ToLower(strings.TrimSpace(getenvDefault("SYNC_LOCK_MODE", defaultSyncLockMode))),
		SyncLockTTL:               defaultSyncLockTTL,
		SyncLockHeartbeatInterval: defaultSyncLockHeartbeatInterval,
		SyncLockHeartbeatTimeout:  defaultSyncLockHeartbeatTimeout,
		SyncLockInstanceID:        strings.TrimSpace(os.Getenv("SYNC_LOCK_INSTANCE_ID")),
	}

	// Metrics are disabled by default in the Go binary (empty address). Some deployment methods (e.g. Helm)
	// may choose a safer non-empty default (like 127.0.0.1:9090) for defense in depth.
	//
	// Set METRICS_ADDR to:
	// - "127.0.0.1:9090" to bind localhost only (not reachable via pod IP / Service)
	// - ":9090" to bind all interfaces (0.0.0.0) inside the container/pod; restrict access via NetworkPolicy / mTLS
	// Metrics may include sensitive identifiers; use "off"/"disabled"/"false" to force-disable.
	if v, ok := os.LookupEnv("METRICS_ADDR"); ok {
		cfg.MetricsAddr = strings.TrimSpace(v)
	}
	switch strings.ToLower(strings.TrimSpace(cfg.MetricsAddr)) {
	case "off", "disabled", "false":
		cfg.MetricsAddr = ""
	}

	if v := os.Getenv("SYNC_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.SyncInterval = d
		}
	}
	if v := os.Getenv("SYNC_DISCOVERY_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.SyncDiscoveryInterval = d
		}
	}
	if v := os.Getenv("SYNC_OKTA_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.SyncOktaInterval = d
		}
	}
	if v := os.Getenv("SYNC_ENTRA_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.SyncEntraInterval = d
		}
	}
	if v := os.Getenv("SYNC_GITHUB_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.SyncGitHubInterval = d
		}
	}
	if v := os.Getenv("SYNC_DATADOG_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.SyncDatadogInterval = d
		}
	}
	if v := os.Getenv("SYNC_AWS_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.SyncAWSInterval = d
		}
	}
	if v := os.Getenv("SYNC_FAILURE_BACKOFF_MAX"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.SyncFailureBackoffMax = d
		}
	}
	if v := os.Getenv("SYNC_LOCK_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.SyncLockTTL = d
		}
	}
	if v := os.Getenv("SYNC_LOCK_HEARTBEAT_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.SyncLockHeartbeatInterval = d
		}
	}
	if v := os.Getenv("SYNC_LOCK_HEARTBEAT_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.SyncLockHeartbeatTimeout = d
		}
	}

	if opts.RequireDatabaseURL && cfg.DatabaseURL == "" {
		return cfg, errors.New("DATABASE_URL is required")
	}

	return cfg, nil
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

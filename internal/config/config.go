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
	defaultHTTPAddr     = ":8080"
	defaultSyncInterval = 15 * time.Minute

	defaultSyncOktaWorkers    = 3
	defaultSyncGitHubWorkers  = 6
	defaultSyncDatadogWorkers = 3
)

type Config struct {
	DatabaseURL           string
	HTTPAddr              string
	AuthCookieSecure      bool
	DevSeedAdmin          bool
	SyncInterval          time.Duration
	SyncOktaInterval      time.Duration
	SyncEntraInterval     time.Duration
	SyncGitHubInterval    time.Duration
	SyncDatadogInterval   time.Duration
	SyncAWSInterval       time.Duration
	SyncFailureBackoffMax time.Duration
	SyncOktaWorkers       int
	SyncGitHubWorkers     int
	SyncDatadogWorkers    int
	ResyncEnabled         bool
	ResyncMode            string
	GlobalEvalMode        string
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
		DatabaseURL:        os.Getenv("DATABASE_URL"),
		HTTPAddr:           getenvDefault("HTTP_ADDR", defaultHTTPAddr),
		AuthCookieSecure:   getenvBoolDefault("AUTH_COOKIE_SECURE", false),
		DevSeedAdmin:       getenvBoolDefault("DEV_SEED_ADMIN", false),
		SyncInterval:       defaultSyncInterval,
		SyncOktaWorkers:    getenvIntDefault("SYNC_OKTA_WORKERS", defaultSyncOktaWorkers),
		SyncGitHubWorkers:  getenvIntDefault("SYNC_GITHUB_WORKERS", defaultSyncGitHubWorkers),
		SyncDatadogWorkers: getenvIntDefault("SYNC_DATADOG_WORKERS", defaultSyncDatadogWorkers),
		ResyncEnabled:      getenvBoolDefault("RESYNC_ENABLED", true),
		ResyncMode:         getenvDefault("RESYNC_MODE", "inline"),
		GlobalEvalMode:     strings.ToLower(strings.TrimSpace(getenvDefault("GLOBAL_EVAL_MODE", "best_effort"))),
	}

	if v := os.Getenv("SYNC_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.SyncInterval = d
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

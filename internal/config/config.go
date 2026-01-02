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
	DatabaseURL        string
	HTTPAddr           string
	SyncInterval       time.Duration
	SyncOktaWorkers    int
	SyncGitHubWorkers  int
	SyncDatadogWorkers int
	ResyncEnabled      bool
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
		SyncInterval:       defaultSyncInterval,
		SyncOktaWorkers:    getenvIntDefault("SYNC_OKTA_WORKERS", defaultSyncOktaWorkers),
		SyncGitHubWorkers:  getenvIntDefault("SYNC_GITHUB_WORKERS", defaultSyncGitHubWorkers),
		SyncDatadogWorkers: getenvIntDefault("SYNC_DATADOG_WORKERS", defaultSyncDatadogWorkers),
		ResyncEnabled:      getenvBoolDefault("RESYNC_ENABLED", true),
	}

	if v := os.Getenv("SYNC_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.SyncInterval = d
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

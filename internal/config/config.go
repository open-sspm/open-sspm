package config

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/mail"
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
	defaultSyncTailInterval      = 5 * time.Minute
	defaultStartupReadModelMode  = StartupReadModelRebuildAuto
	defaultSMTPPort              = 587
	defaultSMTPTLSMode           = SMTPTLSModeStartTLS

	defaultSyncOktaWorkers    = 3
	defaultSyncGitHubWorkers  = 6
	defaultSyncDatadogWorkers = 3

	defaultSyncLockMode              = "lease"
	defaultSyncLockTTL               = 60 * time.Second
	defaultSyncLockHeartbeatInterval = 15 * time.Second
	defaultSyncLockHeartbeatTimeout  = 15 * time.Second

	defaultEventInboxBatchSize               = 500
	defaultEventInboxPollInterval            = 5 * time.Second
	defaultEventInboxCleanupInterval         = time.Hour
	defaultEventInboxRetryDelay              = 30 * time.Second
	defaultEventInboxRetryMaxDelay           = 15 * time.Minute
	defaultEventInboxStaleProcessingTimeout  = 5 * time.Minute
	defaultEventInboxMaxAttempts             = 10
	defaultEventInboxProcessedRetentionDays  = 30
	defaultEventInboxDeadLetterRetentionDays = 90

	defaultEventEvaluatorWorkerPollInterval = 5 * time.Second
	defaultEventEvaluatorWorkerBatchSize    = 100
	defaultEventEvaluatorWorkerMaxAttempts  = 10

	defaultEventPartitionMaintenanceInterval = 12 * time.Hour
	defaultEventPartitionFutureDays          = 7
	defaultEventRetentionDays                = 90
)

const (
	StartupReadModelRebuildAuto   = "auto"
	StartupReadModelRebuildAlways = "always"

	SMTPTLSModeStartTLS = "starttls"
	SMTPTLSModeTLS      = "tls"
	SMTPTLSModePlain    = "plain"
)

type Config struct {
	DatabaseURL                 string
	ConnectorSecretKey          []byte
	SMTP                        SMTPConfig
	HTTPAddr                    string
	MetricsAddr                 string
	StaticDir                   string
	AuthCookieSecure            bool
	TrustedProxyCIDRs           []string
	DevSeedAdmin                bool
	SyncFullEnabled             bool
	SyncDiscoveryEnabled        bool
	EventInboxEnabled           bool
	SyncInterval                time.Duration
	SyncDiscoveryInterval       time.Duration
	SyncTailInterval            time.Duration
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
	EventInbox                  EventInboxConfig
	EventEvaluatorWorker        EventEvaluatorWorkerConfig
	EventPartitions             EventPartitionConfig
}

type SMTPConfig struct {
	Enabled     bool
	Host        string
	Port        int
	Username    string
	Password    string
	FromAddress string
	FromName    string
	TLSMode     string
}

type EventInboxConfig struct {
	BatchSize               int32
	PollInterval            time.Duration
	CleanupInterval         time.Duration
	RetryDelay              time.Duration
	RetryDelayMax           time.Duration
	StaleProcessingTimeout  time.Duration
	MaxAttempts             int32
	ProcessedRetentionDays  int32
	DeadLetterRetentionDays int32
}

type EventEvaluatorWorkerConfig struct {
	PollInterval time.Duration
	BatchSize    int32
	MaxAttempts  int32
}

type EventPartitionConfig struct {
	MaintenanceInterval time.Duration
	FutureDays          int32
	RetentionDays       int32
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
		SyncFullEnabled:       getenvBoolDefault("SYNC_FULL_ENABLED", true),
		SyncDiscoveryEnabled:  getenvBoolDefault("SYNC_DISCOVERY_ENABLED", true),
		EventInboxEnabled:     true,
		SyncInterval:          defaultSyncInterval,
		SyncDiscoveryInterval: defaultSyncDiscoveryInterval,
		SyncTailInterval:      defaultSyncTailInterval,
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
		EventInbox: EventInboxConfig{
			BatchSize:               defaultEventInboxBatchSize,
			PollInterval:            defaultEventInboxPollInterval,
			CleanupInterval:         defaultEventInboxCleanupInterval,
			RetryDelay:              defaultEventInboxRetryDelay,
			RetryDelayMax:           defaultEventInboxRetryMaxDelay,
			StaleProcessingTimeout:  defaultEventInboxStaleProcessingTimeout,
			MaxAttempts:             defaultEventInboxMaxAttempts,
			ProcessedRetentionDays:  defaultEventInboxProcessedRetentionDays,
			DeadLetterRetentionDays: defaultEventInboxDeadLetterRetentionDays,
		},
		EventEvaluatorWorker: EventEvaluatorWorkerConfig{
			PollInterval: defaultEventEvaluatorWorkerPollInterval,
			BatchSize:    defaultEventEvaluatorWorkerBatchSize,
			MaxAttempts:  defaultEventEvaluatorWorkerMaxAttempts,
		},
		EventPartitions: EventPartitionConfig{
			MaintenanceInterval: defaultEventPartitionMaintenanceInterval,
			FutureDays:          defaultEventPartitionFutureDays,
			RetentionDays:       defaultEventRetentionDays,
		},
	}
	var err error
	cfg.EventInboxEnabled, err = getenvBoolDefaultStrict("EVENT_INBOX_ENABLED", cfg.EventInboxEnabled)
	if err != nil {
		return cfg, err
	}
	smtpConfig, err := loadSMTPConfig()
	if err != nil {
		return cfg, err
	}
	cfg.SMTP = smtpConfig

	cfg.MetricsAddr = loadMetricsAddr(cfg.MetricsAddr)
	if err := applyDurationEnvOverrides(&cfg); err != nil {
		return cfg, err
	}
	if err := applyIntEnvOverrides(&cfg); err != nil {
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
		{key: "SYNC_TAIL_INTERVAL", target: &cfg.SyncTailInterval, requirePositive: true},
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
		{key: "EVENT_INBOX_POLL_INTERVAL", target: &cfg.EventInbox.PollInterval, requirePositive: true},
		{key: "EVENT_INBOX_CLEANUP_INTERVAL", target: &cfg.EventInbox.CleanupInterval, requirePositive: true},
		{key: "EVENT_INBOX_RETRY_DELAY", target: &cfg.EventInbox.RetryDelay, requirePositive: true},
		{key: "EVENT_INBOX_RETRY_MAX_DELAY", target: &cfg.EventInbox.RetryDelayMax, requirePositive: true},
		{key: "EVENT_INBOX_STALE_PROCESSING_TIMEOUT", target: &cfg.EventInbox.StaleProcessingTimeout, requirePositive: true},
		{key: "EVENT_EVALUATOR_WORKER_POLL_INTERVAL", target: &cfg.EventEvaluatorWorker.PollInterval, requirePositive: true},
		{key: "EVENT_PARTITION_MAINTENANCE_INTERVAL", target: &cfg.EventPartitions.MaintenanceInterval, requirePositive: true},
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

func applyIntEnvOverrides(cfg *Config) error {
	overrides := []struct {
		key    string
		target *int32
	}{
		{key: "EVENT_INBOX_BATCH_SIZE", target: &cfg.EventInbox.BatchSize},
		{key: "EVENT_INBOX_MAX_ATTEMPTS", target: &cfg.EventInbox.MaxAttempts},
		{key: "EVENT_INBOX_PROCESSED_RETENTION_DAYS", target: &cfg.EventInbox.ProcessedRetentionDays},
		{key: "EVENT_INBOX_DEAD_LETTER_RETENTION_DAYS", target: &cfg.EventInbox.DeadLetterRetentionDays},
		{key: "EVENT_EVALUATOR_WORKER_BATCH_SIZE", target: &cfg.EventEvaluatorWorker.BatchSize},
		{key: "EVENT_EVALUATOR_WORKER_MAX_ATTEMPTS", target: &cfg.EventEvaluatorWorker.MaxAttempts},
		{key: "EVENT_PARTITION_FUTURE_DAYS", target: &cfg.EventPartitions.FutureDays},
		{key: "EVENT_RETENTION_DAYS", target: &cfg.EventPartitions.RetentionDays},
	}
	for _, override := range overrides {
		n, ok, err := parsePositiveInt32Env(override.key)
		if err != nil {
			return err
		}
		if ok {
			*override.target = n
		}
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
	if cfg.EventInbox.RetryDelayMax < cfg.EventInbox.RetryDelay {
		return errors.New("EVENT_INBOX_RETRY_MAX_DELAY must be greater than or equal to EVENT_INBOX_RETRY_DELAY")
	}
	if cfg.SyncTailInterval <= 0 {
		return errors.New("SYNC_TAIL_INTERVAL must be greater than zero")
	}
	if cfg.EventEvaluatorWorker.PollInterval <= 0 {
		return errors.New("EVENT_EVALUATOR_WORKER_POLL_INTERVAL must be greater than zero")
	}
	if cfg.EventEvaluatorWorker.BatchSize <= 0 {
		return errors.New("EVENT_EVALUATOR_WORKER_BATCH_SIZE must be greater than zero")
	}
	if cfg.EventEvaluatorWorker.MaxAttempts <= 0 {
		return errors.New("EVENT_EVALUATOR_WORKER_MAX_ATTEMPTS must be greater than zero")
	}
	if cfg.EventPartitions.MaintenanceInterval <= 0 {
		return errors.New("EVENT_PARTITION_MAINTENANCE_INTERVAL must be greater than zero")
	}
	if cfg.EventPartitions.FutureDays <= 0 {
		return errors.New("EVENT_PARTITION_FUTURE_DAYS must be greater than zero")
	}
	if cfg.EventPartitions.RetentionDays <= 0 {
		return errors.New("EVENT_RETENTION_DAYS must be greater than zero")
	}

	return nil
}

func loadSMTPConfig() (SMTPConfig, error) {
	cfg := SMTPConfig{
		Enabled: getenvBoolDefault("SMTP_ENABLED", false),
		Port:    defaultSMTPPort,
		TLSMode: defaultSMTPTLSMode,
	}
	if !cfg.Enabled {
		return cfg, nil
	}

	cfg.Host = strings.TrimSpace(os.Getenv("SMTP_HOST"))
	cfg.Username = strings.TrimSpace(os.Getenv("SMTP_USERNAME"))
	cfg.Password = os.Getenv("SMTP_PASSWORD")
	cfg.FromAddress = strings.TrimSpace(os.Getenv("SMTP_FROM_ADDRESS"))
	cfg.FromName = strings.TrimSpace(os.Getenv("SMTP_FROM_NAME"))
	cfg.TLSMode = strings.ToLower(strings.TrimSpace(getenvDefault("SMTP_TLS_MODE", defaultSMTPTLSMode)))

	port, err := getenvRequiredPositiveIntDefault("SMTP_PORT", defaultSMTPPort)
	if err != nil {
		return cfg, err
	}
	cfg.Port = port

	if cfg.Host == "" {
		return cfg, errors.New("SMTP_HOST is required when SMTP_ENABLED=1")
	}
	if cfg.FromAddress == "" {
		return cfg, errors.New("SMTP_FROM_ADDRESS is required when SMTP_ENABLED=1")
	}
	if cfg.Port > 65535 {
		return cfg, errors.New("SMTP_PORT must be between 1 and 65535")
	}
	switch cfg.TLSMode {
	case SMTPTLSModeStartTLS, SMTPTLSModeTLS, SMTPTLSModePlain:
	default:
		return cfg, fmt.Errorf(
			"SMTP_TLS_MODE must be one of: %s, %s, %s",
			SMTPTLSModeStartTLS,
			SMTPTLSModeTLS,
			SMTPTLSModePlain,
		)
	}
	addr, err := mail.ParseAddress(cfg.FromAddress)
	if err != nil {
		return cfg, fmt.Errorf("SMTP_FROM_ADDRESS must be a valid email address: %w", err)
	}
	cfg.FromAddress = addr.Address
	if cfg.FromName == "" && strings.TrimSpace(addr.Name) != "" {
		cfg.FromName = addr.Name
	}
	if strings.ContainsAny(cfg.FromName, "\r\n") {
		return cfg, errors.New("SMTP_FROM_NAME must not contain carriage returns or line feeds")
	}
	if (cfg.Username == "") != (cfg.Password == "") {
		return cfg, errors.New("SMTP_USERNAME and SMTP_PASSWORD must either both be set or both be empty")
	}
	if cfg.Username != "" && cfg.TLSMode == SMTPTLSModePlain && !smtpPlainAuthAllowsInsecureHost(cfg.Host) {
		return cfg, errors.New("SMTP_TLS_MODE=plain only supports SMTP authentication on localhost")
	}

	return cfg, nil
}

func smtpPlainAuthAllowsInsecureHost(host string) bool {
	switch host {
	case "localhost", "127.0.0.1", "::1":
		return true
	default:
		return false
	}
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

func getenvRequiredPositiveIntDefault(key string, def int) (int, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s must be a whole number", key)
	}
	if n < 1 {
		return 0, fmt.Errorf("%s must be greater than zero", key)
	}
	return n, nil
}

func getenvBoolDefault(key string, def bool) bool {
	value, _ := getenvBoolDefaultWithLookup(key, def)
	return value
}

func getenvBoolDefaultWithLookup(key string, def bool) (bool, bool) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, false
	}
	switch v {
	case "1":
		return true, true
	case "0":
		return false, true
	default:
		return def, true
	}
}

func getenvBoolDefaultStrict(key string, def bool) (bool, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	switch v {
	case "1":
		return true, nil
	case "0":
		return false, nil
	default:
		return def, fmt.Errorf("%s must be 0 or 1", key)
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

func parsePositiveInt32Env(key string) (int32, bool, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return 0, false, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, false, fmt.Errorf("%s must be a whole number", key)
	}
	if n < 1 {
		return 0, false, fmt.Errorf("%s must be greater than zero", key)
	}
	if n > 1<<31-1 {
		return 0, false, fmt.Errorf("%s is too large", key)
	}
	return int32(n), true, nil
}

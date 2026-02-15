package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
)

const (
	// EnvFormat controls the output handler format for structured logs.
	EnvFormat = "LOG_FORMAT"
	// EnvLevel controls the minimum severity level for structured logs.
	EnvLevel = "LOG_LEVEL"

	defaultFormat = "json"
	defaultLevel  = "info"
)

// Config is the validated logging configuration derived from environment variables.
type Config struct {
	Format string
	Level  slog.Level
}

// BootstrapOptions controls logger initialization behavior.
type BootstrapOptions struct {
	Command string
	Writer  io.Writer
}

// DefaultConfig returns the default structured logging configuration.
func DefaultConfig() Config {
	return Config{
		Format: defaultFormat,
		Level:  slog.LevelInfo,
	}
}

// LoadConfigFromEnv parses and validates logging environment variables.
func LoadConfigFromEnv() (Config, error) {
	format, err := parseFormat(os.Getenv(EnvFormat))
	if err != nil {
		return Config{}, err
	}
	level, err := parseLevel(os.Getenv(EnvLevel))
	if err != nil {
		return Config{}, err
	}
	return Config{
		Format: format,
		Level:  level,
	}, nil
}

// NewLogger creates a structured logger with static Open-SSPM context attributes.
func NewLogger(cfg Config, writer io.Writer, command string) *slog.Logger {
	if writer == nil {
		writer = os.Stdout
	}

	opts := &slog.HandlerOptions{Level: cfg.Level}
	var handler slog.Handler
	switch strings.ToLower(strings.TrimSpace(cfg.Format)) {
	case "text":
		handler = slog.NewTextHandler(writer, opts)
	default:
		handler = slog.NewJSONHandler(writer, opts)
	}

	command = strings.TrimSpace(command)
	if command == "" {
		command = "open-sspm"
	}
	return slog.New(handler).With("app", "open-sspm", "command", command)
}

// BootstrapFromEnv loads logging config from env, installs the default logger, and returns it.
func BootstrapFromEnv(opts BootstrapOptions) (*slog.Logger, error) {
	cfg, err := LoadConfigFromEnv()
	if err != nil {
		return nil, err
	}
	logger := NewLogger(cfg, opts.Writer, opts.Command)
	slog.SetDefault(logger)
	return logger, nil
}

func parseFormat(raw string) (string, error) {
	format := strings.ToLower(strings.TrimSpace(raw))
	if format == "" {
		return defaultFormat, nil
	}
	switch format {
	case "json", "text":
		return format, nil
	default:
		return "", fmt.Errorf("%s must be one of: json, text", EnvFormat)
	}
}

func parseLevel(raw string) (slog.Level, error) {
	level := strings.ToLower(strings.TrimSpace(raw))
	if level == "" {
		level = defaultLevel
	}
	switch level {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("%s must be one of: debug, info, warn, error", EnvLevel)
	}
}

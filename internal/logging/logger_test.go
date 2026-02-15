package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestLoadConfigFromEnv_Defaults(t *testing.T) {
	t.Setenv(EnvFormat, "")
	t.Setenv(EnvLevel, "")

	cfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatalf("LoadConfigFromEnv() error = %v", err)
	}
	if cfg.Format != "json" {
		t.Fatalf("Format = %q, want %q", cfg.Format, "json")
	}
	if cfg.Level != slog.LevelInfo {
		t.Fatalf("Level = %v, want %v", cfg.Level, slog.LevelInfo)
	}
}

func TestLoadConfigFromEnv_ValidValues(t *testing.T) {
	t.Setenv(EnvFormat, "text")
	t.Setenv(EnvLevel, "debug")

	cfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatalf("LoadConfigFromEnv() error = %v", err)
	}
	if cfg.Format != "text" {
		t.Fatalf("Format = %q, want %q", cfg.Format, "text")
	}
	if cfg.Level != slog.LevelDebug {
		t.Fatalf("Level = %v, want %v", cfg.Level, slog.LevelDebug)
	}
}

func TestLoadConfigFromEnv_InvalidFormat(t *testing.T) {
	t.Setenv(EnvFormat, "yaml")
	t.Setenv(EnvLevel, "")

	_, err := LoadConfigFromEnv()
	if err == nil {
		t.Fatal("expected invalid LOG_FORMAT error")
	}
}

func TestLoadConfigFromEnv_InvalidLevel(t *testing.T) {
	t.Setenv(EnvFormat, "")
	t.Setenv(EnvLevel, "trace")

	_, err := LoadConfigFromEnv()
	if err == nil {
		t.Fatal("expected invalid LOG_LEVEL error")
	}
}

func TestNewLogger_JSONIncludesStaticAttrs(t *testing.T) {
	var out bytes.Buffer
	logger := NewLogger(DefaultConfig(), &out, "open-sspm serve")
	logger.Info("hello")

	line := strings.TrimSpace(out.String())
	if line == "" {
		t.Fatal("expected JSON log line")
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(line), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if got := payload["app"]; got != "open-sspm" {
		t.Fatalf("app = %v, want %q", got, "open-sspm")
	}
	if got := payload["command"]; got != "open-sspm serve" {
		t.Fatalf("command = %v, want %q", got, "open-sspm serve")
	}
}

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestEmitCommandError_StructuredForScopedCommands(t *testing.T) {
	t.Setenv("LOG_FORMAT", "json")
	t.Setenv("LOG_LEVEL", "info")
	setCommandExecutionContext(commandExecutionContext{
		CommandPath:       "open-sspm serve",
		UsesStructuredLog: true,
	})
	t.Cleanup(resetCommandExecutionContext)

	var out bytes.Buffer
	emitCommandError(errors.New("boom"), "command failed", 1, &out)

	line := strings.TrimSpace(out.String())
	if line == "" {
		t.Fatal("expected structured log output")
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
	if got := payload["exit_code"]; got != float64(1) {
		t.Fatalf("exit_code = %v, want %v", got, 1)
	}
	if got := payload["error"]; got != "boom" {
		t.Fatalf("error = %v, want %q", got, "boom")
	}
}

func TestEmitCommandError_FallsBackToJSONWhenLoggingEnvInvalid(t *testing.T) {
	t.Setenv("LOG_FORMAT", "invalid")
	t.Setenv("LOG_LEVEL", "info")
	setCommandExecutionContext(commandExecutionContext{
		CommandPath:       "open-sspm worker",
		UsesStructuredLog: true,
	})
	t.Cleanup(resetCommandExecutionContext)

	var out bytes.Buffer
	emitCommandError(errors.New("boom"), "command failed", 1, &out)

	line := strings.TrimSpace(out.String())
	if line == "" {
		t.Fatal("expected structured log output")
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(line), &payload); err != nil {
		t.Fatalf("expected JSON fallback log, got parse error: %v", err)
	}
}

func TestEmitCommandError_PlainOutputForNonScopedCommands(t *testing.T) {
	setCommandExecutionContext(commandExecutionContext{
		CommandPath:       "open-sspm users bootstrap-admin",
		UsesStructuredLog: false,
	})
	t.Cleanup(resetCommandExecutionContext)

	var out bytes.Buffer
	emitCommandError(errors.New("plain boom"), "command failed", 1, &out)
	if got := out.String(); got != "plain boom\n" {
		t.Fatalf("output = %q, want %q", got, "plain boom\n")
	}
}

func TestEmitCommandError_CanceledOutputForNonScopedCommands(t *testing.T) {
	setCommandExecutionContext(commandExecutionContext{
		CommandPath:       "open-sspm users bootstrap-admin",
		UsesStructuredLog: false,
	})
	t.Cleanup(resetCommandExecutionContext)

	var out bytes.Buffer
	emitCommandError(context.Canceled, "command canceled", 130, &out)
	if got := out.String(); got != "canceled\n" {
		t.Fatalf("output = %q, want %q", got, "canceled\n")
	}
}

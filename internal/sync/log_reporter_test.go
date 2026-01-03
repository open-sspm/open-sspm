package sync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

type countingHandler struct {
	mu    sync.Mutex
	count int
}

func (h *countingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *countingHandler) Handle(context.Context, slog.Record) error {
	h.mu.Lock()
	h.count++
	h.mu.Unlock()
	return nil
}

func (h *countingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *countingHandler) WithGroup(string) slog.Handler      { return h }

func (h *countingHandler) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.count
}

func TestLogReporterThrottlesProgress(t *testing.T) {
	t.Parallel()

	handler := &countingHandler{}
	logger := slog.New(handler)

	reporter := &LogReporter{
		Logger:              logger,
		ProgressInterval:    time.Hour,
		ProgressPercentStep: 5,
	}

	const total = 1000
	reporter.Report(registry.Event{Source: "okta", Stage: "sync-users", Current: 0, Total: total, Message: "syncing users"})
	for i := int64(1); i < total; i++ {
		reporter.Report(registry.Event{
			Source:  "okta",
			Stage:   "sync-users",
			Current: i,
			Total:   total,
			Message: fmt.Sprintf("users %d/%d", i, total),
		})
	}
	reporter.Report(registry.Event{Source: "okta", Stage: "sync-users", Current: total, Total: total, Message: "sync complete"})

	step := reporter.ProgressPercentStep
	expected := 2 + int(int64(99)/step) // 0% + each step (excluding 100%) + 100%
	if got := handler.Count(); got != expected {
		t.Fatalf("expected %d logs, got %d", expected, got)
	}
}

func TestLogReporterAlwaysLogsErrors(t *testing.T) {
	t.Parallel()

	handler := &countingHandler{}
	logger := slog.New(handler)

	reporter := &LogReporter{Logger: logger}
	reporter.Report(registry.Event{Source: "okta", Stage: "sync-users", Err: errors.New("boom")})

	if got := handler.Count(); got != 1 {
		t.Fatalf("expected 1 log, got %d", got)
	}
}

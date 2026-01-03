package sync

import (
	"log/slog"
	"sync"
	"time"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

const (
	defaultProgressInterval    = 5 * time.Second
	defaultProgressPercentStep = int64(5)
)

type logReporterKey struct {
	source string
	stage  string
}

type logReporterState struct {
	lastLoggedAt      time.Time
	lastLoggedPercent int64
	lastLoggedCurrent int64
}

// LogReporter is a simple reporter that logs events to the default slog logger.
type LogReporter struct {
	Logger              *slog.Logger
	ProgressInterval    time.Duration
	ProgressPercentStep int64

	mu    sync.Mutex
	state map[logReporterKey]logReporterState
}

func (r *LogReporter) Report(e registry.Event) {
	logger := r.Logger
	if logger == nil {
		logger = slog.Default()
	}

	now := e.At
	if now.IsZero() {
		now = time.Now()
	}

	attrs := []any{"source", e.Source}
	if e.Stage != "" {
		attrs = append(attrs, "stage", e.Stage)
	}
	if e.Current != 0 || e.Total != 0 {
		attrs = append(attrs, "current", e.Current, "total", e.Total)
	}

	message := e.Message
	if e.Err != nil {
		if message == "" {
			switch {
			case e.Source != "" && e.Stage != "":
				message = e.Source + " " + e.Stage + " failed"
			case e.Source != "":
				message = e.Source + " failed"
			default:
				message = "sync failed"
			}
		}
		attrs = append(attrs, "err", e.Err)
		logger.Error(message, attrs...)
		return
	}
	if message == "" {
		if e.Done {
			message = "sync complete"
		} else {
			return
		}
	}

	if !r.shouldLogEvent(now, e) {
		return
	}
	logger.Info(message, attrs...)
}

func (r *LogReporter) shouldLogEvent(now time.Time, e registry.Event) bool {
	interval := r.ProgressInterval
	if interval <= 0 {
		interval = defaultProgressInterval
	}
	step := r.ProgressPercentStep
	if step <= 0 {
		step = defaultProgressPercentStep
	}

	total := e.Total
	current := e.Current

	// Always log explicit completion events.
	if e.Done {
		return true
	}

	// Non-progress events (no counters) are logged by default.
	if total == 0 && current == 0 {
		return true
	}
	if total == 1 {
		return true
	}

	// Stage start/end should always log for known totals.
	if total > 1 && (current <= 0 || current >= total) {
		r.recordProgress(now, e, step)
		return true
	}

	// Throttle progress logs.
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == nil {
		r.state = make(map[logReporterKey]logReporterState)
	}
	key := logReporterKey{source: e.Source, stage: e.Stage}
	state := r.state[key]
	if !state.lastLoggedAt.IsZero() {
		if now.Sub(state.lastLoggedAt) < interval {
			if total > 1 {
				percent := (current * 100) / total
				if percent < state.lastLoggedPercent+step {
					return false
				}
			} else {
				return false
			}
		}
	}

	percent := progressPercent(current, total)
	if total > 1 && percent > 0 && step > 0 {
		percent = (percent / step) * step
	}
	r.state[key] = logReporterState{
		lastLoggedAt:      now,
		lastLoggedPercent: percent,
		lastLoggedCurrent: current,
	}
	return true
}

func (r *LogReporter) recordProgress(now time.Time, e registry.Event, step int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == nil {
		r.state = make(map[logReporterKey]logReporterState)
	}
	key := logReporterKey{source: e.Source, stage: e.Stage}
	state := r.state[key]

	percent := progressPercent(e.Current, e.Total)
	if percent > 0 && step > 0 {
		percent = (percent / step) * step
	}
	state.lastLoggedAt = now
	state.lastLoggedPercent = percent
	state.lastLoggedCurrent = e.Current
	r.state[key] = state
}

func progressPercent(current, total int64) int64 {
	if total <= 0 {
		return 0
	}
	if current <= 0 {
		return 0
	}
	if current >= total {
		return 100
	}
	return (current * 100) / total
}

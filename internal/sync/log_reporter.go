package sync

import (
	"log/slog"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

// LogReporter is a simple reporter that logs events to the default slog logger.
type LogReporter struct{}

func (r *LogReporter) Report(e registry.Event) {
	if e.Message != "" {
		attrs := []any{"source", e.Source}
		if e.Stage != "" {
			attrs = append(attrs, "stage", e.Stage)
		}
		if e.Err != nil {
			attrs = append(attrs, "err", e.Err)
			slog.Error(e.Message, attrs...)
		} else {
			slog.Info(e.Message, attrs...)
		}
	}
}

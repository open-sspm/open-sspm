package sync

import (
	"log"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

// LogReporter is a simple reporter that logs events to the standard logger.
type LogReporter struct{}

func (r *LogReporter) Report(e registry.Event) {
	if e.Message != "" {
		if e.Err != nil {
			log.Printf("[%s] %s: %v", e.Source, e.Message, e.Err)
		} else {
			log.Printf("[%s] %s", e.Source, e.Message)
		}
	}
}

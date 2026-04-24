package registry

import "github.com/open-sspm/open-sspm/internal/discovery"

func DiscoveryProgressReporter(report func(Event)) func(discovery.ProgressEvent) {
	return func(event discovery.ProgressEvent) {
		if report == nil {
			return
		}
		report(Event{
			Source:  event.Source,
			Stage:   event.Stage,
			Current: event.Current,
			Total:   event.Total,
			Message: event.Message,
		})
	}
}

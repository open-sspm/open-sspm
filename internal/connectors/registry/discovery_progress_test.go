package registry

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/discovery"
)

func TestDiscoveryProgressReporterMapsEvent(t *testing.T) {
	t.Parallel()

	var got Event
	reporter := DiscoveryProgressReporter(func(event Event) {
		got = event
	})

	reporter(discovery.ProgressEvent{
		Source:  "okta",
		Stage:   "write-discovery",
		Current: 2,
		Total:   5,
		Message: "sources 2/5",
	})

	if got.Source != "okta" {
		t.Fatalf("Source = %q, want okta", got.Source)
	}
	if got.Stage != "write-discovery" {
		t.Fatalf("Stage = %q, want write-discovery", got.Stage)
	}
	if got.Current != 2 {
		t.Fatalf("Current = %d, want 2", got.Current)
	}
	if got.Total != 5 {
		t.Fatalf("Total = %d, want 5", got.Total)
	}
	if got.Message != "sources 2/5" {
		t.Fatalf("Message = %q, want sources 2/5", got.Message)
	}
}

func TestDiscoveryProgressReporterAllowsNilReport(t *testing.T) {
	t.Parallel()

	DiscoveryProgressReporter(nil)(discovery.ProgressEvent{})
}

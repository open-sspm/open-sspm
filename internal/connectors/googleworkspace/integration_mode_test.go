package googleworkspace

import (
	"context"
	"testing"
	"time"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func TestGoogleWorkspaceIntegrationSupportsRunMode(t *testing.T) {
	t.Parallel()

	full := NewGoogleWorkspaceIntegration(nil, "C0123", "", false)
	if !full.SupportsRunMode(registry.RunModeFull) {
		t.Fatalf("full mode should always be supported")
	}
	if full.SupportsRunMode(registry.RunModeDiscovery) {
		t.Fatalf("discovery mode should be disabled when discovery is not configured")
	}
	if full.SupportsRunMode(registry.RunModeTail) {
		t.Fatalf("tail mode should require a Reports API client or test lister")
	}

	tail := NewGoogleWorkspaceIntegration(nil, "C0123", "", false)
	tail.reportsActivityLister = func(context.Context, time.Time) ([]WorkspaceActivity, error) {
		return nil, nil
	}
	if !tail.SupportsRunMode(registry.RunModeTail) {
		t.Fatalf("tail mode should be supported when a Reports activity lister is configured")
	}

	discovery := NewGoogleWorkspaceIntegration(nil, "C0123", "", true)
	if !discovery.SupportsRunMode(registry.RunModeDiscovery) {
		t.Fatalf("discovery mode should be supported when discovery is enabled")
	}
}

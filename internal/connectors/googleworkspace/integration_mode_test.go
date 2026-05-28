package googleworkspace

import (
	"context"
	"testing"
	"time"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func TestGoogleWorkspaceIntegrationSupportsRunMode(t *testing.T) {
	t.Parallel()

	unconfigured := NewGoogleWorkspaceIntegration(nil, "C0123", "", false)
	if unconfigured.SupportsRunMode(registry.RunModeFull) {
		t.Fatalf("full mode should require an API client")
	}
	if unconfigured.SupportsRunMode(registry.RunModeTail) {
		t.Fatalf("tail mode should require a Reports API client or test lister")
	}

	full := NewGoogleWorkspaceIntegration(&Client{}, "C0123", "", false)
	if !full.SupportsRunMode(registry.RunModeFull) {
		t.Fatalf("full mode should be supported when an API client is configured")
	}
	if full.SupportsRunMode(registry.RunModeDiscovery) {
		t.Fatalf("discovery mode should be disabled when discovery is not configured")
	}
	if !full.SupportsRunMode(registry.RunModeTail) {
		t.Fatalf("tail mode should be supported when a Reports API client is configured")
	}

	tail := NewGoogleWorkspaceIntegration(nil, "C0123", "", false)
	tail.reportsActivityLister = func(context.Context, time.Time) ([]WorkspaceActivity, error) {
		return nil, nil
	}
	if !tail.SupportsRunMode(registry.RunModeTail) {
		t.Fatalf("tail mode should be supported when a Reports activity lister is configured")
	}

	discovery := NewGoogleWorkspaceIntegration(&Client{}, "C0123", "", true)
	if !discovery.SupportsRunMode(registry.RunModeDiscovery) {
		t.Fatalf("discovery mode should be supported when discovery is enabled")
	}
}

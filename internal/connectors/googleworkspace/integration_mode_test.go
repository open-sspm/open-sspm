package googleworkspace

import (
	"testing"

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

	discovery := NewGoogleWorkspaceIntegration(nil, "C0123", "", true)
	if !discovery.SupportsRunMode(registry.RunModeDiscovery) {
		t.Fatalf("discovery mode should be supported when discovery is enabled")
	}
}

package entra

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func TestEntraIntegration_SupportsRunMode(t *testing.T) {
	t.Parallel()

	unconfigured := NewEntraIntegration(nil, "tenant", false)
	if unconfigured.SupportsRunMode(registry.RunModeFull) {
		t.Fatalf("full mode should require an API client")
	}

	full := NewEntraIntegration(stubEntraClient{}, "tenant", false)
	if !full.SupportsRunMode(registry.RunModeFull) {
		t.Fatalf("full mode should be supported when an API client is configured")
	}
	if full.SupportsRunMode(registry.RunModeDiscovery) {
		t.Fatalf("discovery mode should be disabled when discovery is not configured")
	}
	if full.SupportsRunMode(registry.RunModeTail) {
		t.Fatalf("tail mode should be disabled until Entra tail is implemented")
	}

	discovery := NewEntraIntegration(stubEntraClient{}, "tenant", true)
	if !discovery.SupportsRunMode(registry.RunModeDiscovery) {
		t.Fatalf("discovery mode should be supported when discovery is enabled")
	}
}

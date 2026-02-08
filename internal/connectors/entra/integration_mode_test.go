package entra

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func TestEntraIntegration_SupportsRunMode(t *testing.T) {
	t.Parallel()

	full := NewEntraIntegration(nil, "tenant", false)
	if !full.SupportsRunMode(registry.RunModeFull) {
		t.Fatalf("full mode should always be supported")
	}
	if full.SupportsRunMode(registry.RunModeDiscovery) {
		t.Fatalf("discovery mode should be disabled when discovery is not configured")
	}

	discovery := NewEntraIntegration(nil, "tenant", true)
	if !discovery.SupportsRunMode(registry.RunModeDiscovery) {
		t.Fatalf("discovery mode should be supported when discovery is enabled")
	}
}

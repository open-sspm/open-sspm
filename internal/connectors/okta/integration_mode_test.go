package okta

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func TestOktaIntegration_SupportsRunMode(t *testing.T) {
	t.Parallel()

	full := NewOktaIntegration(nil, "example.okta.com", 1, false)
	if !full.SupportsRunMode(registry.RunModeFull) {
		t.Fatalf("full mode should always be supported")
	}
	if full.SupportsRunMode(registry.RunModeDiscovery) {
		t.Fatalf("discovery mode should be disabled when discovery is not configured")
	}

	discovery := NewOktaIntegration(nil, "example.okta.com", 1, true)
	if !discovery.SupportsRunMode(registry.RunModeDiscovery) {
		t.Fatalf("discovery mode should be supported when discovery is enabled")
	}
}

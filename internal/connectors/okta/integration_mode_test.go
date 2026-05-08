package okta

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func TestOktaIntegration_SupportsRunMode(t *testing.T) {
	t.Parallel()

	full := NewOktaIntegration(&Client{BaseURL: "https://example.okta.com", Token: "token"}, "example.okta.com", 1, false)
	if !full.SupportsRunMode(registry.RunModeFull) {
		t.Fatalf("full mode should be supported when an API client is configured")
	}
	if full.SupportsRunMode(registry.RunModeDiscovery) {
		t.Fatalf("discovery mode should be disabled when discovery is not configured")
	}

	discovery := NewOktaIntegration(&Client{BaseURL: "https://example.okta.com", Token: "token"}, "example.okta.com", 1, true)
	if !discovery.SupportsRunMode(registry.RunModeDiscovery) {
		t.Fatalf("discovery mode should be supported when discovery polling is enabled")
	}

	pushOnly := NewOktaIntegrationWithDiscoveryPolling(nil, "example.okta.com", 1, true, false)
	if pushOnly.SupportsRunMode(registry.RunModeFull) || pushOnly.SupportsRunMode(registry.RunModeDiscovery) {
		t.Fatalf("push-only integration without an API client should not run full or polling syncs")
	}
}

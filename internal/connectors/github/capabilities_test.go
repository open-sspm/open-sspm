package github

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/capabilities"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func TestGitHubIntegrationCapabilities(t *testing.T) {
	t.Parallel()

	integration := NewGitHubIntegration(&Client{}, "acme", "", 1, false)
	if supported, declared := capabilities.SupportsRunMode(integration, registry.RunModeFull); !declared || !supported {
		t.Fatalf("full capability support = supported %v declared %v, want true/true", supported, declared)
	}
	if supported, declared := capabilities.SupportsRunMode(integration, registry.RunModeTail); !declared || supported {
		t.Fatalf("tail capability support = supported %v declared %v, want false/true", supported, declared)
	}
}

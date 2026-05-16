package vault

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/capabilities"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func TestVaultIntegrationCapabilities(t *testing.T) {
	t.Parallel()

	integration := NewVaultIntegration(&Client{}, "prod", true)
	if supported, declared := capabilities.SupportsRunMode(integration, registry.RunModeFull); !declared || !supported {
		t.Fatalf("full capability support = supported %v declared %v, want true/true", supported, declared)
	}
	if supported, declared := capabilities.SupportsRunMode(integration, registry.RunModeTail); !declared || supported {
		t.Fatalf("tail capability support = supported %v declared %v, want false/true", supported, declared)
	}
}

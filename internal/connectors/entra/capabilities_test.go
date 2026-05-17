package entra

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/capabilities"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func TestEntraIntegrationCapabilitiesDoNotEnableTailYet(t *testing.T) {
	t.Parallel()

	integration := NewEntraIntegration(nil, "tenant", false)
	if supported, declared := capabilities.SupportsRunMode(integration, registry.RunModeTail); !declared || supported {
		t.Fatalf("tail capability support = supported %v declared %v, want false/true", supported, declared)
	}
}

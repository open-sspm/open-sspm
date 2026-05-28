package entra

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/capabilities"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/records"
)

func TestEntraIntegrationCapabilitiesDoNotEnableTailYet(t *testing.T) {
	t.Parallel()

	integration := NewEntraIntegration(nil, "tenant", false)
	if supported, declared := capabilities.SupportsRunMode(integration, registry.RunModeTail); !declared || supported {
		t.Fatalf("tail capability support = supported %v declared %v, want false/true", supported, declared)
	}
}

func TestEntraIntegrationCapabilitiesDescribeDiscovery(t *testing.T) {
	t.Parallel()

	integration := NewEntraIntegration(stubEntraClient{}, "tenant", true)
	caps := integration.Capabilities()
	if caps.Discovery == nil {
		t.Fatalf("discovery capability = nil, want configured")
	}
	if !caps.Discovery.Incremental {
		t.Fatalf("discovery incremental = false, want true")
	}
	if len(caps.Discovery.Resources) != 1 || caps.Discovery.Resources[0].Name != records.ResourceDiscoveryEvidence {
		t.Fatalf("discovery resources = %+v, want discovery evidence", caps.Discovery.Resources)
	}
	if supported, declared := capabilities.SupportsRunMode(integration, registry.RunModeDiscovery); !declared || !supported {
		t.Fatalf("discovery capability support = supported %v declared %v, want true/true", supported, declared)
	}

	descriptor := integration.Descriptor()
	for _, resource := range descriptor.Resources {
		if resource == records.ResourceDiscoveryEvidence {
			return
		}
	}
	t.Fatalf("descriptor resources = %+v, want discovery evidence", descriptor.Resources)
}

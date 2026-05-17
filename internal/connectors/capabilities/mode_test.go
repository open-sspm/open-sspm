package capabilities

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

type modeSupportProvider struct {
	caps Capabilities
}

func (p modeSupportProvider) Capabilities() Capabilities {
	return p.caps
}

func TestSupportsRunModeFromCapabilities(t *testing.T) {
	t.Parallel()

	provider := modeSupportProvider{
		caps: Capabilities{
			Full: &FullCapability{},
			Tail: &TailCapability{},
		},
	}
	if supported, declared := SupportsRunMode(provider, registry.RunModeFull); !declared || !supported {
		t.Fatalf("full support = %v/%v, want declared supported", supported, declared)
	}
	if supported, declared := SupportsRunMode(provider, registry.RunModeTail); !declared || !supported {
		t.Fatalf("tail support = %v/%v, want declared supported", supported, declared)
	}
	if supported, declared := SupportsRunMode(provider, registry.RunModeDiscovery); declared || supported {
		t.Fatalf("discovery support = %v/%v, want not declared", supported, declared)
	}
}

func TestSupportsRunModeReportsDeclaredUnsupported(t *testing.T) {
	t.Parallel()

	provider := modeSupportProvider{caps: Capabilities{Full: &FullCapability{}}}
	if supported, declared := SupportsRunMode(provider, registry.RunModeTail); !declared || supported {
		t.Fatalf("tail support = %v/%v, want declared unsupported", supported, declared)
	}
}

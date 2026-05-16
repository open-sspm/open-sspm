package capabilities

import (
	"strings"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

type CompatibilityFullAdapter struct {
	Integration registry.Integration
}

func NewCompatibilityFullAdapter(integration registry.Integration) CompatibilityFullAdapter {
	return CompatibilityFullAdapter{Integration: integration}
}

func (a CompatibilityFullAdapter) Descriptor() Descriptor {
	if a.Integration == nil {
		return Descriptor{}
	}
	return Descriptor{
		Kind:        strings.TrimSpace(a.Integration.Kind()),
		DisplayName: strings.TrimSpace(a.Integration.Name()),
		Role:        a.Integration.Role(),
	}
}

func (a CompatibilityFullAdapter) Capabilities() Capabilities {
	if a.Integration == nil {
		return Capabilities{}
	}
	return Capabilities{
		Full: &FullCapability{},
	}
}

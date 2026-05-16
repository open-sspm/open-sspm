package aws

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/capabilities"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func TestAWSIntegrationCapabilities(t *testing.T) {
	t.Parallel()

	integration := NewAWSIntegration(&Client{}, "prod")
	if supported, declared := capabilities.SupportsRunMode(integration, registry.RunModeFull); !declared || !supported {
		t.Fatalf("full capability support = supported %v declared %v, want true/true", supported, declared)
	}
	if supported, declared := capabilities.SupportsRunMode(integration, registry.RunModeTail); !declared || supported {
		t.Fatalf("tail capability support = supported %v declared %v, want false/true", supported, declared)
	}

	tailIntegration := NewAWSIntegration(&Client{cloudtrail: fakeCloudTrail{}}, "prod")
	if supported, declared := capabilities.SupportsRunMode(tailIntegration, registry.RunModeTail); !declared || !supported {
		t.Fatalf("tail capability support with CloudTrail = supported %v declared %v, want true/true", supported, declared)
	}
}

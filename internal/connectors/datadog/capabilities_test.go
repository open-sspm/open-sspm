package datadog

import (
	"context"
	"testing"
	"time"

	"github.com/open-sspm/open-sspm/internal/connectors/capabilities"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

type capabilityTestDatadogAdapter struct{}

func (capabilityTestDatadogAdapter) ListAccounts(context.Context) ([]Account, error) {
	return nil, nil
}

func (capabilityTestDatadogAdapter) ListRoles(context.Context) ([]Role, error) {
	return nil, nil
}

func (capabilityTestDatadogAdapter) ListRoleMembers(context.Context, string) ([]string, error) {
	return nil, nil
}

func (capabilityTestDatadogAdapter) ListAuditEvents(context.Context, time.Time) ([]AuditEvent, error) {
	return nil, nil
}

func TestDatadogIntegrationCapabilities(t *testing.T) {
	t.Parallel()

	integration := NewDatadogIntegration(capabilityTestDatadogAdapter{}, "datadoghq.com", 1)
	if supported, declared := capabilities.SupportsRunMode(integration, registry.RunModeFull); !declared || !supported {
		t.Fatalf("full capability support = supported %v declared %v, want true/true", supported, declared)
	}
	if supported, declared := capabilities.SupportsRunMode(integration, registry.RunModeTail); !declared || !supported {
		t.Fatalf("tail capability support = supported %v declared %v, want true/true", supported, declared)
	}
}

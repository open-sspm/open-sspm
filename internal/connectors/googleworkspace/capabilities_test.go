package googleworkspace

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/capabilities"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func TestGoogleWorkspaceIntegrationCapabilities(t *testing.T) {
	t.Parallel()

	integration := NewGoogleWorkspaceIntegration(&Client{}, "C0123", "example.com", false)
	caps := integration.Capabilities()
	if caps.Full == nil {
		t.Fatalf("expected full capability")
	}
	if caps.Tail == nil {
		t.Fatalf("expected Reports tail capability")
	}
	if len(caps.Tail.Resources) != 1 || caps.Tail.Resources[0].ProviderResource != GoogleWorkspaceReportsTailResource {
		t.Fatalf("tail resources = %+v, want provider resource %q", caps.Tail.Resources, GoogleWorkspaceReportsTailResource)
	}
	if caps.Tail.CursorKind != capabilities.CursorKindWatermarkOverlap {
		t.Fatalf("tail cursor kind = %q, want %q", caps.Tail.CursorKind, capabilities.CursorKindWatermarkOverlap)
	}
	if supported, declared := capabilities.SupportsRunMode(integration, registry.RunModeTail); !declared || !supported {
		t.Fatalf("capability mode support tail = supported %v declared %v, want true/true", supported, declared)
	}
}

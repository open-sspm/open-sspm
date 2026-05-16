package okta

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/capabilities"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func TestOktaIntegrationCapabilitiesDescribePushTailAndFull(t *testing.T) {
	t.Parallel()

	integration := NewOktaIntegration(&Client{BaseURL: "https://example.okta.com", Token: "token"}, "example.okta.com", 1, false)
	caps := integration.Capabilities()
	if caps.Push == nil || len(caps.Push.Channels) != 2 {
		t.Fatalf("push capability = %+v, want two channels", caps.Push)
	}
	if caps.Tail == nil || caps.Tail.CursorKind != capabilities.CursorKindWatermarkOverlap {
		t.Fatalf("tail capability = %+v, want watermark-overlap tail", caps.Tail)
	}
	if caps.Full == nil {
		t.Fatalf("full capability = nil, want configured")
	}
	if supported, declared := capabilities.SupportsRunMode(integration, registry.RunModeTail); !declared || !supported {
		t.Fatalf("tail support = %v/%v, want declared supported", supported, declared)
	}
}

func TestOktaIntegrationCapabilitiesKeepPushWithoutAPIClient(t *testing.T) {
	t.Parallel()

	integration := NewOktaIntegration(nil, "example.okta.com", 1, false)
	caps := integration.Capabilities()
	if caps.Push == nil {
		t.Fatalf("push capability = nil, want configured")
	}
	if caps.Tail != nil {
		t.Fatalf("tail capability = %+v, want nil without API client", caps.Tail)
	}
	if caps.Full != nil {
		t.Fatalf("full capability = %+v, want nil without API client", caps.Full)
	}
	if supported, declared := capabilities.SupportsRunMode(integration, registry.RunModeTail); !declared || supported {
		t.Fatalf("tail support = %v/%v, want declared unsupported", supported, declared)
	}
}

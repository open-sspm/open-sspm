package okta

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/capabilities"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/records"
)

func TestOktaIntegrationCapabilitiesDescribePushTailAndFull(t *testing.T) {
	t.Parallel()

	integration := NewOktaIntegration(&Client{BaseURL: "https://example.okta.com", Token: "token"}, "example.okta.com", 1, false)
	caps := integration.Capabilities()
	if caps.Push == nil || len(caps.Push.Channels) != 2 {
		t.Fatalf("push capability = %+v, want two channels", caps.Push)
	}
	assertPushCapabilityChannel(t, caps.Push, PushChannelEventHook)
	assertPushCapabilityChannel(t, caps.Push, PushChannelEventBridge)
	if caps.Tail == nil || caps.Tail.CursorKind != capabilities.CursorKindWatermarkOverlap {
		t.Fatalf("tail capability = %+v, want watermark-overlap tail", caps.Tail)
	}
	if caps.Full == nil {
		t.Fatalf("full capability = nil, want configured")
	}
	assertFullCapabilityResource(t, caps.Full, records.ResourceIdentity, capabilities.SnapshotComplete, true)
	assertFullCapabilityResource(t, caps.Full, records.ResourceGroup, capabilities.SnapshotComplete, true)
	assertFullCapabilityResource(t, caps.Full, records.ResourceApplication, capabilities.SnapshotComplete, true)
	assertFullCapabilityResource(t, caps.Full, records.ResourceEntitlement, capabilities.SnapshotComplete, true)
	if supported, declared := capabilities.SupportsRunMode(integration, registry.RunModeTail); !declared || !supported {
		t.Fatalf("tail support = %v/%v, want declared supported", supported, declared)
	}

	discoveryIntegration := NewOktaIntegration(&Client{BaseURL: "https://example.okta.com", Token: "token"}, "example.okta.com", 1, true)
	discoveryCaps := discoveryIntegration.Capabilities()
	assertDiscoveryCapabilityResource(t, discoveryCaps.Discovery, records.ResourceDiscoveryEvidence)
	if supported, declared := capabilities.SupportsRunMode(discoveryIntegration, registry.RunModeDiscovery); !declared || !supported {
		t.Fatalf("discovery support = %v/%v, want declared supported", supported, declared)
	}

	descriptor := integration.Descriptor()
	assertDescriptorResource(t, descriptor, records.ResourceEntitlement)
	assertDescriptorResource(t, descriptor, records.ResourceDiscoveryEvidence)
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
	if caps.Discovery != nil {
		t.Fatalf("discovery capability = %+v, want nil without API client", caps.Discovery)
	}
	if supported, declared := capabilities.SupportsRunMode(integration, registry.RunModeTail); !declared || supported {
		t.Fatalf("tail support = %v/%v, want declared unsupported", supported, declared)
	}
}

func TestOktaPushChannelValidation(t *testing.T) {
	t.Parallel()

	for _, channel := range []string{PushChannelEventHook, PushChannelEventBridge, " " + PushChannelEventHook + " "} {
		if !IsPushChannel(channel) {
			t.Fatalf("IsPushChannel(%q) = false, want true", channel)
		}
	}
	if IsPushChannel("polling") {
		t.Fatal("IsPushChannel(polling) = true, want false")
	}
}

func assertPushCapabilityChannel(t *testing.T, cap *capabilities.PushCapability, channel string) {
	t.Helper()
	for _, got := range cap.Channels {
		if got.Name == channel {
			return
		}
	}
	t.Fatalf("push capability missing channel %s", channel)
}

func assertFullCapabilityResource(t *testing.T, cap *capabilities.FullCapability, resource records.ResourceName, completeness capabilities.SnapshotCompleteness, expireAbsent bool) {
	t.Helper()
	for _, got := range cap.Resources {
		if got.Name == resource {
			if got.SnapshotCompleteness != completeness {
				t.Fatalf("%s snapshot completeness = %q, want %q", resource, got.SnapshotCompleteness, completeness)
			}
			if got.ExpireAbsentAllowed != expireAbsent {
				t.Fatalf("%s expire absent = %v, want %v", resource, got.ExpireAbsentAllowed, expireAbsent)
			}
			return
		}
	}
	t.Fatalf("full capability missing resource %s", resource)
}

func assertDiscoveryCapabilityResource(t *testing.T, cap *capabilities.DiscoveryCapability, resource records.ResourceName) {
	t.Helper()
	if cap == nil {
		t.Fatalf("discovery capability = nil, want configured")
	}
	if !cap.Incremental {
		t.Fatalf("discovery capability incremental = false, want true")
	}
	for _, got := range cap.Resources {
		if got.Name == resource {
			if len(got.SignalKinds) == 0 {
				t.Fatalf("discovery resource %s has no signal kinds", resource)
			}
			return
		}
	}
	t.Fatalf("discovery capability missing resource %s", resource)
}

func assertDescriptorResource(t *testing.T, descriptor capabilities.Descriptor, resource records.ResourceName) {
	t.Helper()
	for _, got := range descriptor.Resources {
		if got == resource {
			return
		}
	}
	t.Fatalf("descriptor missing resource %s", resource)
}

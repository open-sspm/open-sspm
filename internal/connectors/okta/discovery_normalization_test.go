package okta

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/open-sspm/open-sspm/internal/discovery"
)

func TestNormalizeOktaDiscoveryVendorName(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)

	t.Run("falls back to app name when domain vendor is unavailable", func(t *testing.T) {
		t.Parallel()

		sources, events := NormalizeDiscoveryEvents([]SystemLogEvent{
			{
				ID:        "evt-1",
				EventType: "user.authentication.sso",
				Published: now,
				AppID:     "0oa1",
				AppName:   "Payroll Tool",
			},
		}, "dev-123.okta.com", now)

		if len(sources) != 1 {
			t.Fatalf("len(sources) = %d, want 1", len(sources))
		}
		if got := sources[0].SourceVendorName; got != "Payroll Tool" {
			t.Fatalf("sources[0].SourceVendorName = %q, want %q", got, "Payroll Tool")
		}
		if len(events) != 1 {
			t.Fatalf("len(events) = %d, want 1", len(events))
		}
		if got := events[0].SourceVendorName; got != "Payroll Tool" {
			t.Fatalf("events[0].SourceVendorName = %q, want %q", got, "Payroll Tool")
		}
	})

	t.Run("keeps domain-derived vendor when domain is present", func(t *testing.T) {
		t.Parallel()

		sources, events := NormalizeDiscoveryEvents([]SystemLogEvent{
			{
				ID:        "evt-2",
				EventType: "user.authentication.sso",
				Published: now,
				AppID:     "0oa2",
				AppName:   "Payroll Tool",
				AppDomain: "https://sub.acme.com/path",
			},
		}, "dev-123.okta.com", now)

		if len(sources) != 1 {
			t.Fatalf("len(sources) = %d, want 1", len(sources))
		}
		if got := sources[0].SourceVendorName; got != "Acme" {
			t.Fatalf("sources[0].SourceVendorName = %q, want %q", got, "Acme")
		}
		if len(events) != 1 {
			t.Fatalf("len(events) = %d, want 1", len(events))
		}
		if got := events[0].SourceVendorName; got != "Acme" {
			t.Fatalf("events[0].SourceVendorName = %q, want %q", got, "Acme")
		}
	})

	t.Run("carries inferred app category on sources and events", func(t *testing.T) {
		t.Parallel()

		sources, events := NormalizeDiscoveryEvents([]SystemLogEvent{
			{
				ID:        "evt-3",
				EventType: "user.authentication.sso",
				Published: now,
				AppID:     "0oa3",
				AppName:   "GitHub Enterprise",
				AppDomain: "https://github.com/login",
			},
		}, "dev-123.okta.com", now)

		if len(sources) != 1 {
			t.Fatalf("len(sources) = %d, want 1", len(sources))
		}
		if got := sources[0].SourceCategory; got != "developer_tools" {
			t.Fatalf("sources[0].SourceCategory = %q, want %q", got, "developer_tools")
		}
		if len(events) != 1 {
			t.Fatalf("len(events) = %d, want 1", len(events))
		}
		if got := events[0].SourceCategory; got != "developer_tools" {
			t.Fatalf("events[0].SourceCategory = %q, want %q", got, "developer_tools")
		}
	})
}

func TestOktaDiscoveryFixturesNormalize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		path           string
		eventHook      bool
		wantExternalID string
		wantSignalKind string
	}{
		{
			name:           "event hook sso",
			path:           "event_hook_sso.json",
			eventHook:      true,
			wantExternalID: "evt-sso-1",
			wantSignalKind: discovery.SignalKindIDPSSO,
		},
		{
			name:           "event hook consent",
			path:           "event_hook_consent.json",
			eventHook:      true,
			wantExternalID: "evt-consent-1",
			wantSignalKind: discovery.SignalKindOAuth,
		},
		{
			name:           "event hook membership",
			path:           "event_hook_membership.json",
			eventHook:      true,
			wantExternalID: "evt-membership-1",
			wantSignalKind: discovery.SignalKindAssignment,
		},
		{
			name:           "system log oauth signon",
			path:           "systemlog_app_oauth2_signon.json",
			wantExternalID: "evt-systemlog-signon-1",
			wantSignalKind: discovery.SignalKindIDPSSO,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			raw := readOktaFixture(t, tc.path)
			if tc.eventHook {
				var envelope struct {
					Data struct {
						Events []json.RawMessage `json:"events"`
					} `json:"data"`
				}
				if err := json.Unmarshal(raw, &envelope); err != nil {
					t.Fatalf("decode fixture envelope: %v", err)
				}
				if len(envelope.Data.Events) != 1 {
					t.Fatalf("fixture events = %d, want 1", len(envelope.Data.Events))
				}
				raw = envelope.Data.Events[0]
			}

			event, err := MapSystemLogEventJSON(raw)
			if err != nil {
				t.Fatalf("MapSystemLogEventJSON(): %v", err)
			}
			_, events := NormalizeDiscoveryEvents([]SystemLogEvent{event}, "dev-123.okta.com", time.Now().UTC())
			if len(events) != 1 {
				t.Fatalf("len(events) = %d, want 1", len(events))
			}
			if events[0].EventExternalID != tc.wantExternalID {
				t.Fatalf("event_external_id = %q, want %q", events[0].EventExternalID, tc.wantExternalID)
			}
			if events[0].SignalKind != tc.wantSignalKind {
				t.Fatalf("signal_kind = %q, want %q", events[0].SignalKind, tc.wantSignalKind)
			}
		})
	}
}

func TestOktaEventBridgeFixtureNormalizesDetail(t *testing.T) {
	t.Parallel()

	var envelope struct {
		Detail json.RawMessage `json:"detail"`
	}
	if err := json.Unmarshal(readOktaFixture(t, "eventbridge_systemlog.json"), &envelope); err != nil {
		t.Fatalf("decode EventBridge fixture: %v", err)
	}
	event, err := MapSystemLogEventJSON(envelope.Detail)
	if err != nil {
		t.Fatalf("MapSystemLogEventJSON(): %v", err)
	}
	_, events := NormalizeDiscoveryEvents([]SystemLogEvent{event}, "dev-123.okta.com", time.Now().UTC())
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(events))
	}
	if events[0].EventExternalID != "evt-eventbridge-signon-1" {
		t.Fatalf("event_external_id = %q, want evt-eventbridge-signon-1", events[0].EventExternalID)
	}
	if events[0].SignalKind != discovery.SignalKindIDPSSO {
		t.Fatalf("signal_kind = %q, want %q", events[0].SignalKind, discovery.SignalKindIDPSSO)
	}
}

func readOktaFixture(t *testing.T, name string) []byte {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return raw
}

func TestMapSystemLogEventJSON(t *testing.T) {
	t.Parallel()

	raw := []byte(`{
		"uuid": "evt-json-1",
		"eventType": "user.authentication.sso",
		"published": "2026-01-01T12:00:00Z",
		"outcome": {"result": "SUCCESS", "reason": "OK"},
		"actor": {
			"id": "00u1",
			"alternateId": "alice@example.com",
			"displayName": "Alice Example"
		},
		"target": [{
			"id": "0oa1",
			"type": "AppInstance",
			"alternateId": "https://app.example.com/login",
			"displayName": "Example App",
			"detailEntry": {
				"appId": "0oa-detail",
				"appName": "Detail App",
				"domain": "https://detail.example.com"
			}
		}],
		"debugContext": {
			"debugData": {
				"scopes": "openid profile email"
			}
		}
	}`)

	event, err := MapSystemLogEventJSON(raw)
	if err != nil {
		t.Fatalf("MapSystemLogEventJSON(): %v", err)
	}

	if event.ID != "evt-json-1" {
		t.Fatalf("ID = %q, want %q", event.ID, "evt-json-1")
	}
	if event.EventType != "user.authentication.sso" {
		t.Fatalf("EventType = %q, want user.authentication.sso", event.EventType)
	}
	if event.AppID != "0oa1" {
		t.Fatalf("AppID = %q, want %q", event.AppID, "0oa1")
	}
	if event.AppName != "Example App" {
		t.Fatalf("AppName = %q, want %q", event.AppName, "Example App")
	}
	if event.AppDomain != "https://app.example.com/login" {
		t.Fatalf("AppDomain = %q, want target alternateId", event.AppDomain)
	}
	if event.ActorEmail != "alice@example.com" {
		t.Fatalf("ActorEmail = %q, want alice@example.com", event.ActorEmail)
	}
	if event.OutcomeResult != "SUCCESS" || event.OutcomeReason != "OK" {
		t.Fatalf("outcome = %q/%q, want SUCCESS/OK", event.OutcomeResult, event.OutcomeReason)
	}
	if got, want := event.GrantedScopes, []string{"openid", "profile", "email"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("GrantedScopes = %#v, want %#v", got, want)
	}
}

func TestNormalizeOktaDiscoverySignalKinds(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	_, events := NormalizeDiscoveryEvents([]SystemLogEvent{
		{
			ID:        "sso-1",
			EventType: "user.authentication.sso",
			Published: now,
			AppID:     "0oa-sso",
			AppName:   "SSO App",
		},
		{
			ID:        "oauth-1",
			EventType: "app.oauth2.as.consent.grant",
			Published: now,
			AppID:     "0oa-oauth",
			AppName:   "OAuth App",
		},
		{
			ID:        "assign-1",
			EventType: "application.user_membership.add",
			Published: now,
			AppID:     "0oa-assign",
			AppName:   "Assigned App",
		},
		{
			ID:        "ignored-1",
			EventType: "policy.lifecycle.update",
			Published: now,
			AppID:     "0oa-policy",
			AppName:   "Policy App",
		},
		{
			ID:        "no-app",
			EventType: "user.authentication.sso",
			Published: now,
		},
	}, "dev-123.okta.com", now)

	if len(events) != 3 {
		t.Fatalf("len(events) = %d, want 3", len(events))
	}
	got := map[string]string{}
	for _, event := range events {
		got[event.EventExternalID] = event.SignalKind
	}
	if got["sso-1"] != discovery.SignalKindIDPSSO {
		t.Fatalf("sso signal = %q, want %q", got["sso-1"], discovery.SignalKindIDPSSO)
	}
	if got["oauth-1"] != discovery.SignalKindOAuth {
		t.Fatalf("oauth signal = %q, want %q", got["oauth-1"], discovery.SignalKindOAuth)
	}
	if got["assign-1"] != discovery.SignalKindAssignment {
		t.Fatalf("assignment signal = %q, want %q", got["assign-1"], discovery.SignalKindAssignment)
	}
	if got["ignored-1"] != "" {
		t.Fatalf("ignored signal = %q, want absent", got["ignored-1"])
	}
	if got["no-app"] != "" {
		t.Fatalf("no-app signal = %q, want absent", got["no-app"])
	}
}

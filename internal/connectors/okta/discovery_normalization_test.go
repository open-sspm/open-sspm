package okta

import (
	"testing"
	"time"
)

func TestNormalizeOktaDiscoveryVendorName(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)

	t.Run("falls back to app name when domain vendor is unavailable", func(t *testing.T) {
		t.Parallel()

		sources, events := normalizeOktaDiscovery([]SystemLogEvent{
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

		sources, events := normalizeOktaDiscovery([]SystemLogEvent{
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
}

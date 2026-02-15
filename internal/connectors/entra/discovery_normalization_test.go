package entra

import (
	"testing"
	"time"
)

func TestNormalizeEntraDiscovery_VendorPrecedence(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.February, 10, 12, 0, 0, 0, time.UTC)
	signIns := []SignInEvent{
		{
			ID:                 "signin-1",
			CreatedDateTimeRaw: "2026-02-10T10:00:00Z",
			AppID:              "client-app-1",
			AppDisplayName:     "App One",
			UserID:             "user-1",
		},
		{
			ID:                 "signin-2",
			CreatedDateTimeRaw: "2026-02-10T11:00:00Z",
			AppID:              "client-app-2",
			AppDisplayName:     "",
			UserID:             "user-2",
		},
	}
	grants := []OAuth2PermissionGrant{
		{
			ID:                 "grant-1",
			ClientID:           "sp-3",
			PrincipalID:        "principal-1",
			Scope:              "User.Read",
			CreatedDateTimeRaw: "2026-02-10T09:00:00Z",
		},
	}
	applications := []Application{
		{
			AppID:             "client-app-1",
			DisplayName:       "App One",
			PublisherDomain:   "one.example.com",
			VerifiedPublisher: VerifiedPublisher{DisplayName: "Verified One"},
		},
		{
			AppID:           "client-app-2",
			DisplayName:     "App Two",
			PublisherDomain: "jira.com",
		},
		{
			AppID:       "client-app-3",
			DisplayName: "App Three",
		},
	}
	servicePrincipals := []ServicePrincipal{
		{
			ID:            "sp-3",
			AppID:         "client-app-3",
			DisplayName:   "Service Principal Three",
			PublisherName: "Publisher Three",
		},
	}

	sources, events := normalizeEntraDiscovery(signIns, grants, applications, servicePrincipals, "tenant-1", now)
	if len(sources) != 3 {
		t.Fatalf("len(sources)=%d want 3", len(sources))
	}
	if len(events) != 3 {
		t.Fatalf("len(events)=%d want 3", len(events))
	}

	sourceByAppID := map[string]normalizedDiscoverySource{}
	for _, source := range sources {
		sourceByAppID[source.SourceAppID] = source
	}

	if got := sourceByAppID["client-app-1"].SourceVendorName; got != "Verified One" {
		t.Fatalf("verified publisher vendor = %q want %q", got, "Verified One")
	}
	if got := sourceByAppID["client-app-2"].SourceVendorName; got != "Jira" {
		t.Fatalf("publisher domain vendor = %q want %q", got, "Jira")
	}
	if got := sourceByAppID["client-app-3"].SourceVendorName; got != "Publisher Three" {
		t.Fatalf("service principal vendor = %q want %q", got, "Publisher Three")
	}

	eventByID := map[string]normalizedDiscoveryEvent{}
	for _, event := range events {
		eventByID[event.EventExternalID] = event
	}
	if got := eventByID["signin-1"].SourceVendorName; got != "Verified One" {
		t.Fatalf("signin verified vendor = %q want %q", got, "Verified One")
	}
	if got := eventByID["signin-2"].SourceVendorName; got != "Jira" {
		t.Fatalf("signin domain vendor = %q want %q", got, "Jira")
	}
	if got := eventByID["grant-1"].SourceVendorName; got != "Publisher Three" {
		t.Fatalf("grant service principal vendor = %q want %q", got, "Publisher Three")
	}
}

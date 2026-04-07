package entra

import (
	"testing"
	"time"
)

func TestNormalizeEntraDiscovery_VendorPrecedence(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.February, 10, 12, 0, 0, 0, time.UTC)
	signIns := []SignInEvent{
		mustParseSignIn(t, `{"id":"signin-1","createdDateTime":"2026-02-10T10:00:00Z","appId":"client-app-1","appDisplayName":"App One","userId":"user-1"}`),
		mustParseSignIn(t, `{"id":"signin-2","createdDateTime":"2026-02-10T11:00:00Z","appId":"client-app-2","appDisplayName":"","userId":"user-2"}`),
	}
	grants := []OAuth2PermissionGrant{
		mustParseGrant(t, `{"id":"grant-1","clientId":"sp-3","principalId":"principal-1","scope":"User.Read"}`),
	}
	applications := []Application{
		mustParseApplication(t, `{"id":"app-1","appId":"client-app-1","displayName":"App One","publisherDomain":"one.example.com","verifiedPublisher":{"displayName":"Verified One"}}`),
		mustParseApplication(t, `{"id":"app-2","appId":"client-app-2","displayName":"App Two","publisherDomain":"jira.com"}`),
		mustParseApplication(t, `{"id":"app-3","appId":"client-app-3","displayName":"App Three"}`),
	}
	servicePrincipals := []ServicePrincipal{
		mustParseServicePrincipal(t, `{"id":"sp-3","appId":"client-app-3","displayName":"Service Principal Three","publisherName":"Publisher Three"}`),
	}

	sources, events := normalizeEntraDiscovery(signIns, grants, applications, servicePrincipals, nil, "tenant-1", now)
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

func TestNormalizeEntraDiscovery_GrantActorResolution(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.April, 6, 12, 0, 0, 0, time.UTC)
	grants := []OAuth2PermissionGrant{
		mustParseGrant(t, `{"id":"grant-user","clientId":"sp-1","consentType":"Principal","principalId":"user-1","scope":"User.Read"}`),
		mustParseGrant(t, `{"id":"grant-admin","clientId":"sp-1","consentType":"AllPrincipals","principalId":"","scope":"Files.Read"}`),
		mustParseGrant(t, `{"id":"grant-fallback","clientId":"sp-1","consentType":"Principal","principalId":"missing-user-1","scope":"Mail.Read"}`),
	}
	servicePrincipals := []ServicePrincipal{
		mustParseServicePrincipal(t, `{"id":"sp-1","appId":"client-app-1","displayName":"App One SP"}`),
	}
	users := []User{
		mustParseUser(t, `{"id":"user-1","displayName":"Alice Example","userPrincipalName":"alice@example.com"}`),
	}

	_, events := normalizeEntraDiscovery(nil, grants, nil, servicePrincipals, users, "tenant-1", now)
	if len(events) != 3 {
		t.Fatalf("len(events)=%d want 3", len(events))
	}

	eventByID := map[string]normalizedDiscoveryEvent{}
	for _, event := range events {
		eventByID[event.EventExternalID] = event
	}

	if got := eventByID["grant-user"].ActorDisplayName; got != "Alice Example" {
		t.Fatalf("grant user actor display name = %q want %q", got, "Alice Example")
	}
	if got := eventByID["grant-user"].ActorEmail; got != "alice@example.com" {
		t.Fatalf("grant user actor email = %q want %q", got, "alice@example.com")
	}
	if got := eventByID["grant-user"].ActorExternalID; got != "user-1" {
		t.Fatalf("grant user actor external id = %q want %q", got, "user-1")
	}
	if got := eventByID["grant-admin"].ActorDisplayName; got != "All principals" {
		t.Fatalf("grant admin actor display name = %q want %q", got, "All principals")
	}
	if got := eventByID["grant-fallback"].ActorDisplayName; got != "missing-user-1" {
		t.Fatalf("grant fallback actor display name = %q want %q", got, "missing-user-1")
	}
}

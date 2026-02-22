package googleworkspace

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func TestGoogleWorkspaceGrantExternalIDIsDeterministic(t *testing.T) {
	t.Parallel()

	id1 := googleWorkspaceGrantExternalID("client-1", "user-1")
	id2 := googleWorkspaceGrantExternalID("client-1", "user-1")
	if id1 != id2 {
		t.Fatalf("grant external id should be deterministic: %q != %q", id1, id2)
	}
	id3 := googleWorkspaceGrantExternalID("client-1", "user-2")
	if id1 == id3 {
		t.Fatalf("grant external id should differ when user differs: %q", id1)
	}
}

func TestGoogleWorkspaceClientExternalIDFallsBackToSynthetic(t *testing.T) {
	t.Parallel()

	grant := WorkspaceOAuthTokenGrant{
		UserKey:     "user-1",
		DisplayText: "Example App",
		Scopes:      []string{"scope.a", "scope.b"},
	}
	got := googleWorkspaceClientExternalID(grant)
	if got == "" {
		t.Fatalf("expected synthetic client id")
	}
	if got == grant.ClientID {
		t.Fatalf("synthetic client id should not match empty client id")
	}
}

func TestBuildGoogleWorkspaceAccountRowsMapsUsersAndGroups(t *testing.T) {
	t.Parallel()

	users := []WorkspaceUser{
		{
			ID:           "u-1",
			PrimaryEmail: "Alice@Example.com",
			Name: struct {
				FullName string `json:"fullName"`
			}{FullName: "Alice"},
		},
		{
			ID:           "u-2",
			PrimaryEmail: "svc@example.iam.gserviceaccount.com",
			Suspended:    true,
		},
	}
	groups := []WorkspaceGroup{
		{
			ID:    "g-1",
			Email: "team@example.com",
			Name:  "Team Group",
		},
	}

	rows := buildGoogleWorkspaceAccountRows(users, groups)
	if len(rows) != 3 {
		t.Fatalf("len(rows) = %d, want 3", len(rows))
	}

	rowByExternalID := make(map[string]googleWorkspaceAccountRow, len(rows))
	for _, row := range rows {
		rowByExternalID[row.ExternalID] = row
	}

	userRow := rowByExternalID["u-1"]
	if userRow.Email != "alice@example.com" {
		t.Fatalf("user email = %q, want %q", userRow.Email, "alice@example.com")
	}
	if userRow.Status != "active" {
		t.Fatalf("user status = %q, want %q", userRow.Status, "active")
	}
	if userRow.AccountKind != registry.AccountKindHuman {
		t.Fatalf("user account kind = %q, want %q", userRow.AccountKind, registry.AccountKindHuman)
	}

	serviceRow := rowByExternalID["u-2"]
	if serviceRow.AccountKind != registry.AccountKindService {
		t.Fatalf("service account kind = %q, want %q", serviceRow.AccountKind, registry.AccountKindService)
	}
	if serviceRow.Status != "suspended" {
		t.Fatalf("service status = %q, want %q", serviceRow.Status, "suspended")
	}

	groupRow := rowByExternalID["g-1"]
	if groupRow.Email != "team@example.com" {
		t.Fatalf("group email = %q, want %q", groupRow.Email, "team@example.com")
	}
	if groupRow.AccountKind != registry.AccountKindUnknown {
		t.Fatalf("group account kind = %q, want %q", groupRow.AccountKind, registry.AccountKindUnknown)
	}
	if groupRow.Status != "active" {
		t.Fatalf("group status = %q, want %q", groupRow.Status, "active")
	}
}

func TestBuildGoogleWorkspaceAdminRoleEntitlementsMapsAssignments(t *testing.T) {
	t.Parallel()

	roles := []WorkspaceAdminRole{
		{RoleID: "role-1", RoleName: "Security Admin"},
	}
	assignments := []WorkspaceAdminRoleAssignment{
		{
			RoleID:       "role-1",
			AssignedTo:   "user-1",
			AssigneeType: "USER",
			ScopeType:    "",
		},
	}

	rows := buildGoogleWorkspaceAdminRoleEntitlements(roles, assignments)
	if len(rows) != 1 {
		t.Fatalf("len(rows) = %d, want 1", len(rows))
	}

	row := rows[0]
	if row.AppUserExternalID != "user-1" {
		t.Fatalf("app user external id = %q, want %q", row.AppUserExternalID, "user-1")
	}
	if row.Kind != "google_admin_role" {
		t.Fatalf("kind = %q, want %q", row.Kind, "google_admin_role")
	}
	if row.Resource != "google_admin_role:role-1" {
		t.Fatalf("resource = %q, want %q", row.Resource, "google_admin_role:role-1")
	}
	if row.Permission != "global" {
		t.Fatalf("permission = %q, want %q", row.Permission, "global")
	}
}

func TestBuildOAuthInventoryRowsMapsAssetsOwnersAndCredentials(t *testing.T) {
	t.Parallel()

	integration := NewGoogleWorkspaceIntegration(nil, "C0123", "example.com", true)
	grants := []WorkspaceOAuthTokenGrant{
		{
			UserKey:     "u-1",
			ClientID:    "client-1",
			DisplayText: "App One",
			Scopes:      []string{"scope.b", "scope.a", "scope.a"},
		},
		{
			UserKey:     "u-2",
			ClientID:    "client-1",
			DisplayText: "App One",
			Scopes:      []string{"scope.a"},
		},
	}
	users := []WorkspaceUser{
		{
			ID:           "u-1",
			PrimaryEmail: "Owner@One.com",
			Name: struct {
				FullName string `json:"fullName"`
			}{FullName: "Owner One"},
		},
	}

	assets, owners, credentials := integration.buildOAuthInventoryRows(grants, users)
	if len(assets) != 1 {
		t.Fatalf("len(assets) = %d, want 1", len(assets))
	}
	if len(owners) != 2 {
		t.Fatalf("len(owners) = %d, want 2", len(owners))
	}
	if len(credentials) != 2 {
		t.Fatalf("len(credentials) = %d, want 2", len(credentials))
	}

	if assets[0].AssetKind != "google_oauth_client" || assets[0].ExternalID != "client-1" {
		t.Fatalf("unexpected asset row: %#v", assets[0])
	}

	ownerByExternalID := make(map[string]googleWorkspaceAppAssetOwnerRow, len(owners))
	for _, owner := range owners {
		ownerByExternalID[owner.OwnerExternalID] = owner
	}
	if ownerByExternalID["u-1"].OwnerEmail != "owner@one.com" {
		t.Fatalf("owner email = %q, want %q", ownerByExternalID["u-1"].OwnerEmail, "owner@one.com")
	}
	if ownerByExternalID["u-1"].OwnerDisplayName != "Owner One" {
		t.Fatalf("owner display = %q, want %q", ownerByExternalID["u-1"].OwnerDisplayName, "Owner One")
	}
	if ownerByExternalID["u-2"].OwnerEmail != "u-2" {
		t.Fatalf("fallback owner email = %q, want %q", ownerByExternalID["u-2"].OwnerEmail, "u-2")
	}

	for _, cred := range credentials {
		if cred.AssetRefKind != "google_oauth_client" {
			t.Fatalf("credential asset ref kind = %q, want %q", cred.AssetRefKind, "google_oauth_client")
		}
		if cred.AssetRefExternalID != "google_oauth_client:client-1" {
			t.Fatalf("credential asset ref external id = %q, want %q", cred.AssetRefExternalID, "google_oauth_client:client-1")
		}
		if !strings.HasPrefix(cred.ExternalID, "grant:client-1:") {
			t.Fatalf("credential external id = %q, expected grant prefix", cred.ExternalID)
		}
	}
}

func TestBuildGoogleWorkspaceAuditEventRowsMapsTokenActivity(t *testing.T) {
	t.Parallel()

	var withEvent WorkspaceActivity
	if err := json.Unmarshal([]byte(`{
		"id":{"time":"2026-02-20T10:00:00Z","uniqueQualifier":"uq-1"},
		"actor":{"email":"Admin@Example.com","profileId":"p-1"},
		"events":[{"name":"authorize","parameters":[{"name":"client_id","value":"client-1"},{"name":"display_name","value":"Example App"}]}]
	}`), &withEvent); err != nil {
		t.Fatalf("unmarshal withEvent: %v", err)
	}
	withEvent.RawJSON = []byte(`{"event":"with"}`)

	var withoutEvent WorkspaceActivity
	if err := json.Unmarshal([]byte(`{
		"id":{"time":"2026-02-20T10:01:00Z","uniqueQualifier":"uq-2"},
		"actor":{"email":"Admin@Example.com","profileId":"p-1"},
		"events":[{"parameters":[{"name":"client_id","value":"client-1"},{"name":"display_name","value":"Example App"}]}]
	}`), &withoutEvent); err != nil {
		t.Fatalf("unmarshal withoutEvent: %v", err)
	}
	withoutEvent.Events[0].Name = ""
	withoutEvent.Events[0].Type = ""
	withoutEvent.RawJSON = []byte(`{"event":"without"}`)

	rows := buildGoogleWorkspaceAuditEventRows([]WorkspaceActivity{withEvent, withoutEvent})
	if len(rows) != 2 {
		t.Fatalf("len(rows) = %d, want 2", len(rows))
	}

	first := rows[0]
	if first.EventType != "authorize" {
		t.Fatalf("first event type = %q, want %q", first.EventType, "authorize")
	}
	if first.TargetExternalID != "client-1" {
		t.Fatalf("first target external id = %q, want %q", first.TargetExternalID, "client-1")
	}
	if first.ActorExternalID != "p-1" {
		t.Fatalf("first actor external id = %q, want %q", first.ActorExternalID, "p-1")
	}
	if first.CredentialExternalID != googleWorkspaceGrantExternalID("client-1", "p-1") {
		t.Fatalf("first credential external id = %q", first.CredentialExternalID)
	}

	second := rows[1]
	if second.EventType != "token.activity" {
		t.Fatalf("second event type = %q, want %q", second.EventType, "token.activity")
	}
}

func TestNormalizeDiscoverySourceFromActivity(t *testing.T) {
	t.Parallel()

	var activity WorkspaceActivity
	if err := json.Unmarshal([]byte(`{
		"events":[{"parameters":[
			{"name":"clientId","value":"client-123"},
			{"name":"application_name","value":"App Name"},
			{"name":"app_domain","value":"example.com"}
		]}]
	}`), &activity); err != nil {
		t.Fatalf("unmarshal activity: %v", err)
	}

	appID, appName, appDomain := discoverySourceFromActivity(activity)
	if appID != "client-123" {
		t.Fatalf("appID = %q, want %q", appID, "client-123")
	}
	if appName != "App Name" {
		t.Fatalf("appName = %q, want %q", appName, "App Name")
	}
	if appDomain != "example.com" {
		t.Fatalf("appDomain = %q, want %q", appDomain, "example.com")
	}
}

func TestNewGoogleWorkspaceIntegrationIncludesDiscoverySourceName(t *testing.T) {
	t.Parallel()

	integration := NewGoogleWorkspaceIntegration(nil, "C0123", "example.com", true)
	if integration.Kind() != configstore.KindGoogleWorkspace {
		t.Fatalf("kind = %q, want %q", integration.Kind(), configstore.KindGoogleWorkspace)
	}
	if integration.Name() != "C0123" {
		t.Fatalf("name = %q, want %q", integration.Name(), "C0123")
	}
}

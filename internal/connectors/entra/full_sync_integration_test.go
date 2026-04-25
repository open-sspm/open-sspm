package entra

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/discovery"
	"github.com/open-sspm/open-sspm/internal/testdb"
)

type fullSyncTestClient struct {
	applications               []Application
	servicePrincipals          []ServicePrincipal
	assignmentsBySP            map[string][]ServicePrincipalAppRoleAssignment
	directoryRoles             []DirectoryRole
	directoryRoleAssignments   []DirectoryRoleAssignment
	users                      []User
	groups                     []Group
	groupMembersByID           map[string][]User
	applicationOwnersByID      map[string][]DirectoryOwner
	servicePrincipalOwnersByID map[string][]DirectoryOwner
	directoryAudits            []DirectoryAuditEvent
	signIns                    []SignInEvent
	grants                     []OAuth2PermissionGrant
	lookupUsersByID            map[string]User
}

func (c fullSyncTestClient) ListApplications(context.Context) ([]Application, error) {
	return c.applications, nil
}

func (c fullSyncTestClient) ListServicePrincipals(context.Context) ([]ServicePrincipal, error) {
	return c.servicePrincipals, nil
}

func (c fullSyncTestClient) ListServicePrincipalAssignedTo(_ context.Context, servicePrincipalID string) ([]ServicePrincipalAppRoleAssignment, error) {
	return c.assignmentsBySP[servicePrincipalID], nil
}

func (c fullSyncTestClient) ListDirectoryAudits(context.Context, *time.Time) ([]DirectoryAuditEvent, error) {
	return c.directoryAudits, nil
}

func (c fullSyncTestClient) ListDirectoryRoles(context.Context) ([]DirectoryRole, error) {
	return c.directoryRoles, nil
}

func (c fullSyncTestClient) ListDirectoryRoleAssignments(context.Context) ([]DirectoryRoleAssignment, error) {
	return c.directoryRoleAssignments, nil
}

func (c fullSyncTestClient) ListUsers(context.Context) ([]User, error) {
	return c.users, nil
}

func (c fullSyncTestClient) ListGroups(context.Context) ([]Group, error) {
	return c.groups, nil
}

func (c fullSyncTestClient) ListGroupUserMembers(_ context.Context, groupID string) ([]User, error) {
	return c.groupMembersByID[groupID], nil
}

func (c fullSyncTestClient) ListGroupTransitiveUserMembers(_ context.Context, groupID string) ([]User, error) {
	return c.groupMembersByID[groupID], nil
}

func (c fullSyncTestClient) ListApplicationOwners(_ context.Context, applicationID string) ([]DirectoryOwner, error) {
	return c.applicationOwnersByID[applicationID], nil
}

func (c fullSyncTestClient) ListServicePrincipalOwners(_ context.Context, servicePrincipalID string) ([]DirectoryOwner, error) {
	return c.servicePrincipalOwnersByID[servicePrincipalID], nil
}

func (c fullSyncTestClient) ListSignIns(context.Context, *time.Time) ([]SignInEvent, error) {
	return c.signIns, nil
}

func (c fullSyncTestClient) ListOAuth2PermissionGrants(context.Context) ([]OAuth2PermissionGrant, error) {
	return c.grants, nil
}

func (c fullSyncTestClient) LookupUsersByIDs(_ context.Context, ids []string) ([]User, error) {
	users := make([]User, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		user, ok := c.lookupUsersByID[id]
		if !ok {
			continue
		}
		users = append(users, user)
	}
	return users, nil
}

const (
	fullSyncTenantID              = "tenant-1"
	fullSyncApplicationID         = "app-1"
	fullSyncApplicationClientID   = "client-app-1"
	fullSyncServicePrincipalID    = "11111111-1111-1111-1111-111111111111"
	fullSyncUserAliceID           = "22222222-2222-2222-2222-222222222222"
	fullSyncUserBobID             = "33333333-3333-3333-3333-333333333333"
	fullSyncUserCarolID           = "44444444-4444-4444-4444-444444444444"
	fullSyncGroupEngineeringID    = "55555555-5555-5555-5555-555555555555"
	fullSyncAppRoleID             = "77777777-7777-7777-7777-777777777777"
	fullSyncAppSecretID           = "88888888-8888-8888-8888-888888888888"
	fullSyncAppCertificateID      = "99999999-9999-9999-9999-999999999999"
	fullSyncSPSecretID            = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	fullSyncSPCertificateID       = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	fullSyncAuditActorUserID      = "owner-user-1"
	fullSyncAuditCredentialUserID = "owner@example.com"
	discoveryGrantSPID            = "sp-discovery-2"
	discoveryGrantAppID           = "client-app-2"
	discoveryGrantActorID         = "discovery-user-1"
)

func fullSyncFixtureClient(t *testing.T) fullSyncTestClient {
	t.Helper()

	return fullSyncTestClient{
		applications: []Application{
			mustParseApplication(t, `{"id":"`+fullSyncApplicationID+`","appId":"`+fullSyncApplicationClientID+`","displayName":"Zendesk App","publisherDomain":"zendesk.com","verifiedPublisher":{"displayName":"Zendesk"},"passwordCredentials":[{"keyId":"`+fullSyncAppSecretID+`","displayName":"Primary App Secret","startDateTime":"2025-01-01T00:00:00Z","endDateTime":"2099-01-01T00:00:00Z","hint":"abc"}],"keyCredentials":[{"keyId":"`+fullSyncAppCertificateID+`","displayName":"Primary App Certificate","type":"AsymmetricX509Cert","usage":"Verify","startDateTime":"2025-01-01T00:00:00Z","endDateTime":"2099-01-01T00:00:00Z","customKeyIdentifier":"AQIDBA=="}]}`),
		},
		servicePrincipals: []ServicePrincipal{
			mustParseServicePrincipal(t, `{"id":"`+fullSyncServicePrincipalID+`","appId":"`+fullSyncApplicationClientID+`","displayName":"Zendesk","accountEnabled":true,"verifiedPublisher":{"displayName":"Zendesk"},"createdDateTime":"2025-01-02T00:00:00Z","appRoles":[{"id":"`+fullSyncAppRoleID+`","displayName":"Agent","value":"agent"}],"passwordCredentials":[{"keyId":"`+fullSyncSPSecretID+`","displayName":"SP Secret","startDateTime":"2025-01-01T00:00:00Z","endDateTime":"2099-01-01T00:00:00Z","hint":"xyz"}],"keyCredentials":[{"keyId":"`+fullSyncSPCertificateID+`","displayName":"SP Certificate","type":"AsymmetricX509Cert","usage":"Verify","startDateTime":"2025-01-01T00:00:00Z","endDateTime":"2099-01-01T00:00:00Z","customKeyIdentifier":"BQYHCA=="}]}`),
		},
		assignmentsBySP: map[string][]ServicePrincipalAppRoleAssignment{
			fullSyncServicePrincipalID: {
				mustParseAppRoleAssignment(t, `{"id":"assign-1","principalId":"`+fullSyncUserAliceID+`","principalType":"User","resourceId":"`+fullSyncServicePrincipalID+`","resourceDisplayName":"Zendesk","appRoleId":"`+fullSyncAppRoleID+`"}`),
				mustParseAppRoleAssignment(t, `{"id":"assign-2","principalId":"`+fullSyncGroupEngineeringID+`","principalType":"Group","principalDisplayName":"Engineering","resourceId":"`+fullSyncServicePrincipalID+`","resourceDisplayName":"Zendesk","appRoleId":"`+fullSyncAppRoleID+`"}`),
			},
		},
		directoryRoles: []DirectoryRole{
			mustParseDirectoryRole(t, `{"id":"role-def-1","displayName":"Global Administrator","templateId":"tmpl-1"}`),
			mustParseDirectoryRole(t, `{"id":"role-def-2","displayName":"Scoped Custom Role"}`),
		},
		directoryRoleAssignments: []DirectoryRoleAssignment{
			mustParseDirectoryRoleAssignment(t, `{"id":"assign-1","principalId":"`+fullSyncUserAliceID+`","roleDefinitionId":"role-def-1","directoryScopeId":"/","principal":{"id":"`+fullSyncUserAliceID+`","@odata.type":"#microsoft.graph.user"}}`),
			mustParseDirectoryRoleAssignment(t, `{"id":"assign-2","principalId":"`+fullSyncGroupEngineeringID+`","roleDefinitionId":"role-def-2","directoryScopeId":"/administrativeUnits/au-1","principal":{"id":"`+fullSyncGroupEngineeringID+`","displayName":"Engineering","@odata.type":"#microsoft.graph.group"}}`),
			mustParseDirectoryRoleAssignment(t, `{"id":"assign-3","principalId":"`+fullSyncUserBobID+`","roleDefinitionId":"role-def-missing","directoryScopeId":"/","principal":{"id":"`+fullSyncUserBobID+`","@odata.type":"#microsoft.graph.user"}}`),
			mustParseDirectoryRoleAssignment(t, `{"id":"assign-4","principalId":"`+fullSyncUserCarolID+`","roleDefinitionId":"role-def-1","directoryScopeId":"/"}`),
		},
		users: []User{
			mustParseUser(t, `{"id":"`+fullSyncUserAliceID+`","displayName":"Alice","mail":"alice@example.com","userPrincipalName":"alice@example.com","accountEnabled":true}`),
			mustParseUser(t, `{"id":"`+fullSyncUserBobID+`","displayName":"Bob","mail":"bob@example.com","userPrincipalName":"bob@example.com","accountEnabled":true}`),
			mustParseUser(t, `{"id":"`+fullSyncUserCarolID+`","displayName":"Carol","mail":"carol@example.com","userPrincipalName":"carol@example.com","accountEnabled":true}`),
		},
		groups: []Group{
			mustParseGroup(t, `{"id":"`+fullSyncGroupEngineeringID+`","displayName":"Engineering","mail":"engineering@example.com"}`),
		},
		groupMembersByID: map[string][]User{
			fullSyncGroupEngineeringID: {
				mustParseUser(t, `{"id":"`+fullSyncUserAliceID+`","displayName":"Alice","userPrincipalName":"alice@example.com"}`),
				mustParseUser(t, `{"id":"`+fullSyncUserCarolID+`","displayName":"Carol","userPrincipalName":"carol@example.com"}`),
			},
		},
		applicationOwnersByID: map[string][]DirectoryOwner{
			fullSyncApplicationID: {
				mustParseDirectoryOwner(t, `{"id":"owner-user-1","@odata.type":"#microsoft.graph.user","displayName":"Owner User","mail":"owner@example.com","userPrincipalName":"owner@example.com"}`),
				mustParseDirectoryOwner(t, `{"id":"owner-sp-1","@odata.type":"#microsoft.graph.servicePrincipal","displayName":"Owner Service Principal","appId":"owner-client-app"}`),
			},
		},
		servicePrincipalOwnersByID: map[string][]DirectoryOwner{
			fullSyncServicePrincipalID: {
				mustParseDirectoryOwner(t, `{"id":"owner-user-2","@odata.type":"#microsoft.graph.user","displayName":"SP Owner","mail":"sp-owner@example.com","userPrincipalName":"sp-owner@example.com"}`),
			},
		},
		directoryAudits: []DirectoryAuditEvent{
			mustParseDirectoryAudit(t, `{"id":"audit-1","category":"ApplicationManagement","result":"success","activityDisplayName":"Add application password credential","activityDateTime":"2026-02-07T23:00:00Z","initiatedBy":{"user":{"id":"`+fullSyncAuditActorUserID+`","displayName":"Owner User","userPrincipalName":"`+fullSyncAuditCredentialUserID+`"}},"targetResources":[{"id":"`+fullSyncApplicationID+`","displayName":"Zendesk App","type":"Application","modifiedProperties":[{"displayName":"PasswordCredentials","newValue":"{\"keyId\":\"`+fullSyncAppSecretID+`\"}"}]}]}`),
		},
	}
}

func discoveryFixtureClient(t *testing.T) fullSyncTestClient {
	t.Helper()

	return fullSyncTestClient{
		applications: []Application{
			mustParseApplication(t, `{"id":"app-discovery-1","appId":"`+fullSyncApplicationClientID+`","displayName":"Zendesk App","publisherDomain":"zendesk.com","verifiedPublisher":{"displayName":"Zendesk"}}`),
		},
		servicePrincipals: []ServicePrincipal{
			mustParseServicePrincipal(t, `{"id":"`+discoveryGrantSPID+`","appId":"`+discoveryGrantAppID+`","displayName":"Confluence","verifiedPublisher":{"displayName":"Atlassian"}}`),
		},
		signIns: []SignInEvent{
			mustParseSignIn(t, `{"id":"signin-1","createdDateTime":"2026-04-06T10:00:00Z","appId":"`+fullSyncApplicationClientID+`","appDisplayName":"Zendesk App","userId":"signin-user-1","userDisplayName":"Alice","userPrincipalName":"alice@example.com"}`),
		},
		grants: []OAuth2PermissionGrant{
			mustParseGrant(t, `{"id":"grant-1","clientId":"`+discoveryGrantSPID+`","consentType":"Principal","principalId":"`+discoveryGrantActorID+`","scope":"User.Read Mail.Read"}`),
		},
		lookupUsersByID: map[string]User{
			discoveryGrantActorID: mustParseUser(t, `{"id":"`+discoveryGrantActorID+`","displayName":"Grace Hopper","mail":"grace@example.com","userPrincipalName":"grace@example.com"}`),
		},
	}
}

func mustJSONObject(t *testing.T, raw []byte) map[string]any {
	t.Helper()

	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("json.Unmarshal(): %v", err)
	}
	return payload
}

func mustJSONStringSlice(t *testing.T, raw []byte) []string {
	t.Helper()

	var values []string
	if err := json.Unmarshal(raw, &values); err != nil {
		t.Fatalf("json.Unmarshal([]string): %v", err)
	}
	return values
}

func TestEntraRunFullPersistsEffectiveEntitlements(t *testing.T) {
	t.Parallel()

	withEntraTestDB(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, migrator *migrate.Migrate) {
		migrateEntraUp(t, migrator)

		integration := &EntraIntegration{
			client:   fullSyncFixtureClient(t),
			tenantID: fullSyncTenantID,
		}
		if err := integration.runFull(ctx, q, pool, func(registry.Event) {}); err != nil {
			t.Fatalf("runFull() error = %v", err)
		}

		aliceRows, err := q.ListSourceAccountsPageBySourceAndQuery(ctx, gen.ListSourceAccountsPageBySourceAndQueryParams{
			SourceKind:     "entra",
			SourceName:     fullSyncTenantID,
			EntityCategory: registry.EntityCategoryUser,
			Query:          "alice@example.com",
			PageLimit:      20,
		})
		if err != nil {
			t.Fatalf("ListSourceAccountsPageBySourceAndQuery(alice): %v", err)
		}
		if len(aliceRows) != 1 {
			t.Fatalf("len(aliceRows)=%d want 1", len(aliceRows))
		}
		aliceRaw := mustJSONObject(t, aliceRows[0].RawJson)
		if aliceRows[0].ExternalID != fullSyncUserAliceID {
			t.Fatalf("alice external_id=%q want %q", aliceRows[0].ExternalID, fullSyncUserAliceID)
		}
		if got := aliceRaw["status"]; got != "Active" {
			t.Fatalf("alice raw_json status=%v want %q", got, "Active")
		}
		if got := aliceRaw["entity_category"]; got != registry.EntityCategoryUser {
			t.Fatalf("alice raw_json entity_category=%v want %q", got, registry.EntityCategoryUser)
		}

		groupRowsRaw, err := q.ListSourceAccountsPageBySourceAndQuery(ctx, gen.ListSourceAccountsPageBySourceAndQueryParams{
			SourceKind:     "entra",
			SourceName:     fullSyncTenantID,
			EntityCategory: registry.EntityCategoryGroup,
			Query:          "Engineering",
			PageLimit:      20,
		})
		if err != nil {
			t.Fatalf("ListSourceAccountsPageBySourceAndQuery(group): %v", err)
		}
		if len(groupRowsRaw) != 1 {
			t.Fatalf("len(groupRowsRaw)=%d want 1", len(groupRowsRaw))
		}
		groupRaw := mustJSONObject(t, groupRowsRaw[0].RawJson)
		if groupRowsRaw[0].ExternalID != entraGroupExternalID(fullSyncGroupEngineeringID) {
			t.Fatalf("group external_id=%q want %q", groupRowsRaw[0].ExternalID, entraGroupExternalID(fullSyncGroupEngineeringID))
		}
		if got := groupRaw["status"]; got != "" {
			t.Fatalf("group raw_json status=%v want empty string", got)
		}
		if got := groupRaw["entity_category"]; got != registry.EntityCategoryGroup {
			t.Fatalf("group raw_json entity_category=%v want %q", got, registry.EntityCategoryGroup)
		}

		servicePrincipalRows, err := q.ListSourceAccountsPageBySourceAndQuery(ctx, gen.ListSourceAccountsPageBySourceAndQueryParams{
			SourceKind:     "entra",
			SourceName:     fullSyncTenantID,
			EntityCategory: registry.EntityCategoryServicePrincipal,
			Query:          fullSyncServicePrincipalID,
			PageLimit:      20,
		})
		if err != nil {
			t.Fatalf("ListSourceAccountsPageBySourceAndQuery(service principal): %v", err)
		}
		if len(servicePrincipalRows) != 1 {
			t.Fatalf("len(servicePrincipalRows)=%d want 1", len(servicePrincipalRows))
		}
		servicePrincipalRaw := mustJSONObject(t, servicePrincipalRows[0].RawJson)
		if servicePrincipalRows[0].ExternalID != entraServicePrincipalExternalID(fullSyncServicePrincipalID) {
			t.Fatalf("service principal external_id=%q want %q", servicePrincipalRows[0].ExternalID, entraServicePrincipalExternalID(fullSyncServicePrincipalID))
		}
		if got := servicePrincipalRaw["status"]; got != "Active" {
			t.Fatalf("service principal raw_json status=%v want %q", got, "Active")
		}
		if got := servicePrincipalRaw["entity_category"]; got != registry.EntityCategoryServicePrincipal {
			t.Fatalf("service principal raw_json entity_category=%v want %q", got, registry.EntityCategoryServicePrincipal)
		}

		applicationAsset, err := q.GetAppAssetBySourceAndKindAndExternalID(ctx, gen.GetAppAssetBySourceAndKindAndExternalIDParams{
			SourceKind: "entra",
			SourceName: fullSyncTenantID,
			AssetKind:  "entra_application",
			ExternalID: fullSyncApplicationID,
		})
		if err != nil {
			t.Fatalf("GetAppAssetBySourceAndKindAndExternalID(application): %v", err)
		}
		if applicationAsset.DisplayName != "Zendesk App" {
			t.Fatalf("application asset display_name=%q want %q", applicationAsset.DisplayName, "Zendesk App")
		}
		if applicationAsset.Status != "" {
			t.Fatalf("application asset status=%q want empty", applicationAsset.Status)
		}
		if len(applicationAsset.RawJson) == 0 {
			t.Fatalf("expected application asset raw_json")
		}

		servicePrincipalAsset, err := q.GetAppAssetBySourceAndKindAndExternalID(ctx, gen.GetAppAssetBySourceAndKindAndExternalIDParams{
			SourceKind: "entra",
			SourceName: fullSyncTenantID,
			AssetKind:  "entra_service_principal",
			ExternalID: fullSyncServicePrincipalID,
		})
		if err != nil {
			t.Fatalf("GetAppAssetBySourceAndKindAndExternalID(service principal): %v", err)
		}
		if servicePrincipalAsset.ParentExternalID != fullSyncApplicationClientID {
			t.Fatalf("service principal asset parent_external_id=%q want %q", servicePrincipalAsset.ParentExternalID, fullSyncApplicationClientID)
		}
		if servicePrincipalAsset.Status != "Active" {
			t.Fatalf("service principal asset status=%q want %q", servicePrincipalAsset.Status, "Active")
		}
		if !servicePrincipalAsset.CreatedAtSource.Valid || !servicePrincipalAsset.CreatedAtSource.Time.Equal(time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC)) {
			t.Fatalf("service principal asset created_at_source=%v want %v", servicePrincipalAsset.CreatedAtSource, time.Date(2025, time.January, 2, 0, 0, 0, 0, time.UTC))
		}
		if len(servicePrincipalAsset.RawJson) == 0 {
			t.Fatalf("expected service principal asset raw_json")
		}

		applicationOwners, err := q.ListAppAssetOwnersByAssetID(ctx, applicationAsset.ID)
		if err != nil {
			t.Fatalf("ListAppAssetOwnersByAssetID(application): %v", err)
		}
		if len(applicationOwners) != 2 {
			t.Fatalf("len(applicationOwners)=%d want 2", len(applicationOwners))
		}
		applicationOwnersByExternalID := make(map[string]gen.AppAssetOwner, len(applicationOwners))
		for _, owner := range applicationOwners {
			applicationOwnersByExternalID[owner.OwnerExternalID] = owner
		}
		if owner, ok := applicationOwnersByExternalID["owner-user-1"]; !ok || owner.OwnerKind != "entra_user" || owner.OwnerEmail != "owner@example.com" {
			t.Fatalf("unexpected application user owner: %+v", owner)
		}
		if owner, ok := applicationOwnersByExternalID[entraServicePrincipalExternalID("owner-sp-1")]; !ok || owner.OwnerKind != "entra_service_principal" {
			t.Fatalf("unexpected application service principal owner: %+v", owner)
		}

		servicePrincipalOwners, err := q.ListAppAssetOwnersByAssetID(ctx, servicePrincipalAsset.ID)
		if err != nil {
			t.Fatalf("ListAppAssetOwnersByAssetID(service principal): %v", err)
		}
		if len(servicePrincipalOwners) != 1 {
			t.Fatalf("len(servicePrincipalOwners)=%d want 1", len(servicePrincipalOwners))
		}
		if servicePrincipalOwners[0].OwnerExternalID != "owner-user-2" || servicePrincipalOwners[0].OwnerKind != "entra_user" {
			t.Fatalf("unexpected service principal owner: %+v", servicePrincipalOwners[0])
		}

		applicationCredentials, err := q.ListCredentialArtifactsForAssetRef(ctx, gen.ListCredentialArtifactsForAssetRefParams{
			SourceKind:         "entra",
			SourceName:         fullSyncTenantID,
			AssetRefKind:       "app_asset",
			AssetRefExternalID: appAssetRefExternalID("entra_application", fullSyncApplicationID),
		})
		if err != nil {
			t.Fatalf("ListCredentialArtifactsForAssetRef(application): %v", err)
		}
		if len(applicationCredentials) != 2 {
			t.Fatalf("len(applicationCredentials)=%d want 2", len(applicationCredentials))
		}
		applicationCredentialsByExternalID := make(map[string]gen.ListCredentialArtifactsForAssetRefRow, len(applicationCredentials))
		for _, credential := range applicationCredentials {
			applicationCredentialsByExternalID[credential.ExternalID] = credential
		}
		if credential, ok := applicationCredentialsByExternalID[fullSyncAppSecretID]; !ok || credential.CredentialKind != "entra_client_secret" || credential.Status != "active" {
			t.Fatalf("unexpected application secret credential: %+v", credential)
		}
		if credential, ok := applicationCredentialsByExternalID[fullSyncAppCertificateID]; !ok || credential.CredentialKind != "entra_certificate" || credential.Status != "active" {
			t.Fatalf("unexpected application certificate credential: %+v", credential)
		}

		servicePrincipalCredentials, err := q.ListCredentialArtifactsForAssetRef(ctx, gen.ListCredentialArtifactsForAssetRefParams{
			SourceKind:         "entra",
			SourceName:         fullSyncTenantID,
			AssetRefKind:       "app_asset",
			AssetRefExternalID: appAssetRefExternalID("entra_service_principal", fullSyncServicePrincipalID),
		})
		if err != nil {
			t.Fatalf("ListCredentialArtifactsForAssetRef(service principal): %v", err)
		}
		if len(servicePrincipalCredentials) != 2 {
			t.Fatalf("len(servicePrincipalCredentials)=%d want 2", len(servicePrincipalCredentials))
		}
		servicePrincipalCredentialsByExternalID := make(map[string]gen.ListCredentialArtifactsForAssetRefRow, len(servicePrincipalCredentials))
		for _, credential := range servicePrincipalCredentials {
			servicePrincipalCredentialsByExternalID[credential.ExternalID] = credential
		}
		if credential, ok := servicePrincipalCredentialsByExternalID[fullSyncSPSecretID]; !ok || credential.CredentialKind != "entra_client_secret" {
			t.Fatalf("unexpected service principal secret credential: %+v", credential)
		}
		if credential, ok := servicePrincipalCredentialsByExternalID[fullSyncSPCertificateID]; !ok || credential.CredentialKind != "entra_certificate" {
			t.Fatalf("unexpected service principal certificate credential: %+v", credential)
		}

		auditEvents, err := q.ListCredentialAuditEventsForCredential(ctx, gen.ListCredentialAuditEventsForCredentialParams{
			SourceKind:           "entra",
			SourceName:           fullSyncTenantID,
			CredentialKind:       "entra_client_secret",
			CredentialExternalID: fullSyncAppSecretID,
			LimitRows:            20,
		})
		if err != nil {
			t.Fatalf("ListCredentialAuditEventsForCredential(): %v", err)
		}
		if len(auditEvents) != 1 {
			t.Fatalf("len(auditEvents)=%d want 1", len(auditEvents))
		}
		if auditEvents[0].TargetExternalID != fullSyncApplicationID || auditEvents[0].ActorExternalID != fullSyncAuditActorUserID {
			t.Fatalf("unexpected audit event target=%q actor=%q", auditEvents[0].TargetExternalID, auditEvents[0].ActorExternalID)
		}
		if auditEvents[0].CredentialKind != "entra_client_secret" {
			t.Fatalf("audit event credential_kind=%q want %q", auditEvents[0].CredentialKind, "entra_client_secret")
		}

		rows, err := q.ListSourceAccountsPageBySourceAndQueryWithEntitlementCounts(ctx, gen.ListSourceAccountsPageBySourceAndQueryWithEntitlementCountsParams{
			SourceKind:            "entra",
			SourceName:            fullSyncTenantID,
			EntityCategory:        "user",
			Query:                 "alice@example.com",
			PageLimit:             20,
			DistinctResourceKind1: "entra_directory_role",
			DistinctResourceKind2: "entra_app_role",
		})
		if err != nil {
			t.Fatalf("ListSourceAccountsPageBySourceAndQueryWithEntitlementCounts(user-1): %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("len(rows)=%d want 1", len(rows))
		}
		if rows[0].DistinctResourceCount1 != 2 || rows[0].DistinctResourceCount2 != 1 {
			t.Fatalf("user-1 summary counts=(%d,%d) want (2,1)", rows[0].DistinctResourceCount1, rows[0].DistinctResourceCount2)
		}

		groupRows, err := q.ListSourceAccountsPageBySourceAndQueryWithEntitlementCounts(ctx, gen.ListSourceAccountsPageBySourceAndQueryWithEntitlementCountsParams{
			SourceKind:            "entra",
			SourceName:            fullSyncTenantID,
			EntityCategory:        "user",
			Query:                 "carol@example.com",
			PageLimit:             20,
			DistinctResourceKind1: "entra_directory_role",
			DistinctResourceKind2: "entra_app_role",
		})
		if err != nil {
			t.Fatalf("ListSourceAccountsPageBySourceAndQueryWithEntitlementCounts(user-3): %v", err)
		}
		if len(groupRows) != 1 {
			t.Fatalf("len(groupRows)=%d want 1", len(groupRows))
		}
		if groupRows[0].DistinctResourceCount1 != 1 || groupRows[0].DistinctResourceCount2 != 1 {
			t.Fatalf("user-3 summary counts=(%d,%d) want (1,1)", groupRows[0].DistinctResourceCount1, groupRows[0].DistinctResourceCount2)
		}

		var entitlementCount int
		if err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM entitlements e
			JOIN accounts a ON a.id = e.app_user_id
			WHERE a.source_kind = 'entra'
			  AND a.source_name = $1
			  AND e.expired_at IS NULL
			  AND e.last_observed_run_id IS NOT NULL
		`, fullSyncTenantID).Scan(&entitlementCount); err != nil {
			t.Fatalf("count entitlements: %v", err)
		}
		if entitlementCount != 6 {
			t.Fatalf("entitlementCount=%d want 6", entitlementCount)
		}

		var appRoleRawJSON []byte
		if err := pool.QueryRow(ctx, `
			SELECT e.raw_json
			FROM entitlements e
			JOIN accounts a ON a.id = e.app_user_id
			WHERE a.source_kind = 'entra'
			  AND a.source_name = $1
			  AND a.external_id = $2
			  AND e.kind = 'entra_app_role'
			  AND e.resource = $3
		`, fullSyncTenantID, fullSyncUserAliceID, entraServicePrincipalResourceRef(fullSyncServicePrincipalID)).Scan(&appRoleRawJSON); err != nil {
			t.Fatalf("query app role entitlement raw_json: %v", err)
		}
		appRolePayload := mustJSONObject(t, appRoleRawJSON)
		if got := appRolePayload["role_name"]; got != "Agent" {
			t.Fatalf("app role entitlement role_name=%v want %q", got, "Agent")
		}

		var directoryRoleRawJSON []byte
		if err := pool.QueryRow(ctx, `
			SELECT e.raw_json
			FROM entitlements e
			JOIN accounts a ON a.id = e.app_user_id
			WHERE a.source_kind = 'entra'
			  AND a.source_name = $1
			  AND a.external_id = $2
			  AND e.kind = 'entra_directory_role'
			  AND e.resource = 'entra_directory_role:tmpl-1'
		`, fullSyncTenantID, fullSyncUserAliceID).Scan(&directoryRoleRawJSON); err != nil {
			t.Fatalf("query directory role entitlement raw_json: %v", err)
		}
		directoryRolePayload := mustJSONObject(t, directoryRoleRawJSON)
		if got := directoryRolePayload["role_name"]; got != "Global Administrator" {
			t.Fatalf("directory role entitlement role_name=%v want %q", got, "Global Administrator")
		}

		var danglingDirectoryRoleRawJSON []byte
		if err := pool.QueryRow(ctx, `
			SELECT e.raw_json
			FROM entitlements e
			JOIN accounts a ON a.id = e.app_user_id
			WHERE a.source_kind = 'entra'
			  AND a.source_name = $1
			  AND a.external_id = $2
			  AND e.kind = 'entra_directory_role'
			  AND e.resource = 'entra_directory_role:role-def-missing'
		`, fullSyncTenantID, fullSyncUserBobID).Scan(&danglingDirectoryRoleRawJSON); err != nil {
			t.Fatalf("query dangling directory role entitlement raw_json: %v", err)
		}
		danglingDirectoryRolePayload := mustJSONObject(t, danglingDirectoryRoleRawJSON)
		if got := danglingDirectoryRolePayload["role_definition_id"]; got != "role-def-missing" {
			t.Fatalf("dangling directory role definition_id=%v want %q", got, "role-def-missing")
		}
		if got := danglingDirectoryRolePayload["role_template_id"]; got != "" {
			t.Fatalf("dangling directory role template_id=%v want empty string", got)
		}
		if got := danglingDirectoryRolePayload["role_name"]; got != "role-def-missing" {
			t.Fatalf("dangling directory role role_name=%v want %q", got, "role-def-missing")
		}
		if got := danglingDirectoryRolePayload["role_display_name"]; got != "role-def-missing" {
			t.Fatalf("dangling directory role role_display_name=%v want %q", got, "role-def-missing")
		}

		var skippedNilPrincipalCount int
		if err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM entitlements e
			JOIN accounts a ON a.id = e.app_user_id
			WHERE a.source_kind = 'entra'
			  AND a.source_name = $1
			  AND a.external_id = $2
			  AND e.kind = 'entra_directory_role'
			  AND e.resource = 'entra_directory_role:tmpl-1'
		`, fullSyncTenantID, fullSyncUserCarolID).Scan(&skippedNilPrincipalCount); err != nil {
			t.Fatalf("count nil-principal directory role entitlements: %v", err)
		}
		if skippedNilPrincipalCount != 0 {
			t.Fatalf("skippedNilPrincipalCount=%d want 0", skippedNilPrincipalCount)
		}
	})
}

func TestEntraRunDiscoveryPersistsSourcesAndEvents(t *testing.T) {
	t.Parallel()

	withEntraTestDB(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, migrator *migrate.Migrate) {
		migrateEntraUp(t, migrator)

		if err := q.UpsertConnectorSourceState(ctx, gen.UpsertConnectorSourceStateParams{
			SourceKind:       "entra",
			SourceName:       fullSyncTenantID,
			Enabled:          true,
			Configured:       true,
			DiscoveryEnabled: true,
		}); err != nil {
			t.Fatalf("UpsertConnectorSourceState(): %v", err)
		}

		integration := &EntraIntegration{
			client:           discoveryFixtureClient(t),
			tenantID:         fullSyncTenantID,
			discoveryEnabled: true,
		}
		if err := integration.Run(ctx, q, pool, func(registry.Event) {}, registry.RunModeDiscovery); err != nil {
			t.Fatalf("Run(discovery) error = %v", err)
		}

		apps, err := q.ListSaaSAppsPageByFilters(ctx, gen.ListSaaSAppsPageByFiltersParams{
			SourceKind: "entra",
			SourceName: fullSyncTenantID,
			PageLimit:  20,
		})
		if err != nil {
			t.Fatalf("ListSaaSAppsPageByFilters(): %v", err)
		}
		if len(apps) != 2 {
			t.Fatalf("len(apps)=%d want 2", len(apps))
		}

		appsByName := make(map[string]gen.ListSaaSAppsPageByFiltersRow, len(apps))
		for _, app := range apps {
			appsByName[app.DisplayName] = app
		}

		zendesk := appsByName["Zendesk App"]
		if zendesk.VendorName != "Zendesk" {
			t.Fatalf("zendesk vendor_name=%q want %q", zendesk.VendorName, "Zendesk")
		}
		zendeskSources, err := q.ListSaaSAppSourcesBySaaSAppID(ctx, zendesk.ID)
		if err != nil {
			t.Fatalf("ListSaaSAppSourcesBySaaSAppID(zendesk): %v", err)
		}
		if len(zendeskSources) != 1 || zendeskSources[0].SourceAppID != fullSyncApplicationClientID {
			t.Fatalf("unexpected zendesk sources: %+v", zendeskSources)
		}
		zendeskEvents, err := q.ListSaaSAppEventsBySaaSAppID(ctx, gen.ListSaaSAppEventsBySaaSAppIDParams{
			SaasAppID: zendesk.ID,
			LimitRows: 20,
		})
		if err != nil {
			t.Fatalf("ListSaaSAppEventsBySaaSAppID(zendesk): %v", err)
		}
		if len(zendeskEvents) != 1 {
			t.Fatalf("len(zendeskEvents)=%d want 1", len(zendeskEvents))
		}
		if zendeskEvents[0].SignalKind != discovery.SignalKindIDPSSO || zendeskEvents[0].ActorEmail != "alice@example.com" {
			t.Fatalf("unexpected zendesk event: %+v", zendeskEvents[0])
		}

		confluence := appsByName["Confluence"]
		if confluence.VendorName != "Atlassian" {
			t.Fatalf("confluence vendor_name=%q want %q", confluence.VendorName, "Atlassian")
		}
		confluenceSources, err := q.ListSaaSAppSourcesBySaaSAppID(ctx, confluence.ID)
		if err != nil {
			t.Fatalf("ListSaaSAppSourcesBySaaSAppID(confluence): %v", err)
		}
		if len(confluenceSources) != 1 || confluenceSources[0].SourceAppID != discoveryGrantAppID {
			t.Fatalf("unexpected confluence sources: %+v", confluenceSources)
		}
		confluenceEvents, err := q.ListSaaSAppEventsBySaaSAppID(ctx, gen.ListSaaSAppEventsBySaaSAppIDParams{
			SaasAppID: confluence.ID,
			LimitRows: 20,
		})
		if err != nil {
			t.Fatalf("ListSaaSAppEventsBySaaSAppID(confluence): %v", err)
		}
		if len(confluenceEvents) != 1 {
			t.Fatalf("len(confluenceEvents)=%d want 1", len(confluenceEvents))
		}
		if confluenceEvents[0].SignalKind != discovery.SignalKindOAuth || confluenceEvents[0].ActorEmail != "grace@example.com" || confluenceEvents[0].ActorDisplayName != "Grace Hopper" {
			t.Fatalf("unexpected confluence event: %+v", confluenceEvents[0])
		}
		if got := mustJSONStringSlice(t, confluenceEvents[0].ScopesJson); len(got) != 2 || got[0] != "user.read" || got[1] != "mail.read" {
			t.Fatalf("unexpected oauth scopes: %v", got)
		}
	})
}

func withEntraTestDB(t *testing.T, fn func(context.Context, *pgxpool.Pool, *gen.Queries, *migrate.Migrate)) {
	t.Helper()

	testdb.WithDatabase(t, testdb.Options{NamePrefix: "opensspm_entra"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		fn(ctx, pool, gen.New(pool), migrator)
	})
}

func migrateEntraUp(t *testing.T, migrator *migrate.Migrate) {
	t.Helper()

	testdb.MigrateUp(t, migrator)
}

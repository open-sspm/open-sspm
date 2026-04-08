package entra

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

const (
	appRoleSPID                       = "11111111-1111-1111-1111-111111111111"
	appRoleUser1ID                    = "22222222-2222-2222-2222-222222222222"
	appRoleUser2ID                    = "33333333-3333-3333-3333-333333333333"
	appRoleUser3ID                    = "44444444-4444-4444-4444-444444444444"
	appRoleGroup1                     = "55555555-5555-5555-5555-555555555555"
	appRoleGroup2                     = "66666666-6666-6666-6666-666666666666"
	appRoleID                         = "77777777-7777-7777-7777-777777777777"
	appRoleAssigneeServicePrincipalID = "88888888-8888-8888-8888-888888888888"
)

type entitlementsTestClient struct {
	assignmentsBySP            map[string][]ServicePrincipalAppRoleAssignment
	groupMembersByID           map[string][]User
	transitiveGroupMembersByID map[string][]User
	directoryRoles             []DirectoryRole
	directoryRoleAssignments   []DirectoryRoleAssignment
}

func (c entitlementsTestClient) ListApplications(context.Context) ([]Application, error) {
	panic("unexpected call")
}

func (c entitlementsTestClient) ListServicePrincipals(context.Context) ([]ServicePrincipal, error) {
	panic("unexpected call")
}

func (c entitlementsTestClient) ListServicePrincipalAssignedTo(_ context.Context, servicePrincipalID string) ([]ServicePrincipalAppRoleAssignment, error) {
	return c.assignmentsBySP[servicePrincipalID], nil
}

func (c entitlementsTestClient) ListDirectoryAudits(context.Context, *time.Time) ([]DirectoryAuditEvent, error) {
	panic("unexpected call")
}

func (c entitlementsTestClient) ListDirectoryRoles(context.Context) ([]DirectoryRole, error) {
	return c.directoryRoles, nil
}

func (c entitlementsTestClient) ListDirectoryRoleAssignments(context.Context) ([]DirectoryRoleAssignment, error) {
	return c.directoryRoleAssignments, nil
}

func (c entitlementsTestClient) ListUsers(context.Context) ([]User, error) {
	panic("unexpected call")
}

func (c entitlementsTestClient) ListGroups(context.Context) ([]Group, error) {
	panic("unexpected call")
}

func (c entitlementsTestClient) ListGroupUserMembers(_ context.Context, groupID string) ([]User, error) {
	return c.groupMembersByID[groupID], nil
}

func (c entitlementsTestClient) ListGroupTransitiveUserMembers(_ context.Context, groupID string) ([]User, error) {
	if c.transitiveGroupMembersByID != nil {
		return c.transitiveGroupMembersByID[groupID], nil
	}
	return c.groupMembersByID[groupID], nil
}

func (c entitlementsTestClient) ListApplicationOwners(context.Context, string) ([]DirectoryOwner, error) {
	panic("unexpected call")
}

func (c entitlementsTestClient) ListServicePrincipalOwners(context.Context, string) ([]DirectoryOwner, error) {
	panic("unexpected call")
}

func (c entitlementsTestClient) ListSignIns(context.Context, *time.Time) ([]SignInEvent, error) {
	panic("unexpected call")
}

func (c entitlementsTestClient) ListOAuth2PermissionGrants(context.Context) ([]OAuth2PermissionGrant, error) {
	panic("unexpected call")
}

func (c entitlementsTestClient) LookupUsersByIDs(context.Context, []string) ([]User, error) {
	panic("unexpected call")
}

func TestCollectEntraAppRoleEntitlementsBuildsEffectiveUserAccess(t *testing.T) {
	t.Parallel()

	integration := &EntraIntegration{
		client: entitlementsTestClient{
			assignmentsBySP: map[string][]ServicePrincipalAppRoleAssignment{
				appRoleSPID: {
					mustParseAppRoleAssignment(t, `{"id":"assign-1","principalId":"`+appRoleUser1ID+`","principalType":"User","resourceId":"`+appRoleSPID+`","resourceDisplayName":"Zendesk","appRoleId":"`+appRoleID+`"}`),
					mustParseAppRoleAssignment(t, `{"id":"assign-2","principalId":"`+appRoleGroup1+`","principalType":"Group","principalDisplayName":"Engineering","resourceId":"`+appRoleSPID+`","resourceDisplayName":"Zendesk","appRoleId":"`+appRoleID+`"}`),
					mustParseAppRoleAssignment(t, `{"id":"assign-3","principalId":"`+appRoleGroup2+`","principalType":"Group","principalDisplayName":"Security","resourceId":"`+appRoleSPID+`","resourceDisplayName":"Zendesk","appRoleId":"`+appRoleID+`"}`),
					mustParseAppRoleAssignment(t, `{"id":"assign-4","principalId":"`+appRoleUser2ID+`","principalType":"User","resourceId":"`+appRoleSPID+`","resourceDisplayName":"Zendesk","appRoleId":"`+entraDefaultAppRoleID+`"}`),
					mustParseAppRoleAssignment(t, `{"id":"assign-5","principalId":"`+appRoleAssigneeServicePrincipalID+`","principalType":"ServicePrincipal","resourceId":"`+appRoleSPID+`","resourceDisplayName":"Zendesk","appRoleId":"`+appRoleID+`"}`),
				},
			},
			groupMembersByID: map[string][]User{
				appRoleGroup1: {
					mustParseUser(t, `{"id":"`+appRoleUser1ID+`"}`),
					mustParseUser(t, `{"id":"`+appRoleUser3ID+`"}`),
				},
				appRoleGroup2: {
					mustParseUser(t, `{"id":"`+appRoleUser1ID+`"}`),
				},
			},
		},
	}

	rows, err := integration.collectEntraAppRoleEntitlements(context.Background(), func(registry.Event) {}, []ServicePrincipal{
		mustParseServicePrincipal(t, `{"id":"`+appRoleSPID+`","displayName":"Zendesk","appRoles":[{"id":"`+appRoleID+`","displayName":"Agent","value":"agent"}]}`),
	})
	if err != nil {
		t.Fatalf("collectEntraAppRoleEntitlements() error = %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("len(rows)=%d want 4", len(rows))
	}

	byAccount := make(map[string]entraEntitlementUpsertRow, len(rows))
	for _, row := range rows {
		byAccount[row.AccountExternalID+"|"+row.Permission] = row
	}

	direct := byAccount[appRoleUser1ID+"|Agent"]
	if direct.Kind != "entra_app_role" {
		t.Fatalf("direct.Kind=%q want entra_app_role", direct.Kind)
	}
	if direct.Resource != "entra_service_principal:"+appRoleSPID {
		t.Fatalf("direct.Resource=%q want entra_service_principal:%s", direct.Resource, appRoleSPID)
	}

	var directRaw struct {
		ServicePrincipalID   string               `json:"service_principal_id"`
		ServicePrincipalName string               `json:"service_principal_name"`
		RoleName             string               `json:"role_name"`
		AppRoleID            string               `json:"app_role_id"`
		AppRoleValue         string               `json:"app_role_value"`
		AppRoleDisplayName   string               `json:"app_role_display_name"`
		AssignmentSource     string               `json:"assignment_source"`
		GrantingGroups       []entraGrantingGroup `json:"granting_groups"`
	}
	if err := json.Unmarshal(direct.RawJSON, &directRaw); err != nil {
		t.Fatalf("json.Unmarshal(direct.RawJSON): %v", err)
	}
	if directRaw.AssignmentSource != entraAssignmentSourceDirect {
		t.Fatalf("direct assignment_source=%q want %q", directRaw.AssignmentSource, entraAssignmentSourceDirect)
	}
	if len(directRaw.GrantingGroups) != 2 {
		t.Fatalf("len(directRaw.GrantingGroups)=%d want 2", len(directRaw.GrantingGroups))
	}
	if directRaw.ServicePrincipalName != "Zendesk" || directRaw.RoleName != "Agent" || directRaw.AppRoleDisplayName != "Agent" || directRaw.AppRoleValue != "agent" {
		t.Fatalf("unexpected direct raw payload %+v", directRaw)
	}

	groupOnly := byAccount[appRoleUser3ID+"|Agent"]
	var groupRaw struct {
		AssignmentSource string               `json:"assignment_source"`
		GrantingGroups   []entraGrantingGroup `json:"granting_groups"`
	}
	if err := json.Unmarshal(groupOnly.RawJSON, &groupRaw); err != nil {
		t.Fatalf("json.Unmarshal(groupOnly.RawJSON): %v", err)
	}
	if groupRaw.AssignmentSource != entraAssignmentSourceGroup {
		t.Fatalf("group assignment_source=%q want %q", groupRaw.AssignmentSource, entraAssignmentSourceGroup)
	}
	if len(groupRaw.GrantingGroups) != 1 || groupRaw.GrantingGroups[0].ID != appRoleGroup1 {
		t.Fatalf("unexpected group grant payload %+v", groupRaw)
	}

	defaultAccess := byAccount[appRoleUser2ID+"|Default access"]
	var defaultRaw struct {
		AppRoleID        string `json:"app_role_id"`
		AssignmentSource string `json:"assignment_source"`
	}
	if err := json.Unmarshal(defaultAccess.RawJSON, &defaultRaw); err != nil {
		t.Fatalf("json.Unmarshal(defaultAccess.RawJSON): %v", err)
	}
	if defaultRaw.AppRoleID != entraDefaultAppRoleID || defaultRaw.AssignmentSource != entraAssignmentSourceDirect {
		t.Fatalf("unexpected default access payload %+v", defaultRaw)
	}

	spAssignee := byAccount[entraServicePrincipalExternalID(appRoleAssigneeServicePrincipalID)+"|Agent"]
	if spAssignee.AccountExternalID != entraServicePrincipalExternalID(appRoleAssigneeServicePrincipalID) {
		t.Fatalf("spAssignee.AccountExternalID=%q want %q", spAssignee.AccountExternalID, entraServicePrincipalExternalID(appRoleAssigneeServicePrincipalID))
	}
	var spAssigneeRaw struct {
		AssignmentSource string               `json:"assignment_source"`
		GrantingGroups   []entraGrantingGroup `json:"granting_groups"`
	}
	if err := json.Unmarshal(spAssignee.RawJSON, &spAssigneeRaw); err != nil {
		t.Fatalf("json.Unmarshal(spAssignee.RawJSON): %v", err)
	}
	if spAssigneeRaw.AssignmentSource != entraAssignmentSourceDirect {
		t.Fatalf("sp assignee assignment_source=%q want %q", spAssigneeRaw.AssignmentSource, entraAssignmentSourceDirect)
	}
	if len(spAssigneeRaw.GrantingGroups) != 0 {
		t.Fatalf("unexpected sp assignee granting groups %+v", spAssigneeRaw.GrantingGroups)
	}
}

func TestCollectEntraAppRoleEntitlementsKeepsDistinctRolesWithSharedName(t *testing.T) {
	t.Parallel()

	const duplicateRoleUserID = "99999999-9999-9999-9999-999999999999"
	const duplicateRoleID1 = "aaaaaaaa-1111-1111-1111-111111111111"
	const duplicateRoleID2 = "bbbbbbbb-2222-2222-2222-222222222222"

	integration := &EntraIntegration{
		client: entitlementsTestClient{
			assignmentsBySP: map[string][]ServicePrincipalAppRoleAssignment{
				appRoleSPID: {
					mustParseAppRoleAssignment(t, `{"id":"assign-dup-1","principalId":"`+duplicateRoleUserID+`","principalType":"User","resourceId":"`+appRoleSPID+`","resourceDisplayName":"Zendesk","appRoleId":"`+duplicateRoleID1+`"}`),
					mustParseAppRoleAssignment(t, `{"id":"assign-dup-2","principalId":"`+duplicateRoleUserID+`","principalType":"User","resourceId":"`+appRoleSPID+`","resourceDisplayName":"Zendesk","appRoleId":"`+duplicateRoleID2+`"}`),
				},
			},
		},
	}

	rows, err := integration.collectEntraAppRoleEntitlements(context.Background(), func(registry.Event) {}, []ServicePrincipal{
		mustParseServicePrincipal(t, `{"id":"`+appRoleSPID+`","displayName":"Zendesk","appRoles":[{"id":"`+duplicateRoleID1+`","displayName":"Agent","value":"agent"},{"id":"`+duplicateRoleID2+`","displayName":"Agent","value":"agent"}]}`),
	})
	if err != nil {
		t.Fatalf("collectEntraAppRoleEntitlements() error = %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("len(rows)=%d want 2", len(rows))
	}

	seenPermissions := make(map[string]struct{}, len(rows))
	seenRoleIDs := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if row.AccountExternalID != duplicateRoleUserID {
			t.Fatalf("row.AccountExternalID=%q want %q", row.AccountExternalID, duplicateRoleUserID)
		}
		seenPermissions[row.Permission] = struct{}{}

		var payload struct {
			RoleName  string `json:"role_name"`
			AppRoleID string `json:"app_role_id"`
		}
		if err := json.Unmarshal(row.RawJSON, &payload); err != nil {
			t.Fatalf("json.Unmarshal(row.RawJSON): %v", err)
		}
		if payload.RoleName != "Agent" {
			t.Fatalf("payload.RoleName=%q want %q", payload.RoleName, "Agent")
		}
		seenRoleIDs[payload.AppRoleID] = struct{}{}
	}

	if len(seenPermissions) != 2 {
		t.Fatalf("len(seenPermissions)=%d want 2", len(seenPermissions))
	}
	if _, ok := seenRoleIDs[duplicateRoleID1]; !ok {
		t.Fatalf("missing app role id %q", duplicateRoleID1)
	}
	if _, ok := seenRoleIDs[duplicateRoleID2]; !ok {
		t.Fatalf("missing app role id %q", duplicateRoleID2)
	}
}

func TestCollectEntraAppRoleEntitlementsSkipsNestedGroupMembers(t *testing.T) {
	t.Parallel()

	integration := &EntraIntegration{
		client: entitlementsTestClient{
			assignmentsBySP: map[string][]ServicePrincipalAppRoleAssignment{
				appRoleSPID: {
					mustParseAppRoleAssignment(t, `{"id":"assign-nested-1","principalId":"`+appRoleGroup1+`","principalType":"Group","principalDisplayName":"Engineering","resourceId":"`+appRoleSPID+`","resourceDisplayName":"Zendesk","appRoleId":"`+appRoleID+`"}`),
				},
			},
			groupMembersByID: map[string][]User{
				appRoleGroup1: nil,
			},
			transitiveGroupMembersByID: map[string][]User{
				appRoleGroup1: {
					mustParseUser(t, `{"id":"`+appRoleUser3ID+`"}`),
				},
			},
		},
	}

	rows, err := integration.collectEntraAppRoleEntitlements(context.Background(), func(registry.Event) {}, []ServicePrincipal{
		mustParseServicePrincipal(t, `{"id":"`+appRoleSPID+`","displayName":"Zendesk","appRoles":[{"id":"`+appRoleID+`","displayName":"Agent","value":"agent"}]}`),
	})
	if err != nil {
		t.Fatalf("collectEntraAppRoleEntitlements() error = %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("len(rows)=%d want 0", len(rows))
	}
}

func TestCollectEntraDirectoryRoleEntitlementsBuildsRows(t *testing.T) {
	t.Parallel()

	integration := &EntraIntegration{
		client: entitlementsTestClient{
			directoryRoles: []DirectoryRole{
				mustParseDirectoryRole(t, `{"id":"role-def-1","displayName":"Global Administrator","templateId":"tmpl-1"}`),
				mustParseDirectoryRole(t, `{"id":"role-def-2","displayName":"Scoped Custom Role"}`),
			},
			directoryRoleAssignments: []DirectoryRoleAssignment{
				mustParseDirectoryRoleAssignment(t, `{"id":"assign-1","principalId":"user-1","roleDefinitionId":"role-def-1","directoryScopeId":"/","principal":{"id":"user-1","@odata.type":"#microsoft.graph.user"}}`),
				mustParseDirectoryRoleAssignment(t, `{"id":"assign-2","principalId":"group-1","roleDefinitionId":"role-def-2","directoryScopeId":"/administrativeUnits/au-1","principal":{"id":"group-1","displayName":"Privileged Ops","@odata.type":"#microsoft.graph.group"}}`),
				mustParseDirectoryRoleAssignment(t, `{"id":"assign-3","principalId":"sp-1","roleDefinitionId":"role-def-1","directoryScopeId":"/","principal":{"id":"sp-1","displayName":"Automation SP","@odata.type":"#microsoft.graph.servicePrincipal"}}`),
			},
			groupMembersByID: map[string][]User{
				"group-1": {
					mustParseUser(t, `{"id":"user-1"}`),
					mustParseUser(t, `{"id":"user-1"}`),
					mustParseUser(t, `{"id":"user-2"}`),
				},
			},
		},
	}

	rows, err := integration.collectEntraDirectoryRoleEntitlements(context.Background(), func(registry.Event) {})
	if err != nil {
		t.Fatalf("collectEntraDirectoryRoleEntitlements() error = %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("len(rows)=%d want 4", len(rows))
	}

	byAccountAndResource := make(map[string]entraEntitlementUpsertRow, len(rows))
	for _, row := range rows {
		if row.Kind != "entra_directory_role" {
			t.Fatalf("row.Kind=%q want entra_directory_role", row.Kind)
		}
		if row.Permission != entraDirectoryRolePermission {
			t.Fatalf("row.Permission=%q want %q", row.Permission, entraDirectoryRolePermission)
		}
		byAccountAndResource[row.AccountExternalID+"|"+row.Resource] = row
	}

	globalAdmin := byAccountAndResource["user-1|entra_directory_role:tmpl-1"]
	if globalAdmin.Resource != "entra_directory_role:tmpl-1" {
		t.Fatalf("globalAdmin.Resource=%q want entra_directory_role:tmpl-1", globalAdmin.Resource)
	}
	var globalPayload struct {
		RoleName         string   `json:"role_name"`
		RoleDefinitionID string   `json:"role_definition_id"`
		RoleTemplateID   string   `json:"role_template_id"`
		RoleDisplayName  string   `json:"role_display_name"`
		AssignmentSource string   `json:"assignment_source"`
		DirectoryScopes  []string `json:"directory_scope_ids"`
	}
	if err := json.Unmarshal(globalAdmin.RawJSON, &globalPayload); err != nil {
		t.Fatalf("json.Unmarshal(globalAdmin.RawJSON): %v", err)
	}
	if globalPayload.RoleName != "Global Administrator" || globalPayload.RoleDefinitionID != "role-def-1" || globalPayload.RoleTemplateID != "tmpl-1" || globalPayload.RoleDisplayName != "Global Administrator" {
		t.Fatalf("unexpected global role payload %+v", globalPayload)
	}
	if globalPayload.AssignmentSource != entraAssignmentSourceDirect {
		t.Fatalf("global assignment_source=%q want %q", globalPayload.AssignmentSource, entraAssignmentSourceDirect)
	}
	if len(globalPayload.DirectoryScopes) != 1 || globalPayload.DirectoryScopes[0] != "/" {
		t.Fatalf("unexpected global directory scopes %+v", globalPayload.DirectoryScopes)
	}

	customRole := byAccountAndResource["user-2|entra_directory_role:role-def-2"]
	if customRole.Resource != "entra_directory_role:role-def-2" {
		t.Fatalf("customRole.Resource=%q want entra_directory_role:role-def-2", customRole.Resource)
	}
	var customPayload struct {
		RoleName         string               `json:"role_name"`
		RoleDefinitionID string               `json:"role_definition_id"`
		RoleTemplateID   string               `json:"role_template_id"`
		RoleDisplayName  string               `json:"role_display_name"`
		AssignmentSource string               `json:"assignment_source"`
		GrantingGroups   []entraGrantingGroup `json:"granting_groups"`
		DirectoryScopes  []string             `json:"directory_scope_ids"`
	}
	if err := json.Unmarshal(customRole.RawJSON, &customPayload); err != nil {
		t.Fatalf("json.Unmarshal(customRole.RawJSON): %v", err)
	}
	if customPayload.RoleName != "Scoped Custom Role" || customPayload.RoleDefinitionID != "role-def-2" || customPayload.RoleTemplateID != "" || customPayload.RoleDisplayName != "Scoped Custom Role" {
		t.Fatalf("unexpected custom role payload %+v", customPayload)
	}
	if customPayload.AssignmentSource != entraAssignmentSourceGroup {
		t.Fatalf("custom assignment_source=%q want %q", customPayload.AssignmentSource, entraAssignmentSourceGroup)
	}
	if len(customPayload.GrantingGroups) != 1 || customPayload.GrantingGroups[0].ID != "group-1" {
		t.Fatalf("unexpected custom granting groups %+v", customPayload.GrantingGroups)
	}
	if len(customPayload.DirectoryScopes) != 1 || customPayload.DirectoryScopes[0] != "/administrativeUnits/au-1" {
		t.Fatalf("unexpected custom directory scopes %+v", customPayload.DirectoryScopes)
	}

	servicePrincipalRole := byAccountAndResource[entraServicePrincipalExternalID("sp-1")+"|entra_directory_role:tmpl-1"]
	var servicePrincipalPayload struct {
		RoleDefinitionID string `json:"role_definition_id"`
		AssignmentSource string `json:"assignment_source"`
	}
	if err := json.Unmarshal(servicePrincipalRole.RawJSON, &servicePrincipalPayload); err != nil {
		t.Fatalf("json.Unmarshal(servicePrincipalRole.RawJSON): %v", err)
	}
	if servicePrincipalPayload.RoleDefinitionID != "role-def-1" || servicePrincipalPayload.AssignmentSource != entraAssignmentSourceDirect {
		t.Fatalf("unexpected service principal role payload %+v", servicePrincipalPayload)
	}
}

func TestCollectEntraDirectoryRoleEntitlementsFallsBackWhenRoleDefinitionMissing(t *testing.T) {
	t.Parallel()

	integration := &EntraIntegration{
		client: entitlementsTestClient{
			directoryRoleAssignments: []DirectoryRoleAssignment{
				mustParseDirectoryRoleAssignment(t, `{"id":"assign-1","principalId":"user-1","roleDefinitionId":"role-def-missing","directoryScopeId":"/","principal":{"id":"user-1","@odata.type":"#microsoft.graph.user"}}`),
			},
		},
	}

	rows, err := integration.collectEntraDirectoryRoleEntitlements(context.Background(), func(registry.Event) {})
	if err != nil {
		t.Fatalf("collectEntraDirectoryRoleEntitlements() error = %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("len(rows)=%d want 1", len(rows))
	}

	row := rows[0]
	if row.AccountExternalID != "user-1" {
		t.Fatalf("row.AccountExternalID=%q want %q", row.AccountExternalID, "user-1")
	}
	if row.Resource != "entra_directory_role:role-def-missing" {
		t.Fatalf("row.Resource=%q want %q", row.Resource, "entra_directory_role:role-def-missing")
	}

	var payload struct {
		RoleName         string   `json:"role_name"`
		RoleDefinitionID string   `json:"role_definition_id"`
		RoleTemplateID   string   `json:"role_template_id"`
		RoleDisplayName  string   `json:"role_display_name"`
		AssignmentSource string   `json:"assignment_source"`
		DirectoryScopes  []string `json:"directory_scope_ids"`
	}
	if err := json.Unmarshal(row.RawJSON, &payload); err != nil {
		t.Fatalf("json.Unmarshal(row.RawJSON): %v", err)
	}
	if payload.RoleDefinitionID != "role-def-missing" {
		t.Fatalf("payload.RoleDefinitionID=%q want %q", payload.RoleDefinitionID, "role-def-missing")
	}
	if payload.RoleTemplateID != "" {
		t.Fatalf("payload.RoleTemplateID=%q want empty", payload.RoleTemplateID)
	}
	if payload.RoleName != "role-def-missing" || payload.RoleDisplayName != "role-def-missing" {
		t.Fatalf("unexpected fallback payload %+v", payload)
	}
	if payload.AssignmentSource != entraAssignmentSourceDirect {
		t.Fatalf("payload.AssignmentSource=%q want %q", payload.AssignmentSource, entraAssignmentSourceDirect)
	}
	if len(payload.DirectoryScopes) != 1 || payload.DirectoryScopes[0] != "/" {
		t.Fatalf("unexpected directory scopes %+v", payload.DirectoryScopes)
	}
}

func TestCollectEntraDirectoryRoleEntitlementsSkipsAssignmentsWithoutExpandedPrincipal(t *testing.T) {
	t.Parallel()

	integration := &EntraIntegration{
		client: entitlementsTestClient{
			directoryRoles: []DirectoryRole{
				mustParseDirectoryRole(t, `{"id":"role-def-1","displayName":"Global Administrator","templateId":"tmpl-1"}`),
			},
			directoryRoleAssignments: []DirectoryRoleAssignment{
				mustParseDirectoryRoleAssignment(t, `{"id":"assign-1","principalId":"user-1","roleDefinitionId":"role-def-1","directoryScopeId":"/","principal":{"id":"user-1","@odata.type":"#microsoft.graph.user"}}`),
				mustParseDirectoryRoleAssignment(t, `{"id":"assign-2","principalId":"user-2","roleDefinitionId":"role-def-1","directoryScopeId":"/"}`),
			},
		},
	}

	rows, err := integration.collectEntraDirectoryRoleEntitlements(context.Background(), func(registry.Event) {})
	if err != nil {
		t.Fatalf("collectEntraDirectoryRoleEntitlements() error = %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("len(rows)=%d want 1", len(rows))
	}
	if rows[0].AccountExternalID != "user-1" {
		t.Fatalf("rows[0].AccountExternalID=%q want %q", rows[0].AccountExternalID, "user-1")
	}
	if rows[0].Resource != "entra_directory_role:tmpl-1" {
		t.Fatalf("rows[0].Resource=%q want %q", rows[0].Resource, "entra_directory_role:tmpl-1")
	}
}

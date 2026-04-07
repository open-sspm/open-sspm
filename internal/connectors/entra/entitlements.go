package entra

import (
	"context"
	"fmt"
	"sort"
	"strings"
	gosync "sync"
	"sync/atomic"

	msgraphmodels "github.com/microsoftgraph/msgraph-sdk-go/models"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

const (
	entraEntitlementBatchSize      = 5000
	entraEntitlementWorkers        = 10
	entraDefaultAppRoleID          = "00000000-0000-0000-0000-000000000000"
	entraDefaultAppRolePermission  = "Default access"
	entraAssignmentSourceDirect    = "direct"
	entraAssignmentSourceGroup     = "group"
	entraDirectoryRolePermission   = "member"
	entraServicePrincipalRefPrefix = "entra_service_principal:"
	entraDirectoryRoleRefPrefix    = "entra_directory_role:"
)

type entraEntitlementUpsertRow struct {
	AccountExternalID string
	Kind              string
	Resource          string
	Permission        string
	RawJSON           []byte
}

type entraGrantingGroup struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type entraAppEntitlementAccumulator struct {
	AccountExternalID    string
	Resource             string
	Permission           string
	RoleName             string
	ServicePrincipalID   string
	ServicePrincipalName string
	AppRoleID            string
	AppRoleValue         string
	AppRoleDisplayName   string
	AssignmentSource     string
	GrantingGroups       map[string]entraGrantingGroup
}

type entraDirectoryRoleEntitlementAccumulator struct {
	AccountExternalID string
	Resource          string
	Permission        string
	RoleDefinitionID  string
	RoleTemplateID    string
	RoleDisplayName   string
	AssignmentSource  string
	GrantingGroups    map[string]entraGrantingGroup
	DirectoryScopes   map[string]struct{}
	AppScopes         map[string]struct{}
}

type entraGroupAppGrant struct {
	ServicePrincipalID   string
	ServicePrincipalName string
	AppRoleID            string
	AppRoleValue         string
	AppRoleDisplayName   string
	Resource             string
	Permission           string
	RoleName             string
	GroupID              string
	GroupName            string
}

type entraGroupDirectoryRoleGrant struct {
	RoleDefinitionID string
	RoleTemplateID   string
	RoleDisplayName  string
	Resource         string
	Permission       string
	DirectoryScopeID string
	AppScopeID       string
	GroupID          string
	GroupName        string
}

type entraSPAssignmentsResult struct {
	ServicePrincipal ServicePrincipal
	Assignments      []ServicePrincipalAppRoleAssignment
	Err              error
}

type entraGroupMembersResult struct {
	GroupID string
	Users   []User
	Err     error
}

func (i *EntraIntegration) collectEntraEntitlements(ctx context.Context, report func(registry.Event), servicePrincipals []ServicePrincipal) ([]entraEntitlementUpsertRow, error) {
	appRows, err := i.collectEntraAppRoleEntitlements(ctx, report, servicePrincipals)
	if err != nil {
		return nil, err
	}

	roleRows, err := i.collectEntraDirectoryRoleEntitlements(ctx, report)
	if err != nil {
		return nil, err
	}

	rows := make([]entraEntitlementUpsertRow, 0, len(appRows)+len(roleRows))
	rows = append(rows, appRows...)
	rows = append(rows, roleRows...)
	sortEntraEntitlementRows(rows)
	return rows, nil
}

func (i *EntraIntegration) collectEntraAppRoleEntitlements(ctx context.Context, report func(registry.Event), servicePrincipals []ServicePrincipal) ([]entraEntitlementUpsertRow, error) {
	report(registry.Event{
		Source:  "entra",
		Stage:   "list-entitlements",
		Current: 0,
		Total:   int64(len(servicePrincipals)),
		Message: fmt.Sprintf("listing enterprise app assignments for %d service principals", len(servicePrincipals)),
	})
	if len(servicePrincipals) == 0 {
		return nil, nil
	}

	assignmentsCtx, cancelAssignments := context.WithCancel(ctx)
	defer cancelAssignments()

	jobs := make(chan ServicePrincipal, len(servicePrincipals))
	results := make(chan entraSPAssignmentsResult, len(servicePrincipals))
	var assignmentsDone int64

	workers := min(len(servicePrincipals), entraEntitlementWorkers)
	if workers < 1 {
		workers = 1
	}

	var wg gosync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sp := range jobs {
				if assignmentsCtx.Err() != nil {
					return
				}
				assignments, err := i.client.ListServicePrincipalAssignedTo(assignmentsCtx, entityID(sp))
				if err != nil {
					results <- entraSPAssignmentsResult{
						ServicePrincipal: sp,
						Err:              fmt.Errorf("entra service principal %s assignments: %w", entityID(sp), err),
					}
					cancelAssignments()
					continue
				}
				n := atomic.AddInt64(&assignmentsDone, 1)
				report(registry.Event{
					Source:  "entra",
					Stage:   "list-entitlements",
					Current: n,
					Total:   int64(len(servicePrincipals)),
					Message: fmt.Sprintf("service principals %d/%d", n, len(servicePrincipals)),
				})
				results <- entraSPAssignmentsResult{ServicePrincipal: sp, Assignments: assignments}
			}
		}()
	}

	for _, sp := range servicePrincipals {
		jobs <- sp
	}
	close(jobs)
	wg.Wait()
	close(results)

	appEntitlements := make(map[string]*entraAppEntitlementAccumulator)
	groupGrants := make(map[string][]entraGroupAppGrant)
	var firstErr error

	for result := range results {
		if result.Err != nil {
			if firstErr == nil {
				firstErr = result.Err
			}
			continue
		}
		for _, assignment := range result.Assignments {
			principalID := uuidValueString(assignment.GetPrincipalId())
			if principalID == "" || uuidValueString(assignment.GetResourceId()) == "" {
				continue
			}

			appRoleID, appRoleValue, appRoleDisplayName, roleName := entraAppRoleDetails(result.ServicePrincipal, uuidValueString(assignment.GetAppRoleId()))
			permission := entraAppRolePermissionKey(result.ServicePrincipal, appRoleID, roleName)
			if strings.TrimSpace(permission) == "" {
				continue
			}

			servicePrincipalID := entityID(result.ServicePrincipal)
			servicePrincipalName := firstNonEmptyTrimmed(stringValue(result.ServicePrincipal.GetDisplayName()), stringValue(assignment.GetResourceDisplayName()), servicePrincipalID)
			resource := entraServicePrincipalResourceRef(servicePrincipalID)
			switch normalizeAppRoleAssignmentPrincipalType(stringValue(assignment.GetPrincipalType())) {
			case "user":
				mergeEntraAppEntitlement(appEntitlements, principalID, entraAssignmentSourceDirect, entraGrantingGroup{}, servicePrincipalID, servicePrincipalName, resource, permission, roleName, appRoleID, appRoleValue, appRoleDisplayName)
			case "group":
				groupID := principalID
				groupName := firstNonEmptyTrimmed(stringValue(assignment.GetPrincipalDisplayName()), groupID)
				groupGrants[groupID] = append(groupGrants[groupID], entraGroupAppGrant{
					ServicePrincipalID:   servicePrincipalID,
					ServicePrincipalName: servicePrincipalName,
					AppRoleID:            appRoleID,
					AppRoleValue:         appRoleValue,
					AppRoleDisplayName:   appRoleDisplayName,
					Resource:             resource,
					Permission:           permission,
					RoleName:             roleName,
					GroupID:              groupID,
					GroupName:            groupName,
				})
			case "service_principal":
				mergeEntraAppEntitlement(appEntitlements, entraServicePrincipalExternalID(principalID), entraAssignmentSourceDirect, entraGrantingGroup{}, servicePrincipalID, servicePrincipalName, resource, permission, roleName, appRoleID, appRoleValue, appRoleDisplayName)
			}
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}

	groupIDs := make([]string, 0, len(groupGrants))
	for groupID := range groupGrants {
		groupIDs = append(groupIDs, groupID)
	}
	sort.Strings(groupIDs)

	report(registry.Event{
		Source:  "entra",
		Stage:   "list-entitlements",
		Current: 0,
		Total:   int64(len(groupIDs)),
		Message: fmt.Sprintf("listing members for %d granting groups", len(groupIDs)),
	})
	if len(groupIDs) == 0 {
		return buildEntraAppEntitlementRows(appEntitlements), nil
	}

	groupCtx, cancelGroups := context.WithCancel(ctx)
	defer cancelGroups()

	groupJobs := make(chan string, len(groupIDs))
	groupResults := make(chan entraGroupMembersResult, len(groupIDs))
	var groupsDone int64

	groupWorkers := min(len(groupIDs), entraEntitlementWorkers)
	if groupWorkers < 1 {
		groupWorkers = 1
	}

	for worker := 0; worker < groupWorkers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for groupID := range groupJobs {
				if groupCtx.Err() != nil {
					return
				}
				users, err := i.client.ListGroupUserMembers(groupCtx, groupID)
				if err != nil {
					groupResults <- entraGroupMembersResult{
						GroupID: groupID,
						Err:     fmt.Errorf("entra group %s members: %w", strings.TrimSpace(groupID), err),
					}
					cancelGroups()
					continue
				}
				n := atomic.AddInt64(&groupsDone, 1)
				report(registry.Event{
					Source:  "entra",
					Stage:   "list-entitlements",
					Current: n,
					Total:   int64(len(groupIDs)),
					Message: fmt.Sprintf("granting groups %d/%d", n, len(groupIDs)),
				})
				groupResults <- entraGroupMembersResult{GroupID: groupID, Users: users}
			}
		}()
	}

	for _, groupID := range groupIDs {
		groupJobs <- groupID
	}
	close(groupJobs)
	wg.Wait()
	close(groupResults)

	firstErr = nil
	for result := range groupResults {
		if result.Err != nil {
			if firstErr == nil {
				firstErr = result.Err
			}
			continue
		}
		grants := groupGrants[result.GroupID]
		for _, user := range result.Users {
			accountExternalID := entityID(user)
			if accountExternalID == "" {
				continue
			}
			for _, grant := range grants {
				mergeEntraAppEntitlement(appEntitlements, accountExternalID, entraAssignmentSourceGroup, entraGrantingGroup{
					ID:   grant.GroupID,
					Name: grant.GroupName,
				}, grant.ServicePrincipalID, grant.ServicePrincipalName, grant.Resource, grant.Permission, grant.RoleName, grant.AppRoleID, grant.AppRoleValue, grant.AppRoleDisplayName)
			}
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}

	return buildEntraAppEntitlementRows(appEntitlements), nil
}

func (i *EntraIntegration) collectEntraDirectoryRoleEntitlements(ctx context.Context, report func(registry.Event)) ([]entraEntitlementUpsertRow, error) {
	report(registry.Event{Source: "entra", Stage: "list-entitlements", Current: 0, Total: 2, Message: "listing directory role definitions"})
	roles, err := i.client.ListDirectoryRoles(ctx)
	if err != nil {
		return nil, fmt.Errorf("entra directory roles: %w", err)
	}
	report(registry.Event{
		Source:  "entra",
		Stage:   "list-entitlements",
		Current: 1,
		Total:   2,
		Message: fmt.Sprintf("found %d directory role definitions", len(roles)),
	})

	roleByID := make(map[string]DirectoryRole, len(roles))
	for _, role := range roles {
		roleID := entityID(role)
		if roleID == "" {
			continue
		}
		roleByID[roleID] = role
	}

	assignments, err := i.client.ListDirectoryRoleAssignments(ctx)
	if err != nil {
		return nil, fmt.Errorf("entra directory role assignments: %w", err)
	}
	report(registry.Event{
		Source:  "entra",
		Stage:   "list-entitlements",
		Current: 2,
		Total:   2,
		Message: fmt.Sprintf("found %d directory role assignments", len(assignments)),
	})
	if len(assignments) == 0 {
		return nil, nil
	}

	roleEntitlements := make(map[string]*entraDirectoryRoleEntitlementAccumulator)
	groupGrants := make(map[string][]entraGroupDirectoryRoleGrant)
	for _, assignment := range assignments {
		roleDefinitionID := stringValue(assignment.GetRoleDefinitionId())
		principal := assignment.GetPrincipal()
		principalID := firstNonEmptyTrimmed(stringValue(assignment.GetPrincipalId()), entityID(principal))
		if roleDefinitionID == "" || principalID == "" || principal == nil {
			continue
		}

		role := roleByID[roleDefinitionID]
		roleTemplateID, roleDisplayName, resource := entraDirectoryRoleAssignmentDetails(role, roleDefinitionID)
		if resource == "" {
			continue
		}
		directoryScopeID := stringValue(assignment.GetDirectoryScopeId())
		appScopeID := stringValue(assignment.GetAppScopeId())
		principalType := normalizeDirectoryRoleAssignmentPrincipalType(stringValue(principal.GetOdataType()))

		switch principalType {
		case "user":
			mergeEntraDirectoryRoleEntitlement(
				roleEntitlements,
				principalID,
				entraAssignmentSourceDirect,
				entraGrantingGroup{},
				roleDefinitionID,
				roleTemplateID,
				roleDisplayName,
				resource,
				entraDirectoryRolePermission,
				directoryScopeID,
				appScopeID,
			)
		case "service_principal":
			mergeEntraDirectoryRoleEntitlement(
				roleEntitlements,
				entraServicePrincipalExternalID(principalID),
				entraAssignmentSourceDirect,
				entraGrantingGroup{},
				roleDefinitionID,
				roleTemplateID,
				roleDisplayName,
				resource,
				entraDirectoryRolePermission,
				directoryScopeID,
				appScopeID,
			)
		case "group":
			groupID := principalID
			groupName := firstNonEmptyTrimmed(entraDirectoryObjectDisplayName(principal), groupID)
			groupGrants[groupID] = append(groupGrants[groupID], entraGroupDirectoryRoleGrant{
				RoleDefinitionID: roleDefinitionID,
				RoleTemplateID:   roleTemplateID,
				RoleDisplayName:  roleDisplayName,
				Resource:         resource,
				Permission:       entraDirectoryRolePermission,
				DirectoryScopeID: directoryScopeID,
				AppScopeID:       appScopeID,
				GroupID:          groupID,
				GroupName:        groupName,
			})
		}
	}

	groupIDs := make([]string, 0, len(groupGrants))
	for groupID := range groupGrants {
		groupIDs = append(groupIDs, groupID)
	}
	sort.Strings(groupIDs)

	report(registry.Event{
		Source:  "entra",
		Stage:   "list-entitlements",
		Current: 0,
		Total:   int64(len(groupIDs)),
		Message: fmt.Sprintf("listing members for %d role-assignable groups", len(groupIDs)),
	})
	if len(groupIDs) == 0 {
		return buildEntraDirectoryRoleEntitlementRows(roleEntitlements), nil
	}

	groupCtx, cancelGroups := context.WithCancel(ctx)
	defer cancelGroups()

	groupJobs := make(chan string, len(groupIDs))
	groupResults := make(chan entraGroupMembersResult, len(groupIDs))
	var groupsDone int64

	groupWorkers := min(len(groupIDs), entraEntitlementWorkers)
	if groupWorkers < 1 {
		groupWorkers = 1
	}

	var wg gosync.WaitGroup
	for worker := 0; worker < groupWorkers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for groupID := range groupJobs {
				if groupCtx.Err() != nil {
					return
				}
				users, err := i.client.ListGroupUserMembers(groupCtx, groupID)
				if err != nil {
					groupResults <- entraGroupMembersResult{
						GroupID: groupID,
						Err:     fmt.Errorf("entra group %s members: %w", strings.TrimSpace(groupID), err),
					}
					cancelGroups()
					continue
				}
				n := atomic.AddInt64(&groupsDone, 1)
				report(registry.Event{
					Source:  "entra",
					Stage:   "list-entitlements",
					Current: n,
					Total:   int64(len(groupIDs)),
					Message: fmt.Sprintf("role-assignable groups %d/%d", n, len(groupIDs)),
				})
				groupResults <- entraGroupMembersResult{GroupID: groupID, Users: users}
			}
		}()
	}

	for _, groupID := range groupIDs {
		groupJobs <- groupID
	}
	close(groupJobs)
	wg.Wait()
	close(groupResults)

	var firstErr error
	for result := range groupResults {
		if result.Err != nil {
			if firstErr == nil {
				firstErr = result.Err
			}
			continue
		}
		grants := groupGrants[result.GroupID]
		for _, user := range result.Users {
			accountExternalID := entityID(user)
			if accountExternalID == "" {
				continue
			}
			for _, grant := range grants {
				mergeEntraDirectoryRoleEntitlement(
					roleEntitlements,
					accountExternalID,
					entraAssignmentSourceGroup,
					entraGrantingGroup{ID: grant.GroupID, Name: grant.GroupName},
					grant.RoleDefinitionID,
					grant.RoleTemplateID,
					grant.RoleDisplayName,
					grant.Resource,
					grant.Permission,
					grant.DirectoryScopeID,
					grant.AppScopeID,
				)
			}
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}

	return buildEntraDirectoryRoleEntitlementRows(roleEntitlements), nil
}

func normalizeAppRoleAssignmentPrincipalType(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	switch value {
	case "user":
		return "user"
	case "group":
		return "group"
	case "serviceprincipal", "service_principal", "service principal":
		return "service_principal"
	default:
		return ""
	}
}

func (i *EntraIntegration) upsertEntraEntitlements(ctx context.Context, q *gen.Queries, report func(registry.Event), runID int64, rows []entraEntitlementUpsertRow) error {
	report(registry.Event{
		Source:  "entra",
		Stage:   "write-entitlements",
		Current: 0,
		Total:   int64(len(rows)),
		Message: fmt.Sprintf("writing %d entitlements", len(rows)),
	})
	if len(rows) == 0 {
		return nil
	}

	for start := 0; start < len(rows); start += entraEntitlementBatchSize {
		end := min(start+entraEntitlementBatchSize, len(rows))
		batch := rows[start:end]

		accountExternalIDs := make([]string, 0, len(batch))
		kinds := make([]string, 0, len(batch))
		resources := make([]string, 0, len(batch))
		permissions := make([]string, 0, len(batch))
		rawJSONs := make([][]byte, 0, len(batch))
		for _, row := range batch {
			accountExternalIDs = append(accountExternalIDs, row.AccountExternalID)
			kinds = append(kinds, row.Kind)
			resources = append(resources, row.Resource)
			permissions = append(permissions, row.Permission)
			rawJSONs = append(rawJSONs, row.RawJSON)
		}

		if _, err := q.UpsertEntitlementsBulkBySource(ctx, gen.UpsertEntitlementsBulkBySourceParams{
			SourceKind:         "entra",
			SourceName:         i.tenantID,
			SeenInRunID:        runID,
			AccountExternalIds: accountExternalIDs,
			Kinds:              kinds,
			Resources:          resources,
			Permissions:        permissions,
			RawJsons:           rawJSONs,
		}); err != nil {
			return fmt.Errorf("upsert entra entitlements: %w", err)
		}

		report(registry.Event{
			Source:  "entra",
			Stage:   "write-entitlements",
			Current: int64(end),
			Total:   int64(len(rows)),
			Message: fmt.Sprintf("entitlements %d/%d", end, len(rows)),
		})
	}

	return nil
}

func mergeEntraAppEntitlement(
	acc map[string]*entraAppEntitlementAccumulator,
	accountExternalID string,
	assignmentSource string,
	group entraGrantingGroup,
	servicePrincipalID string,
	servicePrincipalName string,
	resource string,
	permission string,
	roleName string,
	appRoleID string,
	appRoleValue string,
	appRoleDisplayName string,
) {
	accountExternalID = strings.TrimSpace(accountExternalID)
	assignmentSource = strings.TrimSpace(assignmentSource)
	resource = strings.TrimSpace(resource)
	permission = strings.TrimSpace(permission)
	roleName = strings.TrimSpace(roleName)
	if accountExternalID == "" || assignmentSource == "" || resource == "" || permission == "" {
		return
	}

	key := entraEntitlementKey(accountExternalID, "entra_app_role", resource, permission)
	record := acc[key]
	if record == nil {
		record = &entraAppEntitlementAccumulator{
			AccountExternalID:    accountExternalID,
			Resource:             resource,
			Permission:           permission,
			RoleName:             firstNonEmptyTrimmed(roleName, permission),
			ServicePrincipalID:   strings.TrimSpace(servicePrincipalID),
			ServicePrincipalName: strings.TrimSpace(servicePrincipalName),
			AppRoleID:            strings.TrimSpace(appRoleID),
			AppRoleValue:         strings.TrimSpace(appRoleValue),
			AppRoleDisplayName:   strings.TrimSpace(appRoleDisplayName),
			AssignmentSource:     assignmentSource,
			GrantingGroups:       make(map[string]entraGrantingGroup),
		}
		acc[key] = record
	}

	if record.AssignmentSource != entraAssignmentSourceDirect && assignmentSource == entraAssignmentSourceDirect {
		record.AssignmentSource = entraAssignmentSourceDirect
	}

	group.ID = strings.TrimSpace(group.ID)
	group.Name = strings.TrimSpace(group.Name)
	if group.ID != "" {
		record.GrantingGroups[group.ID] = group
	}
}

func buildEntraAppEntitlementRows(acc map[string]*entraAppEntitlementAccumulator) []entraEntitlementUpsertRow {
	rows := make([]entraEntitlementUpsertRow, 0, len(acc))
	for _, record := range acc {
		if record == nil {
			continue
		}

		grantingGroups := make([]entraGrantingGroup, 0, len(record.GrantingGroups))
		for _, group := range record.GrantingGroups {
			grantingGroups = append(grantingGroups, group)
		}
		sort.Slice(grantingGroups, func(i, j int) bool {
			left := strings.ToLower(strings.TrimSpace(grantingGroups[i].Name))
			right := strings.ToLower(strings.TrimSpace(grantingGroups[j].Name))
			if left == right {
				return grantingGroups[i].ID < grantingGroups[j].ID
			}
			if left == "" {
				return grantingGroups[i].ID < grantingGroups[j].ID
			}
			if right == "" {
				return grantingGroups[i].ID < grantingGroups[j].ID
			}
			return left < right
		})

		rows = append(rows, entraEntitlementUpsertRow{
			AccountExternalID: record.AccountExternalID,
			Kind:              "entra_app_role",
			Resource:          record.Resource,
			Permission:        record.Permission,
			RawJSON: registry.MarshalJSON(map[string]any{
				"service_principal_id":   record.ServicePrincipalID,
				"service_principal_name": record.ServicePrincipalName,
				"role_name":              record.RoleName,
				"app_role_id":            record.AppRoleID,
				"app_role_value":         record.AppRoleValue,
				"app_role_display_name":  record.AppRoleDisplayName,
				"assignment_source":      record.AssignmentSource,
				"granting_groups":        grantingGroups,
			}),
		})
	}
	sortEntraEntitlementRows(rows)
	return rows
}

func mergeEntraDirectoryRoleEntitlement(
	acc map[string]*entraDirectoryRoleEntitlementAccumulator,
	accountExternalID string,
	assignmentSource string,
	group entraGrantingGroup,
	roleDefinitionID string,
	roleTemplateID string,
	roleDisplayName string,
	resource string,
	permission string,
	directoryScopeID string,
	appScopeID string,
) {
	accountExternalID = strings.TrimSpace(accountExternalID)
	assignmentSource = strings.TrimSpace(assignmentSource)
	resource = strings.TrimSpace(resource)
	permission = strings.TrimSpace(permission)
	if accountExternalID == "" || assignmentSource == "" || resource == "" || permission == "" {
		return
	}

	key := entraEntitlementKey(accountExternalID, "entra_directory_role", resource, permission)
	record := acc[key]
	if record == nil {
		record = &entraDirectoryRoleEntitlementAccumulator{
			AccountExternalID: accountExternalID,
			Resource:          resource,
			Permission:        permission,
			RoleDefinitionID:  strings.TrimSpace(roleDefinitionID),
			RoleTemplateID:    strings.TrimSpace(roleTemplateID),
			RoleDisplayName:   strings.TrimSpace(roleDisplayName),
			AssignmentSource:  assignmentSource,
			GrantingGroups:    make(map[string]entraGrantingGroup),
			DirectoryScopes:   make(map[string]struct{}),
			AppScopes:         make(map[string]struct{}),
		}
		acc[key] = record
	}

	if record.AssignmentSource != entraAssignmentSourceDirect && assignmentSource == entraAssignmentSourceDirect {
		record.AssignmentSource = entraAssignmentSourceDirect
	}

	group.ID = strings.TrimSpace(group.ID)
	group.Name = strings.TrimSpace(group.Name)
	if group.ID != "" {
		record.GrantingGroups[group.ID] = group
	}

	directoryScopeID = strings.TrimSpace(directoryScopeID)
	if directoryScopeID != "" {
		record.DirectoryScopes[directoryScopeID] = struct{}{}
	}
	appScopeID = strings.TrimSpace(appScopeID)
	if appScopeID != "" {
		record.AppScopes[appScopeID] = struct{}{}
	}
}

func buildEntraDirectoryRoleEntitlementRows(acc map[string]*entraDirectoryRoleEntitlementAccumulator) []entraEntitlementUpsertRow {
	rows := make([]entraEntitlementUpsertRow, 0, len(acc))
	for _, record := range acc {
		if record == nil {
			continue
		}

		grantingGroups := make([]entraGrantingGroup, 0, len(record.GrantingGroups))
		for _, group := range record.GrantingGroups {
			grantingGroups = append(grantingGroups, group)
		}
		sort.Slice(grantingGroups, func(i, j int) bool {
			left := strings.ToLower(strings.TrimSpace(grantingGroups[i].Name))
			right := strings.ToLower(strings.TrimSpace(grantingGroups[j].Name))
			if left == right {
				return grantingGroups[i].ID < grantingGroups[j].ID
			}
			if left == "" {
				return grantingGroups[i].ID < grantingGroups[j].ID
			}
			if right == "" {
				return grantingGroups[i].ID < grantingGroups[j].ID
			}
			return left < right
		})

		directoryScopes := sortedMapKeys(record.DirectoryScopes)
		appScopes := sortedMapKeys(record.AppScopes)

		rows = append(rows, entraEntitlementUpsertRow{
			AccountExternalID: record.AccountExternalID,
			Kind:              "entra_directory_role",
			Resource:          record.Resource,
			Permission:        record.Permission,
			RawJSON: registry.MarshalJSON(map[string]any{
				"role_name":           record.RoleDisplayName,
				"role_definition_id":  record.RoleDefinitionID,
				"role_template_id":    record.RoleTemplateID,
				"role_display_name":   record.RoleDisplayName,
				"assignment_source":   record.AssignmentSource,
				"granting_groups":     grantingGroups,
				"directory_scope_ids": directoryScopes,
				"app_scope_ids":       appScopes,
			}),
		})
	}
	sortEntraEntitlementRows(rows)
	return rows
}

func normalizeDirectoryRoleAssignmentPrincipalType(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	switch value {
	case "#microsoft.graph.user", "microsoft.graph.user", "user":
		return "user"
	case "#microsoft.graph.group", "microsoft.graph.group", "group":
		return "group"
	case "#microsoft.graph.serviceprincipal", "microsoft.graph.serviceprincipal", "serviceprincipal", "service_principal", "service principal":
		return "service_principal"
	default:
		return ""
	}
}

func entraDirectoryRoleAssignmentDetails(role DirectoryRole, roleDefinitionID string) (roleTemplateID, roleDisplayName, resource string) {
	roleDefinitionID = strings.TrimSpace(roleDefinitionID)
	if roleDefinitionID == "" {
		return "", "", ""
	}

	resourceID := roleDefinitionID
	roleDisplayName = roleDefinitionID
	if role != nil {
		roleTemplateID = stringValue(role.GetTemplateId())
		resourceID = firstNonEmptyTrimmed(roleTemplateID, entityID(role), roleDefinitionID)
		roleDisplayName = firstNonEmptyTrimmed(stringValue(role.GetDisplayName()), roleTemplateID, roleDefinitionID)
	}

	return roleTemplateID, roleDisplayName, entraDirectoryRoleResourceRef(resourceID)
}

func sortedMapKeys(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	keys := make([]string, 0, len(set))
	for key := range set {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func entraAppRoleDetails(sp ServicePrincipal, appRoleID string) (resolvedID, value, displayName, permission string) {
	resolvedID = strings.TrimSpace(appRoleID)
	if resolvedID == "" || strings.EqualFold(resolvedID, entraDefaultAppRoleID) {
		return resolvedID, "", "", entraDefaultAppRolePermission
	}

	for _, role := range sp.GetAppRoles() {
		roleID := uuidValueString(role.GetId())
		if !strings.EqualFold(roleID, resolvedID) {
			continue
		}
		value = stringValue(role.GetValue())
		displayName = stringValue(role.GetDisplayName())
		switch {
		case displayName != "":
			permission = displayName
		case value != "":
			permission = value
		default:
			permission = entraDefaultAppRolePermission
		}
		return resolvedID, value, displayName, permission
	}

	return resolvedID, "", "", resolvedID
}

func entraAppRolePermissionKey(sp ServicePrincipal, appRoleID, permission string) string {
	appRoleID = strings.TrimSpace(appRoleID)
	permission = strings.TrimSpace(permission)
	if permission == "" {
		return ""
	}
	if appRoleID == "" {
		return permission
	}
	if !entraAppRolePermissionCollides(sp, appRoleID, permission) {
		return permission
	}
	return permission + " (" + appRoleID + ")"
}

func entraAppRolePermissionCollides(sp ServicePrincipal, appRoleID, permission string) bool {
	appRoleID = strings.TrimSpace(appRoleID)
	permission = strings.TrimSpace(permission)
	if appRoleID == "" || permission == "" {
		return false
	}

	matches := 0
	if strings.EqualFold(appRoleID, entraDefaultAppRoleID) && permission == entraDefaultAppRolePermission {
		matches++
	}

	for _, role := range sp.GetAppRoles() {
		rolePermission := firstNonEmptyTrimmed(stringValue(role.GetDisplayName()), stringValue(role.GetValue()), entraDefaultAppRolePermission)
		if !strings.EqualFold(rolePermission, permission) {
			continue
		}
		matches++
		if matches > 1 {
			return true
		}
	}

	return false
}

func entraDirectoryObjectDisplayName(object msgraphmodels.DirectoryObjectable) string {
	switch typed := object.(type) {
	case msgraphmodels.Userable:
		return stringValue(typed.GetDisplayName())
	case msgraphmodels.Groupable:
		return stringValue(typed.GetDisplayName())
	case msgraphmodels.ServicePrincipalable:
		return stringValue(typed.GetDisplayName())
	default:
		return ""
	}
}

func entraServicePrincipalResourceRef(servicePrincipalID string) string {
	servicePrincipalID = strings.TrimSpace(servicePrincipalID)
	if servicePrincipalID == "" {
		return ""
	}
	return entraServicePrincipalRefPrefix + servicePrincipalID
}

func entraDirectoryRoleResourceRef(roleTemplateID string) string {
	roleTemplateID = strings.TrimSpace(roleTemplateID)
	if roleTemplateID == "" {
		return ""
	}
	return entraDirectoryRoleRefPrefix + roleTemplateID
}

func entraEntitlementKey(accountExternalID, kind, resource, permission string) string {
	return strings.TrimSpace(accountExternalID) + "\x00" + strings.TrimSpace(kind) + "\x00" + strings.TrimSpace(resource) + "\x00" + strings.TrimSpace(permission)
}

func sortEntraEntitlementRows(rows []entraEntitlementUpsertRow) {
	sort.Slice(rows, func(i, j int) bool {
		left := rows[i]
		right := rows[j]
		if left.AccountExternalID == right.AccountExternalID {
			if left.Kind == right.Kind {
				if left.Resource == right.Resource {
					return left.Permission < right.Permission
				}
				return left.Resource < right.Resource
			}
			return left.Kind < right.Kind
		}
		return left.AccountExternalID < right.AccountExternalID
	})
}

func firstNonEmptyTrimmed(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

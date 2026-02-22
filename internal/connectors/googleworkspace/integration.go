package googleworkspace

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/discovery"
	"github.com/open-sspm/open-sspm/internal/metrics"
)

const (
	googleWorkspaceAccountBatchSize       = 1000
	googleWorkspaceEntitlementBatchSize   = 2000
	googleWorkspaceAssetBatchSize         = 1000
	googleWorkspaceOwnerBatchSize         = 2000
	googleWorkspaceCredentialBatchSize    = 2000
	googleWorkspaceAuditEventBatchSize    = 2000
	googleWorkspaceDiscoveryWatermarkSkew = 15 * time.Minute
)

type GoogleWorkspaceIntegration struct {
	client           *Client
	customerID       string
	primaryDomain    string
	discoveryEnabled bool
}

type googleWorkspaceAccountRow struct {
	ExternalID  string
	Email       string
	DisplayName string
	AccountKind string
	Status      string
	RawJSON     []byte
}

type googleWorkspaceEntitlementRow struct {
	AppUserExternalID string
	Kind              string
	Resource          string
	Permission        string
	RawJSON           []byte
}

type googleWorkspaceAppAssetRow struct {
	AssetKind        string
	ExternalID       string
	ParentExternalID string
	DisplayName      string
	Status           string
	CreatedAtSource  pgtype.Timestamptz
	UpdatedAtSource  pgtype.Timestamptz
	RawJSON          []byte
}

type googleWorkspaceAppAssetOwnerRow struct {
	AssetKind        string
	AssetExternalID  string
	OwnerKind        string
	OwnerExternalID  string
	OwnerDisplayName string
	OwnerEmail       string
	RawJSON          []byte
}

type googleWorkspaceCredentialArtifactRow struct {
	AssetRefKind          string
	AssetRefExternalID    string
	CredentialKind        string
	ExternalID            string
	DisplayName           string
	Fingerprint           string
	ScopeJSON             []byte
	Status                string
	CreatedAtSource       pgtype.Timestamptz
	ExpiresAtSource       pgtype.Timestamptz
	LastUsedAtSource      pgtype.Timestamptz
	CreatedByKind         string
	CreatedByExternalID   string
	CreatedByDisplayName  string
	ApprovedByKind        string
	ApprovedByExternalID  string
	ApprovedByDisplayName string
	RawJSON               []byte
}

type googleWorkspaceCredentialAuditEventRow struct {
	EventExternalID      string
	EventType            string
	EventTime            pgtype.Timestamptz
	ActorKind            string
	ActorExternalID      string
	ActorDisplayName     string
	TargetKind           string
	TargetExternalID     string
	TargetDisplayName    string
	CredentialKind       string
	CredentialExternalID string
	RawJSON              []byte
}

type normalizedDiscoverySource struct {
	CanonicalKey     string
	SourceAppID      string
	SourceAppName    string
	SourceAppDomain  string
	SourceVendorName string
	SeenAt           time.Time
}

type normalizedDiscoveryEvent struct {
	CanonicalKey     string
	SignalKind       string
	EventExternalID  string
	SourceAppID      string
	SourceAppName    string
	SourceAppDomain  string
	SourceVendorName string
	ActorExternalID  string
	ActorEmail       string
	ActorDisplayName string
	ObservedAt       time.Time
	Scopes           []string
	RawJSON          []byte
}

func NewGoogleWorkspaceIntegration(client *Client, customerID, primaryDomain string, discoveryEnabled bool) *GoogleWorkspaceIntegration {
	return &GoogleWorkspaceIntegration{
		client:           client,
		customerID:       strings.TrimSpace(customerID),
		primaryDomain:    strings.TrimSpace(primaryDomain),
		discoveryEnabled: discoveryEnabled,
	}
}

func (i *GoogleWorkspaceIntegration) Kind() string { return configstore.KindGoogleWorkspace }

func (i *GoogleWorkspaceIntegration) Name() string { return i.customerID }

func (i *GoogleWorkspaceIntegration) Role() registry.IntegrationRole { return registry.RoleApp }

func (i *GoogleWorkspaceIntegration) SupportsRunMode(mode registry.RunMode) bool {
	if i == nil {
		return false
	}
	switch mode.Normalize() {
	case registry.RunModeDiscovery:
		return i.discoveryEnabled
	default:
		return true
	}
}

func (i *GoogleWorkspaceIntegration) InitEvents() []registry.Event {
	return []registry.Event{
		{Source: configstore.KindGoogleWorkspace, Stage: "list-users", Current: 0, Total: 1, Message: "listing Google Workspace users"},
		{Source: configstore.KindGoogleWorkspace, Stage: "list-groups", Current: 0, Total: 1, Message: "listing Google Workspace groups"},
		{Source: configstore.KindGoogleWorkspace, Stage: "write-users", Current: 0, Total: registry.UnknownTotal, Message: "writing Google Workspace users and groups"},
		{Source: configstore.KindGoogleWorkspace, Stage: "list-group-members", Current: 0, Total: registry.UnknownTotal, Message: "listing Google Workspace group members"},
		{Source: configstore.KindGoogleWorkspace, Stage: "list-admin-roles", Current: 0, Total: 1, Message: "listing Google Workspace admin roles"},
		{Source: configstore.KindGoogleWorkspace, Stage: "write-entitlements", Current: 0, Total: registry.UnknownTotal, Message: "writing Google Workspace entitlements"},
		{Source: configstore.KindGoogleWorkspace, Stage: "list-oauth-grants", Current: 0, Total: 1, Message: "listing Google Workspace OAuth grants"},
		{Source: configstore.KindGoogleWorkspace, Stage: "write-app-assets", Current: 0, Total: registry.UnknownTotal, Message: "writing Google OAuth app assets"},
		{Source: configstore.KindGoogleWorkspace, Stage: "write-owners", Current: 0, Total: registry.UnknownTotal, Message: "writing Google OAuth app owners"},
		{Source: configstore.KindGoogleWorkspace, Stage: "write-credentials", Current: 0, Total: registry.UnknownTotal, Message: "writing Google OAuth credential artifacts"},
		{Source: configstore.KindGoogleWorkspace, Stage: "list-token-audit", Current: 0, Total: 1, Message: "listing Google token audit activities"},
		{Source: configstore.KindGoogleWorkspace, Stage: "write-audit-events", Current: 0, Total: registry.UnknownTotal, Message: "writing Google credential audit events"},
		{Source: configstore.KindGoogleWorkspace, Stage: "list-discovery-events", Current: 0, Total: 1, Message: "listing Google discovery activities"},
		{Source: configstore.KindGoogleWorkspace, Stage: "normalize-discovery", Current: 0, Total: 1, Message: "normalizing discovery evidence"},
		{Source: configstore.KindGoogleWorkspace, Stage: "write-discovery", Current: 0, Total: registry.UnknownTotal, Message: "writing discovery data"},
	}
}

func (i *GoogleWorkspaceIntegration) Run(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, report func(registry.Event), mode registry.RunMode) error {
	switch mode.Normalize() {
	case registry.RunModeDiscovery:
		if !i.SupportsRunMode(registry.RunModeDiscovery) {
			return nil
		}
		return i.runDiscovery(ctx, q, pool, report)
	default:
		return i.runFull(ctx, q, pool, report)
	}
}

func (i *GoogleWorkspaceIntegration) runFull(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, report func(registry.Event)) error {
	started := time.Now()
	runID, err := q.CreateSyncRun(ctx, gen.CreateSyncRunParams{
		SourceKind: registry.SyncRunSourceKind(configstore.KindGoogleWorkspace, registry.RunModeFull),
		SourceName: i.customerID,
	})
	if err != nil {
		return err
	}

	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "list-users", Current: 0, Total: 1, Message: "listing users"})
	users, err := i.client.ListUsers(ctx, i.customerID)
	if err != nil {
		report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "list-users", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
	}
	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "list-users", Current: 1, Total: 1, Message: fmt.Sprintf("found %d users", len(users))})

	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "list-groups", Current: 0, Total: 1, Message: "listing groups"})
	groups, err := i.client.ListGroups(ctx, i.customerID)
	if err != nil {
		report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "list-groups", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
	}
	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "list-groups", Current: 1, Total: 1, Message: fmt.Sprintf("found %d groups", len(groups))})

	accounts := buildGoogleWorkspaceAccountRows(users, groups)
	if err := i.upsertAccounts(ctx, q, report, runID, accounts); err != nil {
		report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-users", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}

	groupEntitlements, err := i.collectGroupMemberEntitlements(ctx, report, groups)
	if err != nil {
		report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "list-group-members", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
	}

	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "list-admin-roles", Current: 0, Total: 1, Message: "listing admin role assignments"})
	roles, err := i.client.ListAdminRoles(ctx, i.customerID)
	if err != nil {
		report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "list-admin-roles", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
	}
	assignments, err := i.client.ListAdminRoleAssignments(ctx, i.customerID)
	if err != nil {
		report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "list-admin-roles", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
	}
	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "list-admin-roles", Current: 1, Total: 1, Message: fmt.Sprintf("found %d roles and %d assignments", len(roles), len(assignments))})

	adminEntitlements := buildGoogleWorkspaceAdminRoleEntitlements(roles, assignments)
	allEntitlements := make([]googleWorkspaceEntitlementRow, 0, len(groupEntitlements)+len(adminEntitlements))
	allEntitlements = append(allEntitlements, groupEntitlements...)
	allEntitlements = append(allEntitlements, adminEntitlements...)
	if err := i.upsertEntitlements(ctx, q, report, runID, allEntitlements); err != nil {
		report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-entitlements", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}

	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "list-oauth-grants", Current: 0, Total: 1, Message: "listing OAuth grants"})
	grants, err := i.client.ListOAuthTokenGrants(ctx)
	if err != nil {
		report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "list-oauth-grants", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
	}
	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "list-oauth-grants", Current: 1, Total: 1, Message: fmt.Sprintf("found %d OAuth grants", len(grants))})

	assets, owners, credentials := i.buildOAuthInventoryRows(grants, users)
	if err := i.upsertAppAssets(ctx, q, report, runID, assets); err != nil {
		report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-app-assets", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}
	if err := i.upsertAppAssetOwners(ctx, q, report, runID, owners); err != nil {
		report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-owners", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}
	if err := i.upsertCredentialArtifacts(ctx, q, report, runID, credentials); err != nil {
		report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-credentials", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}

	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "list-token-audit", Current: 0, Total: 1, Message: "listing token audit activities"})
	tokenActivities, err := i.client.ListTokenActivities(ctx, nil)
	if err != nil {
		report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "list-token-audit", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
	}
	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "list-token-audit", Current: 1, Total: 1, Message: fmt.Sprintf("found %d token audit activities", len(tokenActivities))})

	auditRows := buildGoogleWorkspaceAuditEventRows(tokenActivities)
	if err := i.upsertCredentialAuditEvents(ctx, q, report, auditRows); err != nil {
		report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-audit-events", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}

	if err := registry.FinalizeAppRun(ctx, q, pool, runID, configstore.KindGoogleWorkspace, i.customerID, time.Since(started), false); err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}

	slog.Info("google workspace sync complete",
		"customer_id", i.customerID,
		"users", len(users),
		"groups", len(groups),
		"entitlements", len(allEntitlements),
		"oauth_assets", len(assets),
		"oauth_grants", len(credentials),
		"token_audit_events", len(auditRows),
	)
	return nil
}

func (i *GoogleWorkspaceIntegration) runDiscovery(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, report func(registry.Event)) error {
	started := time.Now()
	runID, err := q.CreateSyncRun(ctx, gen.CreateSyncRunParams{
		SourceKind: registry.SyncRunSourceKind(configstore.KindGoogleWorkspace, registry.RunModeDiscovery),
		SourceName: i.customerID,
	})
	if err != nil {
		return err
	}

	if err := i.syncDiscovery(ctx, q, report, runID); err != nil {
		report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-discovery", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindUnknown)
	}
	if err := registry.FinalizeDiscoveryRun(ctx, q, pool, runID, configstore.KindGoogleWorkspace, i.customerID, time.Since(started)); err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}
	slog.Info("google workspace discovery sync complete", "customer_id", i.customerID)
	return nil
}

func buildGoogleWorkspaceAccountRows(users []WorkspaceUser, groups []WorkspaceGroup) []googleWorkspaceAccountRow {
	rows := make([]googleWorkspaceAccountRow, 0, len(users)+len(groups))
	for _, user := range users {
		externalID := strings.TrimSpace(user.ID)
		if externalID == "" {
			continue
		}
		email := normalizeEmail(strings.TrimSpace(user.PrimaryEmail))
		displayName := strings.TrimSpace(user.Name.FullName)
		if displayName == "" {
			displayName = email
		}
		if displayName == "" {
			displayName = externalID
		}
		status := "active"
		if user.Suspended {
			status = "suspended"
		}

		raw := registry.WithEntityCategory(registry.MarshalJSON(map[string]any{
			"id":            externalID,
			"primary_email": strings.TrimSpace(user.PrimaryEmail),
			"full_name":     strings.TrimSpace(user.Name.FullName),
			"suspended":     user.Suspended,
			"status":        status,
		}), registry.EntityCategoryUser)

		rows = append(rows, googleWorkspaceAccountRow{
			ExternalID:  externalID,
			Email:       email,
			DisplayName: displayName,
			AccountKind: googleWorkspaceUserAccountKind(user),
			Status:      status,
			RawJSON:     raw,
		})
	}

	for _, group := range groups {
		externalID := strings.TrimSpace(group.ID)
		if externalID == "" {
			continue
		}
		email := normalizeEmail(strings.TrimSpace(group.Email))
		displayName := strings.TrimSpace(group.Name)
		if displayName == "" {
			displayName = email
		}
		if displayName == "" {
			displayName = externalID
		}

		raw := registry.WithEntityCategory(registry.MarshalJSON(map[string]any{
			"id":     externalID,
			"email":  strings.TrimSpace(group.Email),
			"name":   strings.TrimSpace(group.Name),
			"status": "active",
		}), registry.EntityCategoryGroup)

		rows = append(rows, googleWorkspaceAccountRow{
			ExternalID:  externalID,
			Email:       email,
			DisplayName: displayName,
			AccountKind: registry.AccountKindUnknown,
			Status:      "active",
			RawJSON:     raw,
		})
	}
	return rows
}

func googleWorkspaceUserAccountKind(user WorkspaceUser) string {
	signal := registry.ClassifyKindFromSignals(user.Name.FullName, user.PrimaryEmail, user.ID)
	if signal != registry.AccountKindUnknown {
		return signal
	}
	email := normalizeEmail(user.PrimaryEmail)
	if strings.HasSuffix(email, ".gserviceaccount.com") {
		return registry.AccountKindService
	}
	if email != "" {
		return registry.AccountKindHuman
	}
	return registry.AccountKindUnknown
}

func (i *GoogleWorkspaceIntegration) upsertAccounts(ctx context.Context, q *gen.Queries, report func(registry.Event), runID int64, rows []googleWorkspaceAccountRow) error {
	report(registry.Event{
		Source:  configstore.KindGoogleWorkspace,
		Stage:   "write-users",
		Current: 0,
		Total:   int64(len(rows)),
		Message: fmt.Sprintf("writing %d users and groups", len(rows)),
	})
	if len(rows) == 0 {
		return nil
	}

	for start := 0; start < len(rows); start += googleWorkspaceAccountBatchSize {
		end := min(start+googleWorkspaceAccountBatchSize, len(rows))
		batch := rows[start:end]

		externalIDs := make([]string, 0, len(batch))
		emails := make([]string, 0, len(batch))
		displayNames := make([]string, 0, len(batch))
		accountKinds := make([]string, 0, len(batch))
		rawJSONs := make([][]byte, 0, len(batch))
		lastLoginAts := make([]pgtype.Timestamptz, 0, len(batch))
		lastLoginIPs := make([]string, 0, len(batch))
		lastLoginRegions := make([]string, 0, len(batch))
		for _, row := range batch {
			externalIDs = append(externalIDs, row.ExternalID)
			emails = append(emails, row.Email)
			displayNames = append(displayNames, row.DisplayName)
			accountKinds = append(accountKinds, row.AccountKind)
			rawJSONs = append(rawJSONs, row.RawJSON)
			lastLoginAts = append(lastLoginAts, pgtype.Timestamptz{})
			lastLoginIPs = append(lastLoginIPs, "")
			lastLoginRegions = append(lastLoginRegions, "")
		}

		if _, err := q.UpsertAppUsersBulkBySource(ctx, gen.UpsertAppUsersBulkBySourceParams{
			SourceKind:       configstore.KindGoogleWorkspace,
			SourceName:       i.customerID,
			SeenInRunID:      runID,
			ExternalIds:      externalIDs,
			Emails:           emails,
			DisplayNames:     displayNames,
			AccountKinds:     accountKinds,
			RawJsons:         rawJSONs,
			LastLoginAts:     lastLoginAts,
			LastLoginIps:     lastLoginIPs,
			LastLoginRegions: lastLoginRegions,
		}); err != nil {
			return fmt.Errorf("upsert google workspace accounts: %w", err)
		}

		report(registry.Event{
			Source:  configstore.KindGoogleWorkspace,
			Stage:   "write-users",
			Current: int64(end),
			Total:   int64(len(rows)),
			Message: fmt.Sprintf("accounts %d/%d", end, len(rows)),
		})
	}
	return nil
}

func (i *GoogleWorkspaceIntegration) collectGroupMemberEntitlements(ctx context.Context, report func(registry.Event), groups []WorkspaceGroup) ([]googleWorkspaceEntitlementRow, error) {
	if len(groups) == 0 {
		return nil, nil
	}
	rows := make([]googleWorkspaceEntitlementRow, 0)
	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "list-group-members", Current: 0, Total: int64(len(groups)), Message: fmt.Sprintf("listing members for %d groups", len(groups))})

	for idx, group := range groups {
		groupID := strings.TrimSpace(group.ID)
		if groupID == "" {
			continue
		}
		members, err := i.client.ListGroupMembers(ctx, groupID)
		if err != nil {
			return nil, fmt.Errorf("list group members for %s: %w", groupID, err)
		}
		for _, member := range members {
			memberID := strings.TrimSpace(member.ID)
			if memberID == "" {
				continue
			}
			permission := strings.ToLower(strings.TrimSpace(member.Role))
			if permission == "" {
				permission = "member"
			}
			rows = append(rows, googleWorkspaceEntitlementRow{
				AppUserExternalID: memberID,
				Kind:              "google_group_member",
				Resource:          "google_group:" + groupID,
				Permission:        permission,
				RawJSON: registry.MarshalJSON(map[string]any{
					"group_id":      groupID,
					"group_email":   strings.TrimSpace(group.Email),
					"group_name":    strings.TrimSpace(group.Name),
					"member_id":     memberID,
					"member_email":  strings.TrimSpace(member.Email),
					"member_type":   strings.TrimSpace(member.Type),
					"member_status": strings.TrimSpace(member.Status),
					"member_role":   permission,
				}),
			})
		}
		report(registry.Event{
			Source:  configstore.KindGoogleWorkspace,
			Stage:   "list-group-members",
			Current: int64(idx + 1),
			Total:   int64(len(groups)),
			Message: fmt.Sprintf("groups %d/%d", idx+1, len(groups)),
		})
	}
	return rows, nil
}

func buildGoogleWorkspaceAdminRoleEntitlements(roles []WorkspaceAdminRole, assignments []WorkspaceAdminRoleAssignment) []googleWorkspaceEntitlementRow {
	roleByID := make(map[string]WorkspaceAdminRole, len(roles))
	for _, role := range roles {
		roleID := strings.TrimSpace(role.RoleID)
		if roleID == "" {
			continue
		}
		roleByID[roleID] = role
	}

	rows := make([]googleWorkspaceEntitlementRow, 0, len(assignments))
	for _, assignment := range assignments {
		roleID := strings.TrimSpace(assignment.RoleID)
		assignedTo := strings.TrimSpace(assignment.AssignedTo)
		if roleID == "" || assignedTo == "" {
			continue
		}
		permission := strings.ToLower(strings.TrimSpace(assignment.ScopeType))
		if permission == "" {
			permission = "global"
		}
		roleName := ""
		if role, ok := roleByID[roleID]; ok {
			roleName = strings.TrimSpace(role.RoleName)
		}
		rows = append(rows, googleWorkspaceEntitlementRow{
			AppUserExternalID: assignedTo,
			Kind:              "google_admin_role",
			Resource:          "google_admin_role:" + roleID,
			Permission:        permission,
			RawJSON: registry.MarshalJSON(map[string]any{
				"role_id":       roleID,
				"role_name":     roleName,
				"assigned_to":   assignedTo,
				"assignee_type": strings.TrimSpace(assignment.AssigneeType),
				"scope_type":    strings.TrimSpace(assignment.ScopeType),
				"org_unit_id":   strings.TrimSpace(assignment.OrgUnitID),
			}),
		})
	}
	return rows
}

func (i *GoogleWorkspaceIntegration) upsertEntitlements(ctx context.Context, q *gen.Queries, report func(registry.Event), runID int64, rows []googleWorkspaceEntitlementRow) error {
	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-entitlements", Current: 0, Total: int64(len(rows)), Message: fmt.Sprintf("writing %d entitlements", len(rows))})
	if len(rows) == 0 {
		return nil
	}

	for start := 0; start < len(rows); start += googleWorkspaceEntitlementBatchSize {
		end := min(start+googleWorkspaceEntitlementBatchSize, len(rows))
		batch := rows[start:end]

		appUserExternalIDs := make([]string, 0, len(batch))
		kinds := make([]string, 0, len(batch))
		resources := make([]string, 0, len(batch))
		permissions := make([]string, 0, len(batch))
		rawJSONs := make([][]byte, 0, len(batch))
		for _, row := range batch {
			appUserExternalIDs = append(appUserExternalIDs, row.AppUserExternalID)
			kinds = append(kinds, row.Kind)
			resources = append(resources, row.Resource)
			permissions = append(permissions, row.Permission)
			rawJSONs = append(rawJSONs, row.RawJSON)
		}

		if _, err := q.UpsertEntitlementsBulkBySource(ctx, gen.UpsertEntitlementsBulkBySourceParams{
			SourceKind:         configstore.KindGoogleWorkspace,
			SourceName:         i.customerID,
			SeenInRunID:        runID,
			AppUserExternalIds: appUserExternalIDs,
			Kinds:              kinds,
			Resources:          resources,
			Permissions:        permissions,
			RawJsons:           rawJSONs,
		}); err != nil {
			return fmt.Errorf("upsert google workspace entitlements: %w", err)
		}

		report(registry.Event{
			Source:  configstore.KindGoogleWorkspace,
			Stage:   "write-entitlements",
			Current: int64(end),
			Total:   int64(len(rows)),
			Message: fmt.Sprintf("entitlements %d/%d", end, len(rows)),
		})
	}
	return nil
}

func (i *GoogleWorkspaceIntegration) buildOAuthInventoryRows(grants []WorkspaceOAuthTokenGrant, users []WorkspaceUser) ([]googleWorkspaceAppAssetRow, []googleWorkspaceAppAssetOwnerRow, []googleWorkspaceCredentialArtifactRow) {
	userByID := make(map[string]WorkspaceUser, len(users))
	for _, user := range users {
		userByID[strings.TrimSpace(user.ID)] = user
	}

	assetByExternalID := map[string]googleWorkspaceAppAssetRow{}
	ownerRows := make([]googleWorkspaceAppAssetOwnerRow, 0, len(grants))
	credentialRows := make([]googleWorkspaceCredentialArtifactRow, 0, len(grants))

	for _, grant := range grants {
		clientExternalID := googleWorkspaceClientExternalID(grant)
		if clientExternalID == "" {
			continue
		}
		displayName := strings.TrimSpace(grant.DisplayText)
		if displayName == "" {
			displayName = clientExternalID
		}

		assetRow := googleWorkspaceAppAssetRow{
			AssetKind:        "google_oauth_client",
			ExternalID:       clientExternalID,
			ParentExternalID: "",
			DisplayName:      displayName,
			Status:           "active",
			CreatedAtSource:  pgtype.Timestamptz{},
			UpdatedAtSource:  pgtype.Timestamptz{},
			RawJSON: registry.MarshalJSON(map[string]any{
				"client_id":    strings.TrimSpace(grant.ClientID),
				"display_text": strings.TrimSpace(grant.DisplayText),
				"native_app":   grant.NativeApp,
				"anonymous":    grant.Anonymous,
			}),
		}
		if _, exists := assetByExternalID[clientExternalID]; !exists {
			assetByExternalID[clientExternalID] = assetRow
		}

		ownerExternalID := strings.TrimSpace(grant.UserKey)
		ownerEmail := ""
		ownerDisplayName := ownerExternalID
		if user, ok := userByID[ownerExternalID]; ok {
			ownerEmail = normalizeEmail(strings.TrimSpace(user.PrimaryEmail))
			ownerDisplayName = strings.TrimSpace(user.Name.FullName)
			if ownerDisplayName == "" {
				ownerDisplayName = ownerEmail
			}
		}
		if ownerDisplayName == "" {
			ownerDisplayName = ownerEmail
		}
		if ownerDisplayName == "" {
			ownerDisplayName = ownerExternalID
		}
		if ownerEmail == "" {
			ownerEmail = normalizeEmail(ownerExternalID)
		}

		if ownerExternalID != "" {
			ownerRows = append(ownerRows, googleWorkspaceAppAssetOwnerRow{
				AssetKind:        "google_oauth_client",
				AssetExternalID:  clientExternalID,
				OwnerKind:        "google_user",
				OwnerExternalID:  ownerExternalID,
				OwnerDisplayName: ownerDisplayName,
				OwnerEmail:       ownerEmail,
				RawJSON: registry.MarshalJSON(map[string]any{
					"user_key": ownerExternalID,
					"email":    ownerEmail,
				}),
			})
		}

		credentialRows = append(credentialRows, googleWorkspaceCredentialArtifactRow{
			AssetRefKind:          "google_oauth_client",
			AssetRefExternalID:    appAssetRefExternalID("google_oauth_client", clientExternalID),
			CredentialKind:        "google_oauth_grant",
			ExternalID:            googleWorkspaceGrantExternalID(clientExternalID, ownerExternalID),
			DisplayName:           displayName,
			Fingerprint:           "",
			ScopeJSON:             discovery.ScopesJSON(grant.Scopes),
			Status:                "active",
			CreatedAtSource:       pgtype.Timestamptz{},
			ExpiresAtSource:       pgtype.Timestamptz{},
			LastUsedAtSource:      pgtype.Timestamptz{},
			CreatedByKind:         "google_user",
			CreatedByExternalID:   ownerExternalID,
			CreatedByDisplayName:  ownerDisplayName,
			ApprovedByKind:        "",
			ApprovedByExternalID:  "",
			ApprovedByDisplayName: "",
			RawJSON: registry.MarshalJSON(map[string]any{
				"user_key":     ownerExternalID,
				"client_id":    strings.TrimSpace(grant.ClientID),
				"display_text": strings.TrimSpace(grant.DisplayText),
				"scopes":       discovery.NormalizeScopes(grant.Scopes),
				"native_app":   grant.NativeApp,
				"anonymous":    grant.Anonymous,
			}),
		})
	}

	assets := make([]googleWorkspaceAppAssetRow, 0, len(assetByExternalID))
	for _, row := range assetByExternalID {
		assets = append(assets, row)
	}
	return assets, ownerRows, credentialRows
}

func googleWorkspaceClientExternalID(grant WorkspaceOAuthTokenGrant) string {
	clientID := strings.TrimSpace(grant.ClientID)
	if clientID != "" {
		return clientID
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(strings.TrimSpace(grant.UserKey)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strings.TrimSpace(grant.DisplayText)))
	for _, scope := range discovery.NormalizeScopes(grant.Scopes) {
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(scope))
	}
	return fmt.Sprintf("google_oauth_client:%x", h.Sum64())
}

func googleWorkspaceGrantExternalID(clientExternalID, userKey string) string {
	clientExternalID = strings.TrimSpace(clientExternalID)
	userKey = strings.TrimSpace(userKey)
	h := fnv.New64a()
	_, _ = h.Write([]byte(clientExternalID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(userKey))
	return fmt.Sprintf("grant:%s:%x", clientExternalID, h.Sum64())
}

func appAssetRefExternalID(assetKind, externalID string) string {
	assetKind = strings.TrimSpace(assetKind)
	externalID = strings.TrimSpace(externalID)
	if assetKind == "" {
		return externalID
	}
	if externalID == "" {
		return assetKind
	}
	return assetKind + ":" + externalID
}

func (i *GoogleWorkspaceIntegration) upsertAppAssets(ctx context.Context, q *gen.Queries, report func(registry.Event), runID int64, rows []googleWorkspaceAppAssetRow) error {
	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-app-assets", Current: 0, Total: int64(len(rows)), Message: fmt.Sprintf("writing %d app assets", len(rows))})
	if len(rows) == 0 {
		return nil
	}
	for start := 0; start < len(rows); start += googleWorkspaceAssetBatchSize {
		end := min(start+googleWorkspaceAssetBatchSize, len(rows))
		batch := rows[start:end]

		assetKinds := make([]string, 0, len(batch))
		externalIDs := make([]string, 0, len(batch))
		parentExternalIDs := make([]string, 0, len(batch))
		displayNames := make([]string, 0, len(batch))
		statuses := make([]string, 0, len(batch))
		createdAtSources := make([]pgtype.Timestamptz, 0, len(batch))
		updatedAtSources := make([]pgtype.Timestamptz, 0, len(batch))
		rawJSONs := make([][]byte, 0, len(batch))
		for _, row := range batch {
			assetKinds = append(assetKinds, row.AssetKind)
			externalIDs = append(externalIDs, row.ExternalID)
			parentExternalIDs = append(parentExternalIDs, row.ParentExternalID)
			displayNames = append(displayNames, row.DisplayName)
			statuses = append(statuses, row.Status)
			createdAtSources = append(createdAtSources, row.CreatedAtSource)
			updatedAtSources = append(updatedAtSources, row.UpdatedAtSource)
			rawJSONs = append(rawJSONs, row.RawJSON)
		}

		if _, err := q.UpsertAppAssetsBulkBySource(ctx, gen.UpsertAppAssetsBulkBySourceParams{
			SourceKind:        configstore.KindGoogleWorkspace,
			SourceName:        i.customerID,
			SeenInRunID:       runID,
			AssetKinds:        assetKinds,
			ExternalIds:       externalIDs,
			ParentExternalIds: parentExternalIDs,
			DisplayNames:      displayNames,
			Statuses:          statuses,
			CreatedAtSources:  createdAtSources,
			UpdatedAtSources:  updatedAtSources,
			RawJsons:          rawJSONs,
		}); err != nil {
			return fmt.Errorf("upsert google app assets: %w", err)
		}

		report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-app-assets", Current: int64(end), Total: int64(len(rows)), Message: fmt.Sprintf("app assets %d/%d", end, len(rows))})
	}
	return nil
}

func (i *GoogleWorkspaceIntegration) upsertAppAssetOwners(ctx context.Context, q *gen.Queries, report func(registry.Event), runID int64, rows []googleWorkspaceAppAssetOwnerRow) error {
	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-owners", Current: 0, Total: int64(len(rows)), Message: fmt.Sprintf("writing %d owners", len(rows))})
	if len(rows) == 0 {
		return nil
	}
	for start := 0; start < len(rows); start += googleWorkspaceOwnerBatchSize {
		end := min(start+googleWorkspaceOwnerBatchSize, len(rows))
		batch := rows[start:end]

		assetKinds := make([]string, 0, len(batch))
		assetExternalIDs := make([]string, 0, len(batch))
		ownerKinds := make([]string, 0, len(batch))
		ownerExternalIDs := make([]string, 0, len(batch))
		ownerDisplayNames := make([]string, 0, len(batch))
		ownerEmails := make([]string, 0, len(batch))
		rawJSONs := make([][]byte, 0, len(batch))
		for _, row := range batch {
			assetKinds = append(assetKinds, row.AssetKind)
			assetExternalIDs = append(assetExternalIDs, row.AssetExternalID)
			ownerKinds = append(ownerKinds, row.OwnerKind)
			ownerExternalIDs = append(ownerExternalIDs, row.OwnerExternalID)
			ownerDisplayNames = append(ownerDisplayNames, row.OwnerDisplayName)
			ownerEmails = append(ownerEmails, row.OwnerEmail)
			rawJSONs = append(rawJSONs, row.RawJSON)
		}

		if _, err := q.UpsertAppAssetOwnersBulkBySource(ctx, gen.UpsertAppAssetOwnersBulkBySourceParams{
			SourceKind:        configstore.KindGoogleWorkspace,
			SourceName:        i.customerID,
			SeenInRunID:       runID,
			AssetKinds:        assetKinds,
			AssetExternalIds:  assetExternalIDs,
			OwnerKinds:        ownerKinds,
			OwnerExternalIds:  ownerExternalIDs,
			OwnerDisplayNames: ownerDisplayNames,
			OwnerEmails:       ownerEmails,
			RawJsons:          rawJSONs,
		}); err != nil {
			return fmt.Errorf("upsert google app owners: %w", err)
		}

		report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-owners", Current: int64(end), Total: int64(len(rows)), Message: fmt.Sprintf("owners %d/%d", end, len(rows))})
	}
	return nil
}

func (i *GoogleWorkspaceIntegration) upsertCredentialArtifacts(ctx context.Context, q *gen.Queries, report func(registry.Event), runID int64, rows []googleWorkspaceCredentialArtifactRow) error {
	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-credentials", Current: 0, Total: int64(len(rows)), Message: fmt.Sprintf("writing %d credentials", len(rows))})
	if len(rows) == 0 {
		return nil
	}
	for start := 0; start < len(rows); start += googleWorkspaceCredentialBatchSize {
		end := min(start+googleWorkspaceCredentialBatchSize, len(rows))
		batch := rows[start:end]

		assetRefKinds := make([]string, 0, len(batch))
		assetRefExternalIDs := make([]string, 0, len(batch))
		credentialKinds := make([]string, 0, len(batch))
		externalIDs := make([]string, 0, len(batch))
		displayNames := make([]string, 0, len(batch))
		fingerprints := make([]string, 0, len(batch))
		scopeJSONs := make([][]byte, 0, len(batch))
		statuses := make([]string, 0, len(batch))
		createdAtSources := make([]pgtype.Timestamptz, 0, len(batch))
		expiresAtSources := make([]pgtype.Timestamptz, 0, len(batch))
		lastUsedAtSources := make([]pgtype.Timestamptz, 0, len(batch))
		createdByKinds := make([]string, 0, len(batch))
		createdByExternalIDs := make([]string, 0, len(batch))
		createdByDisplayNames := make([]string, 0, len(batch))
		approvedByKinds := make([]string, 0, len(batch))
		approvedByExternalIDs := make([]string, 0, len(batch))
		approvedByDisplayNames := make([]string, 0, len(batch))
		rawJSONs := make([][]byte, 0, len(batch))
		for _, row := range batch {
			assetRefKinds = append(assetRefKinds, row.AssetRefKind)
			assetRefExternalIDs = append(assetRefExternalIDs, row.AssetRefExternalID)
			credentialKinds = append(credentialKinds, row.CredentialKind)
			externalIDs = append(externalIDs, row.ExternalID)
			displayNames = append(displayNames, row.DisplayName)
			fingerprints = append(fingerprints, row.Fingerprint)
			scopeJSONs = append(scopeJSONs, row.ScopeJSON)
			statuses = append(statuses, row.Status)
			createdAtSources = append(createdAtSources, row.CreatedAtSource)
			expiresAtSources = append(expiresAtSources, row.ExpiresAtSource)
			lastUsedAtSources = append(lastUsedAtSources, row.LastUsedAtSource)
			createdByKinds = append(createdByKinds, row.CreatedByKind)
			createdByExternalIDs = append(createdByExternalIDs, row.CreatedByExternalID)
			createdByDisplayNames = append(createdByDisplayNames, row.CreatedByDisplayName)
			approvedByKinds = append(approvedByKinds, row.ApprovedByKind)
			approvedByExternalIDs = append(approvedByExternalIDs, row.ApprovedByExternalID)
			approvedByDisplayNames = append(approvedByDisplayNames, row.ApprovedByDisplayName)
			rawJSONs = append(rawJSONs, row.RawJSON)
		}

		if _, err := q.UpsertCredentialArtifactsBulkBySource(ctx, gen.UpsertCredentialArtifactsBulkBySourceParams{
			SourceKind:             configstore.KindGoogleWorkspace,
			SourceName:             i.customerID,
			SeenInRunID:            runID,
			AssetRefKinds:          assetRefKinds,
			AssetRefExternalIds:    assetRefExternalIDs,
			CredentialKinds:        credentialKinds,
			ExternalIds:            externalIDs,
			DisplayNames:           displayNames,
			Fingerprints:           fingerprints,
			ScopeJsons:             scopeJSONs,
			Statuses:               statuses,
			CreatedAtSources:       createdAtSources,
			ExpiresAtSources:       expiresAtSources,
			LastUsedAtSources:      lastUsedAtSources,
			CreatedByKinds:         createdByKinds,
			CreatedByExternalIds:   createdByExternalIDs,
			CreatedByDisplayNames:  createdByDisplayNames,
			ApprovedByKinds:        approvedByKinds,
			ApprovedByExternalIds:  approvedByExternalIDs,
			ApprovedByDisplayNames: approvedByDisplayNames,
			RawJsons:               rawJSONs,
		}); err != nil {
			return fmt.Errorf("upsert google credentials: %w", err)
		}

		report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-credentials", Current: int64(end), Total: int64(len(rows)), Message: fmt.Sprintf("credentials %d/%d", end, len(rows))})
	}
	return nil
}

func buildGoogleWorkspaceAuditEventRows(activities []WorkspaceActivity) []googleWorkspaceCredentialAuditEventRow {
	now := time.Now().UTC()
	rows := make([]googleWorkspaceCredentialAuditEventRow, 0, len(activities))
	for _, activity := range activities {
		clientID, clientName, _ := discoverySourceFromActivity(activity)
		if clientID == "" {
			continue
		}
		observedAt := parseGoogleTime(activity.ID.Time)
		if observedAt.IsZero() {
			observedAt = now
		}
		actorExternalID := strings.TrimSpace(activity.Actor.ProfileID)
		actorEmail := normalizeEmail(strings.TrimSpace(activity.Actor.Email))
		if actorExternalID == "" {
			actorExternalID = actorEmail
		}

		if len(activity.Events) == 0 {
			rows = append(rows, googleWorkspaceCredentialAuditEventRow{
				EventExternalID:      activityEventExternalID("token", activity, 0),
				EventType:            "token.activity",
				EventTime:            registry.PgTimestamptzPtr(&observedAt),
				ActorKind:            "google_user",
				ActorExternalID:      actorExternalID,
				ActorDisplayName:     actorEmail,
				TargetKind:           "google_oauth_client",
				TargetExternalID:     clientID,
				TargetDisplayName:    clientName,
				CredentialKind:       "google_oauth_grant",
				CredentialExternalID: googleWorkspaceGrantExternalID(clientID, actorExternalID),
				RawJSON:              registry.NormalizeJSON(activity.RawJSON),
			})
			continue
		}

		for idx, event := range activity.Events {
			eventType := strings.TrimSpace(event.Name)
			if eventType == "" {
				eventType = strings.TrimSpace(event.Type)
			}
			if eventType == "" {
				eventType = "token.activity"
			}
			rows = append(rows, googleWorkspaceCredentialAuditEventRow{
				EventExternalID:      activityEventExternalID("token", activity, idx),
				EventType:            eventType,
				EventTime:            registry.PgTimestamptzPtr(&observedAt),
				ActorKind:            "google_user",
				ActorExternalID:      actorExternalID,
				ActorDisplayName:     actorEmail,
				TargetKind:           "google_oauth_client",
				TargetExternalID:     clientID,
				TargetDisplayName:    clientName,
				CredentialKind:       "google_oauth_grant",
				CredentialExternalID: googleWorkspaceGrantExternalID(clientID, actorExternalID),
				RawJSON:              registry.NormalizeJSON(activity.RawJSON),
			})
		}
	}
	return rows
}

func (i *GoogleWorkspaceIntegration) upsertCredentialAuditEvents(ctx context.Context, q *gen.Queries, report func(registry.Event), rows []googleWorkspaceCredentialAuditEventRow) error {
	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-audit-events", Current: 0, Total: int64(len(rows)), Message: fmt.Sprintf("writing %d audit events", len(rows))})
	if len(rows) == 0 {
		return nil
	}
	for start := 0; start < len(rows); start += googleWorkspaceAuditEventBatchSize {
		end := min(start+googleWorkspaceAuditEventBatchSize, len(rows))
		batch := rows[start:end]

		eventExternalIDs := make([]string, 0, len(batch))
		eventTypes := make([]string, 0, len(batch))
		eventTimes := make([]pgtype.Timestamptz, 0, len(batch))
		actorKinds := make([]string, 0, len(batch))
		actorExternalIDs := make([]string, 0, len(batch))
		actorDisplayNames := make([]string, 0, len(batch))
		targetKinds := make([]string, 0, len(batch))
		targetExternalIDs := make([]string, 0, len(batch))
		targetDisplayNames := make([]string, 0, len(batch))
		credentialKinds := make([]string, 0, len(batch))
		credentialExternalIDs := make([]string, 0, len(batch))
		rawJSONs := make([][]byte, 0, len(batch))
		for _, row := range batch {
			eventExternalIDs = append(eventExternalIDs, row.EventExternalID)
			eventTypes = append(eventTypes, row.EventType)
			eventTimes = append(eventTimes, row.EventTime)
			actorKinds = append(actorKinds, row.ActorKind)
			actorExternalIDs = append(actorExternalIDs, row.ActorExternalID)
			actorDisplayNames = append(actorDisplayNames, row.ActorDisplayName)
			targetKinds = append(targetKinds, row.TargetKind)
			targetExternalIDs = append(targetExternalIDs, row.TargetExternalID)
			targetDisplayNames = append(targetDisplayNames, row.TargetDisplayName)
			credentialKinds = append(credentialKinds, row.CredentialKind)
			credentialExternalIDs = append(credentialExternalIDs, row.CredentialExternalID)
			rawJSONs = append(rawJSONs, row.RawJSON)
		}

		if _, err := q.UpsertCredentialAuditEventsBulkBySource(ctx, gen.UpsertCredentialAuditEventsBulkBySourceParams{
			SourceKind:            configstore.KindGoogleWorkspace,
			SourceName:            i.customerID,
			EventExternalIds:      eventExternalIDs,
			EventTypes:            eventTypes,
			EventTimes:            eventTimes,
			ActorKinds:            actorKinds,
			ActorExternalIds:      actorExternalIDs,
			ActorDisplayNames:     actorDisplayNames,
			TargetKinds:           targetKinds,
			TargetExternalIds:     targetExternalIDs,
			TargetDisplayNames:    targetDisplayNames,
			CredentialKinds:       credentialKinds,
			CredentialExternalIds: credentialExternalIDs,
			RawJsons:              rawJSONs,
		}); err != nil {
			return fmt.Errorf("upsert google audit events: %w", err)
		}

		report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-audit-events", Current: int64(end), Total: int64(len(rows)), Message: fmt.Sprintf("audit events %d/%d", end, len(rows))})
	}
	return nil
}

func (i *GoogleWorkspaceIntegration) syncDiscovery(ctx context.Context, q *gen.Queries, report func(registry.Event), runID int64) error {
	now := time.Now().UTC()
	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "list-discovery-events", Current: 0, Total: 1, Message: "listing login and token activities"})

	since := now.Add(-7 * 24 * time.Hour)
	latestObservedAt, err := q.GetLatestSaaSDiscoveryObservedAtBySource(ctx, gen.GetLatestSaaSDiscoveryObservedAtBySourceParams{
		SourceKind: configstore.KindGoogleWorkspace,
		SourceName: i.customerID,
	})
	if err != nil {
		metrics.DiscoveryIngestFailuresTotal.WithLabelValues(configstore.KindGoogleWorkspace, discovery.SignalKindIDPSSO, "watermark_query_error").Inc()
		return fmt.Errorf("query latest discovery watermark: %w", err)
	}
	if latestObservedAt.Valid {
		candidate := latestObservedAt.Time.UTC().Add(-googleWorkspaceDiscoveryWatermarkSkew)
		if candidate.After(since) {
			since = candidate
		}
	}

	loginActivities, err := i.client.ListLoginActivities(ctx, &since)
	if err != nil {
		metrics.DiscoveryIngestFailuresTotal.WithLabelValues(configstore.KindGoogleWorkspace, discovery.SignalKindIDPSSO, "api_error").Inc()
		return fmt.Errorf("list google login activities: %w", err)
	}
	tokenActivities, err := i.client.ListTokenActivities(ctx, &since)
	if err != nil {
		metrics.DiscoveryIngestFailuresTotal.WithLabelValues(configstore.KindGoogleWorkspace, discovery.SignalKindOAuth, "api_error").Inc()
		return fmt.Errorf("list google token activities: %w", err)
	}
	tokenGrants, err := i.client.ListOAuthTokenGrants(ctx)
	if err != nil {
		metrics.DiscoveryIngestFailuresTotal.WithLabelValues(configstore.KindGoogleWorkspace, discovery.SignalKindOAuth, "api_error").Inc()
		return fmt.Errorf("list google oauth token grants: %w", err)
	}
	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "list-discovery-events", Current: 1, Total: 1, Message: fmt.Sprintf("found %d login events, %d token activities, and %d grants", len(loginActivities), len(tokenActivities), len(tokenGrants))})

	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "normalize-discovery", Current: 0, Total: 1, Message: "normalizing discovery evidence"})
	sources, events := i.normalizeDiscovery(loginActivities, tokenActivities, tokenGrants, now)
	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "normalize-discovery", Current: 1, Total: 1, Message: fmt.Sprintf("normalized %d source rows and %d events", len(sources), len(events))})

	if err := i.writeDiscoveryRows(ctx, q, report, runID, sources, events); err != nil {
		metrics.DiscoveryIngestFailuresTotal.WithLabelValues(configstore.KindGoogleWorkspace, discovery.SignalKindIDPSSO, "db_error").Inc()
		return err
	}
	if err := i.seedGoogleWorkspaceAutoBindings(ctx, q, runID); err != nil {
		return err
	}
	return nil
}

func (i *GoogleWorkspaceIntegration) normalizeDiscovery(loginActivities, tokenActivities []WorkspaceActivity, tokenGrants []WorkspaceOAuthTokenGrant, now time.Time) ([]normalizedDiscoverySource, []normalizedDiscoveryEvent) {
	sourceByID := map[string]normalizedDiscoverySource{}
	events := make([]normalizedDiscoveryEvent, 0, len(loginActivities)+len(tokenActivities)+len(tokenGrants))

	upsertSource := func(signalKind, sourceAppID, sourceAppName, sourceDomain, sourceVendor string, seenAt time.Time) (discovery.AppMetadata, bool) {
		sourceAppID = strings.TrimSpace(sourceAppID)
		if sourceAppID == "" {
			return discovery.AppMetadata{}, false
		}
		sourceAppName = strings.TrimSpace(sourceAppName)
		if sourceAppName == "" {
			sourceAppName = sourceAppID
		}
		if seenAt.IsZero() {
			seenAt = now
		}
		metadata := discovery.BuildMetadata(discovery.CanonicalInput{
			SourceKind:       configstore.KindGoogleWorkspace,
			SourceName:       i.customerID,
			SourceAppID:      sourceAppID,
			SourceAppName:    sourceAppName,
			SourceDomain:     sourceDomain,
			SourceVendorName: sourceVendor,
		})
		current := sourceByID[sourceAppID]
		if current.SourceAppID == "" || seenAt.After(current.SeenAt) {
			sourceByID[sourceAppID] = normalizedDiscoverySource{
				CanonicalKey:     metadata.CanonicalKey,
				SourceAppID:      sourceAppID,
				SourceAppName:    sourceAppName,
				SourceAppDomain:  metadata.Domain,
				SourceVendorName: metadata.VendorName,
				SeenAt:           seenAt,
			}
		}
		_ = signalKind
		return metadata, true
	}

	for _, activity := range loginActivities {
		sourceAppID, sourceAppName, sourceDomain := discoverySourceFromActivity(activity)
		observedAt := parseGoogleTime(activity.ID.Time)
		if observedAt.IsZero() {
			observedAt = now
		}
		metadata, ok := upsertSource(discovery.SignalKindIDPSSO, sourceAppID, sourceAppName, sourceDomain, sourceAppName, observedAt)
		if !ok {
			continue
		}
		actorExternalID := strings.TrimSpace(activity.Actor.ProfileID)
		actorEmail := normalizeEmail(strings.TrimSpace(activity.Actor.Email))
		if actorExternalID == "" {
			actorExternalID = actorEmail
		}
		if len(activity.Events) == 0 {
			events = append(events, normalizedDiscoveryEvent{
				CanonicalKey:     metadata.CanonicalKey,
				SignalKind:       discovery.SignalKindIDPSSO,
				EventExternalID:  activityEventExternalID("login", activity, 0),
				SourceAppID:      sourceAppID,
				SourceAppName:    sourceAppName,
				SourceAppDomain:  metadata.Domain,
				SourceVendorName: metadata.VendorName,
				ActorExternalID:  actorExternalID,
				ActorEmail:       actorEmail,
				ActorDisplayName: actorEmail,
				ObservedAt:       observedAt,
				Scopes:           nil,
				RawJSON:          registry.NormalizeJSON(activity.RawJSON),
			})
			continue
		}
		for idx := range activity.Events {
			events = append(events, normalizedDiscoveryEvent{
				CanonicalKey:     metadata.CanonicalKey,
				SignalKind:       discovery.SignalKindIDPSSO,
				EventExternalID:  activityEventExternalID("login", activity, idx),
				SourceAppID:      sourceAppID,
				SourceAppName:    sourceAppName,
				SourceAppDomain:  metadata.Domain,
				SourceVendorName: metadata.VendorName,
				ActorExternalID:  actorExternalID,
				ActorEmail:       actorEmail,
				ActorDisplayName: actorEmail,
				ObservedAt:       observedAt,
				Scopes:           nil,
				RawJSON:          registry.NormalizeJSON(activity.RawJSON),
			})
		}
	}

	for _, activity := range tokenActivities {
		sourceAppID, sourceAppName, sourceDomain := discoverySourceFromActivity(activity)
		observedAt := parseGoogleTime(activity.ID.Time)
		if observedAt.IsZero() {
			observedAt = now
		}
		metadata, ok := upsertSource(discovery.SignalKindOAuth, sourceAppID, sourceAppName, sourceDomain, sourceAppName, observedAt)
		if !ok {
			continue
		}
		actorExternalID := strings.TrimSpace(activity.Actor.ProfileID)
		actorEmail := normalizeEmail(strings.TrimSpace(activity.Actor.Email))
		if actorExternalID == "" {
			actorExternalID = actorEmail
		}
		scopes := discovery.NormalizeScopes(append(activity.ParameterValues("scope"), activity.ParameterValues("scopes")...))
		if len(activity.Events) == 0 {
			events = append(events, normalizedDiscoveryEvent{
				CanonicalKey:     metadata.CanonicalKey,
				SignalKind:       discovery.SignalKindOAuth,
				EventExternalID:  activityEventExternalID("token", activity, 0),
				SourceAppID:      sourceAppID,
				SourceAppName:    sourceAppName,
				SourceAppDomain:  metadata.Domain,
				SourceVendorName: metadata.VendorName,
				ActorExternalID:  actorExternalID,
				ActorEmail:       actorEmail,
				ActorDisplayName: actorEmail,
				ObservedAt:       observedAt,
				Scopes:           scopes,
				RawJSON:          registry.NormalizeJSON(activity.RawJSON),
			})
			continue
		}
		for idx := range activity.Events {
			events = append(events, normalizedDiscoveryEvent{
				CanonicalKey:     metadata.CanonicalKey,
				SignalKind:       discovery.SignalKindOAuth,
				EventExternalID:  activityEventExternalID("token", activity, idx),
				SourceAppID:      sourceAppID,
				SourceAppName:    sourceAppName,
				SourceAppDomain:  metadata.Domain,
				SourceVendorName: metadata.VendorName,
				ActorExternalID:  actorExternalID,
				ActorEmail:       actorEmail,
				ActorDisplayName: actorEmail,
				ObservedAt:       observedAt,
				Scopes:           scopes,
				RawJSON:          registry.NormalizeJSON(activity.RawJSON),
			})
		}
	}

	for _, grant := range tokenGrants {
		sourceAppID := googleWorkspaceClientExternalID(grant)
		sourceAppName := strings.TrimSpace(grant.DisplayText)
		metadata, ok := upsertSource(discovery.SignalKindOAuth, sourceAppID, sourceAppName, "", sourceAppName, now)
		if !ok {
			continue
		}
		userKey := strings.TrimSpace(grant.UserKey)
		actorEmail := normalizeEmail(userKey)
		events = append(events, normalizedDiscoveryEvent{
			CanonicalKey:     metadata.CanonicalKey,
			SignalKind:       discovery.SignalKindOAuth,
			EventExternalID:  "inventory:" + googleWorkspaceGrantExternalID(sourceAppID, userKey),
			SourceAppID:      sourceAppID,
			SourceAppName:    sourceAppName,
			SourceAppDomain:  metadata.Domain,
			SourceVendorName: metadata.VendorName,
			ActorExternalID:  userKey,
			ActorEmail:       actorEmail,
			ActorDisplayName: actorEmail,
			ObservedAt:       now,
			Scopes:           discovery.NormalizeScopes(grant.Scopes),
			RawJSON:          registry.NormalizeJSON(grant.RawJSON),
		})
	}

	sources := make([]normalizedDiscoverySource, 0, len(sourceByID))
	for _, row := range sourceByID {
		sources = append(sources, row)
	}
	return sources, events
}

func discoverySourceFromActivity(activity WorkspaceActivity) (string, string, string) {
	sourceAppID := firstNonEmpty(
		activity.ParameterValues("client_id"),
		activity.ParameterValues("clientId"),
		activity.ParameterValues("oauth_client_id"),
		activity.ParameterValues("app_id"),
		activity.ParameterValues("application_id"),
		activity.ParameterValues("applicationName"),
	)
	sourceAppName := firstNonEmpty(
		activity.ParameterValues("display_name"),
		activity.ParameterValues("application_name"),
		activity.ParameterValues("app_name"),
		activity.ParameterValues("client_name"),
		activity.ParameterValues("target_app_name"),
	)
	sourceDomain := firstNonEmpty(
		activity.ParameterValues("app_domain"),
		activity.ParameterValues("domain"),
		activity.ParameterValues("host"),
		activity.ParameterValues("url"),
	)
	if sourceAppID == "" {
		sourceAppID = sourceAppName
	}
	if sourceAppName == "" {
		sourceAppName = sourceAppID
	}
	return strings.TrimSpace(sourceAppID), strings.TrimSpace(sourceAppName), strings.TrimSpace(sourceDomain)
}

func firstNonEmpty(candidates ...[]string) string {
	for _, values := range candidates {
		for _, value := range values {
			value = strings.TrimSpace(value)
			if value != "" {
				return value
			}
		}
	}
	return ""
}

func activityEventExternalID(prefix string, activity WorkspaceActivity, idx int) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = "event"
	}
	unique := strings.TrimSpace(activity.ID.UniqueQualifier)
	if unique != "" {
		return fmt.Sprintf("%s:%s:%d", prefix, unique, idx)
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(strings.TrimSpace(activity.ID.Time)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strings.TrimSpace(activity.Actor.ProfileID)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strings.TrimSpace(activity.Actor.Email)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strings.TrimSpace(activity.EventName())))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(fmt.Sprintf("%d", idx)))
	return fmt.Sprintf("%s:%x", prefix, h.Sum64())
}

func (i *GoogleWorkspaceIntegration) writeDiscoveryRows(ctx context.Context, q *gen.Queries, report func(registry.Event), runID int64, sources []normalizedDiscoverySource, events []normalizedDiscoveryEvent) error {
	total := len(sources) + len(events)
	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-discovery", Current: 0, Total: int64(total), Message: fmt.Sprintf("writing %d discovery records", total)})

	appMeta := map[string]discovery.AppMetadata{}
	firstSeenByKey := map[string]time.Time{}
	lastSeenByKey := map[string]time.Time{}
	addMeta := func(key string, seenAt time.Time, sample discovery.AppMetadata) {
		if key == "" {
			return
		}
		if _, ok := appMeta[key]; !ok {
			appMeta[key] = sample
			firstSeenByKey[key] = seenAt
			lastSeenByKey[key] = seenAt
			return
		}
		if seenAt.Before(firstSeenByKey[key]) {
			firstSeenByKey[key] = seenAt
		}
		if seenAt.After(lastSeenByKey[key]) {
			lastSeenByKey[key] = seenAt
		}
	}

	for _, source := range sources {
		meta := discovery.BuildMetadata(discovery.CanonicalInput{
			SourceKind:       configstore.KindGoogleWorkspace,
			SourceName:       i.customerID,
			SourceAppID:      source.SourceAppID,
			SourceAppName:    source.SourceAppName,
			SourceDomain:     source.SourceAppDomain,
			SourceVendorName: source.SourceVendorName,
		})
		meta.CanonicalKey = source.CanonicalKey
		addMeta(source.CanonicalKey, source.SeenAt, meta)
	}
	for _, event := range events {
		meta := discovery.BuildMetadata(discovery.CanonicalInput{
			SourceKind:       configstore.KindGoogleWorkspace,
			SourceName:       i.customerID,
			SourceAppID:      event.SourceAppID,
			SourceAppName:    event.SourceAppName,
			SourceDomain:     event.SourceAppDomain,
			SourceVendorName: event.SourceVendorName,
		})
		meta.CanonicalKey = event.CanonicalKey
		addMeta(event.CanonicalKey, event.ObservedAt, meta)
	}

	if len(appMeta) > 0 {
		canonicalKeys := make([]string, 0, len(appMeta))
		displayNames := make([]string, 0, len(appMeta))
		primaryDomains := make([]string, 0, len(appMeta))
		vendorNames := make([]string, 0, len(appMeta))
		firstSeenAts := make([]pgtype.Timestamptz, 0, len(appMeta))
		lastSeenAts := make([]pgtype.Timestamptz, 0, len(appMeta))
		for key, meta := range appMeta {
			canonicalKeys = append(canonicalKeys, key)
			displayNames = append(displayNames, meta.DisplayName)
			primaryDomains = append(primaryDomains, meta.Domain)
			vendorNames = append(vendorNames, meta.VendorName)
			firstSeenAt := firstSeenByKey[key]
			lastSeenAt := lastSeenByKey[key]
			firstSeenAts = append(firstSeenAts, registry.PgTimestamptzPtr(&firstSeenAt))
			lastSeenAts = append(lastSeenAts, registry.PgTimestamptzPtr(&lastSeenAt))
		}
		if _, err := q.UpsertSaaSAppsBulk(ctx, gen.UpsertSaaSAppsBulkParams{
			CanonicalKeys:  canonicalKeys,
			DisplayNames:   displayNames,
			PrimaryDomains: primaryDomains,
			VendorNames:    vendorNames,
			FirstSeenAts:   firstSeenAts,
			LastSeenAts:    lastSeenAts,
		}); err != nil {
			return fmt.Errorf("upsert saas apps: %w", err)
		}
	}

	written := 0
	if len(sources) > 0 {
		canonicalKeys := make([]string, 0, len(sources))
		sourceAppIDs := make([]string, 0, len(sources))
		sourceAppNames := make([]string, 0, len(sources))
		sourceAppDomains := make([]string, 0, len(sources))
		seenAts := make([]pgtype.Timestamptz, 0, len(sources))
		for _, source := range sources {
			canonicalKeys = append(canonicalKeys, source.CanonicalKey)
			sourceAppIDs = append(sourceAppIDs, source.SourceAppID)
			sourceAppNames = append(sourceAppNames, source.SourceAppName)
			sourceAppDomains = append(sourceAppDomains, source.SourceAppDomain)
			seenAts = append(seenAts, registry.PgTimestamptzPtr(&source.SeenAt))
		}
		if _, err := q.UpsertSaaSAppSourcesBulkBySource(ctx, gen.UpsertSaaSAppSourcesBulkBySourceParams{
			SourceKind:       configstore.KindGoogleWorkspace,
			SourceName:       i.customerID,
			SeenInRunID:      runID,
			CanonicalKeys:    canonicalKeys,
			SourceAppIds:     sourceAppIDs,
			SourceAppNames:   sourceAppNames,
			SourceAppDomains: sourceAppDomains,
			SeenAts:          seenAts,
		}); err != nil {
			return fmt.Errorf("upsert saas app sources: %w", err)
		}
		written += len(sources)
		report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-discovery", Current: int64(written), Total: int64(total), Message: fmt.Sprintf("sources %d/%d", written, total)})
	}

	if len(events) > 0 {
		canonicalKeys := make([]string, 0, len(events))
		signalKinds := make([]string, 0, len(events))
		eventExternalIDs := make([]string, 0, len(events))
		sourceAppIDs := make([]string, 0, len(events))
		sourceAppNames := make([]string, 0, len(events))
		sourceAppDomains := make([]string, 0, len(events))
		actorExternalIDs := make([]string, 0, len(events))
		actorEmails := make([]string, 0, len(events))
		actorDisplayNames := make([]string, 0, len(events))
		observedAts := make([]pgtype.Timestamptz, 0, len(events))
		scopesJSONs := make([][]byte, 0, len(events))
		rawJSONs := make([][]byte, 0, len(events))
		ingestedBySignal := map[string]int{}
		for _, event := range events {
			canonicalKeys = append(canonicalKeys, event.CanonicalKey)
			signalKinds = append(signalKinds, event.SignalKind)
			eventExternalIDs = append(eventExternalIDs, event.EventExternalID)
			sourceAppIDs = append(sourceAppIDs, event.SourceAppID)
			sourceAppNames = append(sourceAppNames, event.SourceAppName)
			sourceAppDomains = append(sourceAppDomains, event.SourceAppDomain)
			actorExternalIDs = append(actorExternalIDs, event.ActorExternalID)
			actorEmails = append(actorEmails, event.ActorEmail)
			actorDisplayNames = append(actorDisplayNames, event.ActorDisplayName)
			observedAts = append(observedAts, registry.PgTimestamptzPtr(&event.ObservedAt))
			scopesJSONs = append(scopesJSONs, discovery.ScopesJSON(event.Scopes))
			rawJSONs = append(rawJSONs, registry.NormalizeJSON(event.RawJSON))
			ingestedBySignal[event.SignalKind]++
		}
		if _, err := q.UpsertSaaSAppEventsBulkBySource(ctx, gen.UpsertSaaSAppEventsBulkBySourceParams{
			SourceKind:        configstore.KindGoogleWorkspace,
			SourceName:        i.customerID,
			SeenInRunID:       runID,
			CanonicalKeys:     canonicalKeys,
			SignalKinds:       signalKinds,
			EventExternalIds:  eventExternalIDs,
			SourceAppIds:      sourceAppIDs,
			SourceAppNames:    sourceAppNames,
			SourceAppDomains:  sourceAppDomains,
			ActorExternalIds:  actorExternalIDs,
			ActorEmails:       actorEmails,
			ActorDisplayNames: actorDisplayNames,
			ObservedAts:       observedAts,
			ScopesJsons:       scopesJSONs,
			RawJsons:          rawJSONs,
		}); err != nil {
			return fmt.Errorf("upsert saas app events: %w", err)
		}
		for signalKind, count := range ingestedBySignal {
			metrics.DiscoveryEventsIngestedTotal.WithLabelValues(configstore.KindGoogleWorkspace, signalKind).Add(float64(count))
		}
		written += len(events)
		report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-discovery", Current: int64(written), Total: int64(total), Message: fmt.Sprintf("events %d/%d", written, total)})
	}

	if written == 0 {
		report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-discovery", Current: 0, Total: 0, Message: "no discovery records to write"})
	}
	return nil
}

func (i *GoogleWorkspaceIntegration) seedGoogleWorkspaceAutoBindings(ctx context.Context, q *gen.Queries, runID int64) error {
	appIDs, err := q.ListSaaSAppIDsFromSourcesSeenInRunBySource(ctx, gen.ListSaaSAppIDsFromSourcesSeenInRunBySourceParams{
		SourceKind:  configstore.KindGoogleWorkspace,
		SourceName:  i.customerID,
		SeenInRunID: runID,
	})
	if err != nil {
		return fmt.Errorf("list google discovery auto-bind candidates: %w", err)
	}
	if len(appIDs) == 0 {
		return nil
	}

	boundCount := 0
	for _, appID := range appIDs {
		sources, err := q.ListSaaSAppSourcesBySaaSAppID(ctx, appID)
		if err != nil {
			return fmt.Errorf("list source rows for saas app %d: %w", appID, err)
		}
		shouldBind := false
		for _, source := range sources {
			if strings.TrimSpace(source.SourceKind) != configstore.KindGoogleWorkspace || strings.TrimSpace(source.SourceName) != i.customerID {
				continue
			}
			sourceAppID := strings.TrimSpace(source.SourceAppID)
			if sourceAppID == "" {
				continue
			}
			_, err := q.GetAppAssetBySourceAndKindAndExternalID(ctx, gen.GetAppAssetBySourceAndKindAndExternalIDParams{
				SourceKind: configstore.KindGoogleWorkspace,
				SourceName: i.customerID,
				AssetKind:  "google_oauth_client",
				ExternalID: sourceAppID,
			})
			if err == nil {
				shouldBind = true
				break
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("lookup google managed app asset for saas app %d: %w", appID, err)
			}
		}
		if !shouldBind {
			continue
		}

		if err := q.UpsertSaaSAppBinding(ctx, gen.UpsertSaaSAppBindingParams{
			SaasAppID:           appID,
			ConnectorKind:       configstore.KindGoogleWorkspace,
			ConnectorSourceName: i.customerID,
			BindingSource:       "auto",
			Confidence:          0.8,
			IsPrimary:           false,
			CreatedByAuthUserID: pgtype.Int8{},
		}); err != nil {
			return fmt.Errorf("upsert google auto binding for app %d: %w", appID, err)
		}
		boundCount++
	}

	if boundCount > 0 {
		if _, err := q.RecomputePrimarySaaSAppBindingsForAll(ctx); err != nil {
			return fmt.Errorf("recompute primary bindings: %w", err)
		}
	}
	return nil
}

func normalizeEmail(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

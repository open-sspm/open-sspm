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
	"github.com/open-sspm/open-sspm/internal/connectors/capabilities"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/discovery"
	"github.com/open-sspm/open-sspm/internal/ingest/recorddispatch"
	"github.com/open-sspm/open-sspm/internal/metrics"
	"github.com/open-sspm/open-sspm/internal/records"
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

	// reportsActivityLister is a test hook for the Reports API tail. Production
	// runs use client.ListLoginActivities and client.ListTokenActivities.
	reportsActivityLister func(context.Context, time.Time) ([]WorkspaceActivity, error)
}

type googleWorkspaceAccountRow struct {
	ExternalID     string
	Email          string
	DisplayName    string
	AccountKind    string
	EntityCategory string
	Status         string
	RawJSON        []byte
}

type googleWorkspaceEntitlementRow struct {
	AccountExternalID string
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
		return i.client != nil && i.discoveryEnabled
	case registry.RunModeTail:
		return i.client != nil || i.reportsActivityLister != nil
	default:
		return i.client != nil
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
	if i == nil {
		return fmt.Errorf("google workspace integration is not configured")
	}
	switch mode.Normalize() {
	case registry.RunModeDiscovery:
		if !i.SupportsRunMode(registry.RunModeDiscovery) {
			return nil
		}
		return i.runDiscovery(ctx, q, pool, report)
	case registry.RunModeTail:
		if !i.SupportsRunMode(registry.RunModeTail) {
			return nil
		}
		return i.runReportsTail(ctx, q, pool, report)
	default:
		if i.client == nil {
			return fmt.Errorf("google workspace API client is required for full sync")
		}
		return i.runFull(ctx, q, pool, report)
	}
}

func (i *GoogleWorkspaceIntegration) runFull(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, report func(registry.Event)) error {
	started := time.Now()
	runID, err := registry.StartSyncRun(ctx, q, configstore.KindGoogleWorkspace, i.customerID)
	if err != nil {
		return err
	}
	emitter := recorddispatch.NewDispatcher(nil, recorddispatch.NewSQLStateProjector(q, runID))

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
	if err := i.upsertAccounts(ctx, emitter, report, runID, accounts); err != nil {
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
	if err := i.upsertEntitlements(ctx, emitter, report, runID, allEntitlements); err != nil {
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
	if err := i.upsertAppAssets(ctx, emitter, report, runID, assets); err != nil {
		report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-app-assets", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}
	if err := i.upsertAppAssetOwners(ctx, emitter, report, runID, owners); err != nil {
		report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-owners", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}
	if err := i.upsertCredentialArtifacts(ctx, emitter, report, runID, credentials); err != nil {
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
	if err := i.upsertCredentialAuditEvents(ctx, emitter, report, auditRows); err != nil {
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
	runID, err := registry.StartSyncRunWithMode(ctx, q, configstore.KindGoogleWorkspace, i.customerID, registry.RunModeDiscovery)
	if err != nil {
		return err
	}

	emitter := recorddispatch.NewDispatcher(nil, recorddispatch.NewSQLStateProjector(q, runID))
	if err := i.syncDiscovery(ctx, q, emitter, report, runID); err != nil {
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
			ExternalID:     externalID,
			Email:          email,
			DisplayName:    displayName,
			AccountKind:    googleWorkspaceUserAccountKind(user),
			EntityCategory: registry.EntityCategoryUser,
			Status:         status,
			RawJSON:        raw,
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
			ExternalID:     externalID,
			Email:          email,
			DisplayName:    displayName,
			AccountKind:    registry.AccountKindUnknown,
			EntityCategory: registry.EntityCategoryGroup,
			Status:         "active",
			RawJSON:        raw,
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

func (i *GoogleWorkspaceIntegration) upsertAccounts(ctx context.Context, emitter capabilities.RecordEmitter, report func(registry.Event), runID int64, rows []googleWorkspaceAccountRow) error {
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

	for idx, row := range rows {
		resource := records.ResourceIdentity
		payload := records.ResourcePayload(records.IdentityPayload{
			ExternalID:  row.ExternalID,
			Email:       row.Email,
			DisplayName: row.DisplayName,
			Status:      row.Status,
			ProviderAttrs: map[string]any{
				"account_kind":    row.AccountKind,
				"entity_category": row.EntityCategory,
			},
			Raw: records.MapFromJSON(row.RawJSON),
		})
		if row.EntityCategory == registry.EntityCategoryGroup {
			resource = records.ResourceGroup
			payload = records.GroupPayload{
				ExternalID:  row.ExternalID,
				DisplayName: row.DisplayName,
				ProviderAttrs: map[string]any{
					"email":           row.Email,
					"account_kind":    row.AccountKind,
					"entity_category": row.EntityCategory,
				},
				Raw: records.MapFromJSON(row.RawJSON),
			}
		}
		if err := emitter.UpsertState(ctx, records.StateUpsert{
			Source:         records.SourceRef{Kind: configstore.KindGoogleWorkspace, Name: i.customerID},
			Resource:       resource,
			Key:            row.ExternalID,
			ProviderID:     row.ExternalID,
			ObservedAt:     time.Now().UTC(),
			DedupeKeyValue: fmt.Sprintf("%s:%s:account:%s", configstore.KindGoogleWorkspace, i.customerID, row.ExternalID),
			Payload:        payload,
		}); err != nil {
			return fmt.Errorf("emit google workspace account %s state: %w", row.ExternalID, err)
		}
		current := idx + 1
		if current%googleWorkspaceAccountBatchSize == 0 || current == len(rows) {
			report(registry.Event{
				Source:  configstore.KindGoogleWorkspace,
				Stage:   "write-users",
				Current: int64(current),
				Total:   int64(len(rows)),
				Message: fmt.Sprintf("accounts %d/%d", current, len(rows)),
			})
		}
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
				AccountExternalID: memberID,
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
			AccountExternalID: assignedTo,
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

func (i *GoogleWorkspaceIntegration) upsertEntitlements(ctx context.Context, emitter capabilities.RecordEmitter, report func(registry.Event), runID int64, rows []googleWorkspaceEntitlementRow) error {
	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-entitlements", Current: 0, Total: int64(len(rows)), Message: fmt.Sprintf("writing %d entitlements", len(rows))})
	if len(rows) == 0 {
		return nil
	}

	for idx, row := range rows {
		key := googleWorkspaceEntitlementKey(row.AccountExternalID, row.Kind, row.Resource, row.Permission)
		if err := emitter.UpsertState(ctx, records.StateUpsert{
			Source:         records.SourceRef{Kind: configstore.KindGoogleWorkspace, Name: i.customerID},
			Resource:       records.ResourceEntitlement,
			Key:            key,
			ProviderID:     key,
			DedupeKeyValue: fmt.Sprintf("%s:%s:entitlement:%s", configstore.KindGoogleWorkspace, i.customerID, key),
			Payload: records.EntitlementPayload{
				ExternalID: key,
				Kind:       row.Kind,
				Subject:    records.ResourceRef{Resource: records.ResourceIdentity, ExternalID: row.AccountExternalID},
				Target:     records.ResourceRef{Resource: records.ResourceGroup, ExternalID: row.Resource},
				Permission: row.Permission,
				Raw:        records.MapFromJSON(row.RawJSON),
			},
		}); err != nil {
			return fmt.Errorf("emit google workspace entitlement %s state: %w", row.AccountExternalID, err)
		}
		current := idx + 1
		if current%googleWorkspaceEntitlementBatchSize == 0 || current == len(rows) {
			report(registry.Event{
				Source:  configstore.KindGoogleWorkspace,
				Stage:   "write-entitlements",
				Current: int64(current),
				Total:   int64(len(rows)),
				Message: fmt.Sprintf("entitlements %d/%d", current, len(rows)),
			})
		}
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

func pgTime(value pgtype.Timestamptz) time.Time {
	if !value.Valid || value.Time.IsZero() {
		return time.Time{}
	}
	return value.Time.UTC()
}

func googleWorkspaceEntitlementKey(accountExternalID, kind, resource, permission string) string {
	return strings.Join([]string{
		strings.TrimSpace(accountExternalID),
		strings.TrimSpace(kind),
		strings.TrimSpace(resource),
		strings.TrimSpace(permission),
	}, "|")
}

func (i *GoogleWorkspaceIntegration) upsertAppAssets(ctx context.Context, emitter capabilities.RecordEmitter, report func(registry.Event), runID int64, rows []googleWorkspaceAppAssetRow) error {
	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-app-assets", Current: 0, Total: int64(len(rows)), Message: fmt.Sprintf("writing %d app assets", len(rows))})
	if len(rows) == 0 {
		return nil
	}
	for idx, row := range rows {
		if err := emitter.UpsertState(ctx, records.StateUpsert{
			Source:         records.SourceRef{Kind: configstore.KindGoogleWorkspace, Name: i.customerID},
			Resource:       records.ResourceAppAsset,
			Key:            appAssetRefExternalID(row.AssetKind, row.ExternalID),
			ProviderID:     row.ExternalID,
			ObservedAt:     time.Now().UTC(),
			DedupeKeyValue: fmt.Sprintf("%s:%s:app_asset:%s:%s", configstore.KindGoogleWorkspace, i.customerID, row.AssetKind, row.ExternalID),
			Payload: records.AppAssetPayload{
				AssetKind:        row.AssetKind,
				ExternalID:       row.ExternalID,
				ParentExternalID: row.ParentExternalID,
				DisplayName:      row.DisplayName,
				Status:           row.Status,
				CreatedAtSource:  pgTime(row.CreatedAtSource),
				UpdatedAtSource:  pgTime(row.UpdatedAtSource),
				Raw:              records.MapFromJSON(row.RawJSON),
			},
		}); err != nil {
			return fmt.Errorf("emit google app asset %s/%s state: %w", row.AssetKind, row.ExternalID, err)
		}
		current := idx + 1
		if current%googleWorkspaceAssetBatchSize == 0 || current == len(rows) {
			report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-app-assets", Current: int64(current), Total: int64(len(rows)), Message: fmt.Sprintf("app assets %d/%d", current, len(rows))})
		}
	}
	return nil
}

func (i *GoogleWorkspaceIntegration) upsertAppAssetOwners(ctx context.Context, emitter capabilities.RecordEmitter, report func(registry.Event), runID int64, rows []googleWorkspaceAppAssetOwnerRow) error {
	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-owners", Current: 0, Total: int64(len(rows)), Message: fmt.Sprintf("writing %d owners", len(rows))})
	if len(rows) == 0 {
		return nil
	}
	for idx, row := range rows {
		if err := emitter.UpsertState(ctx, records.StateUpsert{
			Source:         records.SourceRef{Kind: configstore.KindGoogleWorkspace, Name: i.customerID},
			Resource:       records.ResourceAppAssetOwner,
			Key:            appAssetRefExternalID(row.AssetKind, row.AssetExternalID) + ":" + row.OwnerKind + ":" + row.OwnerExternalID,
			ProviderID:     row.OwnerExternalID,
			ObservedAt:     time.Now().UTC(),
			DedupeKeyValue: fmt.Sprintf("%s:%s:app_asset_owner:%s:%s:%s:%s", configstore.KindGoogleWorkspace, i.customerID, row.AssetKind, row.AssetExternalID, row.OwnerKind, row.OwnerExternalID),
			Payload: records.AppAssetOwnerPayload{
				AssetKind:        row.AssetKind,
				AssetExternalID:  row.AssetExternalID,
				OwnerKind:        row.OwnerKind,
				OwnerExternalID:  row.OwnerExternalID,
				OwnerDisplayName: row.OwnerDisplayName,
				OwnerEmail:       row.OwnerEmail,
				Raw:              records.MapFromJSON(row.RawJSON),
			},
		}); err != nil {
			return fmt.Errorf("emit google app owner %s/%s/%s state: %w", row.AssetKind, row.AssetExternalID, row.OwnerExternalID, err)
		}
		current := idx + 1
		if current%googleWorkspaceOwnerBatchSize == 0 || current == len(rows) {
			report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-owners", Current: int64(current), Total: int64(len(rows)), Message: fmt.Sprintf("owners %d/%d", current, len(rows))})
		}
	}
	return nil
}

func (i *GoogleWorkspaceIntegration) upsertCredentialArtifacts(ctx context.Context, emitter capabilities.RecordEmitter, report func(registry.Event), runID int64, rows []googleWorkspaceCredentialArtifactRow) error {
	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-credentials", Current: 0, Total: int64(len(rows)), Message: fmt.Sprintf("writing %d credentials", len(rows))})
	if len(rows) == 0 {
		return nil
	}
	for idx, row := range rows {
		if err := emitter.UpsertState(ctx, records.StateUpsert{
			Source:         records.SourceRef{Kind: configstore.KindGoogleWorkspace, Name: i.customerID},
			Resource:       records.ResourceCredential,
			Key:            row.AssetRefKind + ":" + row.AssetRefExternalID + ":" + row.CredentialKind + ":" + row.ExternalID,
			ProviderID:     row.ExternalID,
			ObservedAt:     time.Now().UTC(),
			DedupeKeyValue: fmt.Sprintf("%s:%s:credential:%s:%s:%s:%s", configstore.KindGoogleWorkspace, i.customerID, row.AssetRefKind, row.AssetRefExternalID, row.CredentialKind, row.ExternalID),
			Payload: records.CredentialPayload{
				AssetRefKind:       row.AssetRefKind,
				AssetRefExternalID: row.AssetRefExternalID,
				CredentialKind:     row.CredentialKind,
				ExternalID:         row.ExternalID,
				DisplayName:        row.DisplayName,
				Fingerprint:        row.Fingerprint,
				ScopeJSON:          row.ScopeJSON,
				Status:             row.Status,
				CreatedAtSource:    pgTime(row.CreatedAtSource),
				ExpiresAtSource:    pgTime(row.ExpiresAtSource),
				LastUsedAtSource:   pgTime(row.LastUsedAtSource),
				CreatedBy: records.PrincipalRef{
					Kind:        row.CreatedByKind,
					ExternalID:  row.CreatedByExternalID,
					DisplayName: row.CreatedByDisplayName,
				},
				ApprovedBy: records.PrincipalRef{
					Kind:        row.ApprovedByKind,
					ExternalID:  row.ApprovedByExternalID,
					DisplayName: row.ApprovedByDisplayName,
				},
				Raw: records.MapFromJSON(row.RawJSON),
			},
		}); err != nil {
			return fmt.Errorf("emit google credential %s/%s state: %w", row.CredentialKind, row.ExternalID, err)
		}
		current := idx + 1
		if current%googleWorkspaceCredentialBatchSize == 0 || current == len(rows) {
			report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-credentials", Current: int64(current), Total: int64(len(rows)), Message: fmt.Sprintf("credentials %d/%d", current, len(rows))})
		}
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

func (i *GoogleWorkspaceIntegration) upsertCredentialAuditEvents(ctx context.Context, emitter capabilities.RecordEmitter, report func(registry.Event), rows []googleWorkspaceCredentialAuditEventRow) error {
	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-audit-events", Current: 0, Total: int64(len(rows)), Message: fmt.Sprintf("writing %d audit events", len(rows))})
	if len(rows) == 0 {
		return nil
	}
	for idx, row := range rows {
		if err := emitter.UpsertState(ctx, records.StateUpsert{
			Source:         records.SourceRef{Kind: configstore.KindGoogleWorkspace, Name: i.customerID},
			Resource:       records.ResourceAuditEvent,
			Key:            row.EventExternalID,
			ProviderID:     row.EventExternalID,
			ObservedAt:     pgTime(row.EventTime),
			DedupeKeyValue: fmt.Sprintf("%s:%s:credential_audit_event:%s", configstore.KindGoogleWorkspace, i.customerID, row.EventExternalID),
			Payload: records.CredentialAuditEventPayload{
				EventExternalID:      row.EventExternalID,
				EventType:            row.EventType,
				EventTime:            pgTime(row.EventTime),
				ActorKind:            row.ActorKind,
				ActorExternalID:      row.ActorExternalID,
				ActorDisplayName:     row.ActorDisplayName,
				TargetKind:           row.TargetKind,
				TargetExternalID:     row.TargetExternalID,
				TargetDisplayName:    row.TargetDisplayName,
				CredentialKind:       row.CredentialKind,
				CredentialExternalID: row.CredentialExternalID,
				Raw:                  records.MapFromJSON(row.RawJSON),
			},
		}); err != nil {
			return fmt.Errorf("emit google audit event %s state: %w", row.EventExternalID, err)
		}
		current := idx + 1
		if current%googleWorkspaceAuditEventBatchSize == 0 || current == len(rows) {
			report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-audit-events", Current: int64(current), Total: int64(len(rows)), Message: fmt.Sprintf("audit events %d/%d", current, len(rows))})
		}
	}
	return nil
}

func (i *GoogleWorkspaceIntegration) syncDiscovery(ctx context.Context, q *gen.Queries, emitter capabilities.RecordEmitter, report func(registry.Event), runID int64) error {
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

	if err := i.writeDiscoveryRows(ctx, emitter, report, runID, sources, events); err != nil {
		metrics.DiscoveryIngestFailuresTotal.WithLabelValues(configstore.KindGoogleWorkspace, discovery.SignalKindIDPSSO, "db_error").Inc()
		return err
	}
	if err := i.seedGoogleWorkspaceAutoBindings(ctx, q, runID); err != nil {
		return err
	}
	return nil
}

func (i *GoogleWorkspaceIntegration) normalizeDiscovery(loginActivities, tokenActivities []WorkspaceActivity, tokenGrants []WorkspaceOAuthTokenGrant, now time.Time) ([]discovery.SourceRow, []discovery.EventRow) {
	sourceByID := map[string]discovery.SourceRow{}
	events := make([]discovery.EventRow, 0, len(loginActivities)+len(tokenActivities)+len(tokenGrants))

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
			sourceByID[sourceAppID] = discovery.SourceRow{
				CanonicalKey:     metadata.CanonicalKey,
				SourceAppID:      sourceAppID,
				SourceAppName:    sourceAppName,
				SourceAppDomain:  metadata.Domain,
				SourceVendorName: metadata.VendorName,
				SourceCategory:   metadata.Category,
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
			events = append(events, discovery.EventRow{
				CanonicalKey:     metadata.CanonicalKey,
				SignalKind:       discovery.SignalKindIDPSSO,
				EventExternalID:  activityEventExternalID("login", activity, 0),
				SourceAppID:      sourceAppID,
				SourceAppName:    sourceAppName,
				SourceAppDomain:  metadata.Domain,
				SourceVendorName: metadata.VendorName,
				SourceCategory:   metadata.Category,
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
			events = append(events, discovery.EventRow{
				CanonicalKey:     metadata.CanonicalKey,
				SignalKind:       discovery.SignalKindIDPSSO,
				EventExternalID:  activityEventExternalID("login", activity, idx),
				SourceAppID:      sourceAppID,
				SourceAppName:    sourceAppName,
				SourceAppDomain:  metadata.Domain,
				SourceVendorName: metadata.VendorName,
				SourceCategory:   metadata.Category,
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
			events = append(events, discovery.EventRow{
				CanonicalKey:     metadata.CanonicalKey,
				SignalKind:       discovery.SignalKindOAuth,
				EventExternalID:  activityEventExternalID("token", activity, 0),
				SourceAppID:      sourceAppID,
				SourceAppName:    sourceAppName,
				SourceAppDomain:  metadata.Domain,
				SourceVendorName: metadata.VendorName,
				SourceCategory:   metadata.Category,
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
			events = append(events, discovery.EventRow{
				CanonicalKey:     metadata.CanonicalKey,
				SignalKind:       discovery.SignalKindOAuth,
				EventExternalID:  activityEventExternalID("token", activity, idx),
				SourceAppID:      sourceAppID,
				SourceAppName:    sourceAppName,
				SourceAppDomain:  metadata.Domain,
				SourceVendorName: metadata.VendorName,
				SourceCategory:   metadata.Category,
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
		events = append(events, discovery.EventRow{
			CanonicalKey:     metadata.CanonicalKey,
			SignalKind:       discovery.SignalKindOAuth,
			EventExternalID:  "inventory:" + googleWorkspaceGrantExternalID(sourceAppID, userKey),
			SourceAppID:      sourceAppID,
			SourceAppName:    sourceAppName,
			SourceAppDomain:  metadata.Domain,
			SourceVendorName: metadata.VendorName,
			SourceCategory:   metadata.Category,
			ActorExternalID:  userKey,
			ActorEmail:       actorEmail,
			ActorDisplayName: actorEmail,
			ObservedAt:       now,
			Scopes:           discovery.NormalizeScopes(grant.Scopes),
			RawJSON:          registry.NormalizeJSON(grant.RawJSON),
		})
	}

	sources := make([]discovery.SourceRow, 0, len(sourceByID))
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

func (i *GoogleWorkspaceIntegration) writeDiscoveryRows(ctx context.Context, emitter capabilities.RecordEmitter, report func(registry.Event), runID int64, sources []discovery.SourceRow, events []discovery.EventRow) error {
	total := len(sources) + len(events)
	report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-discovery", Current: 0, Total: int64(total), Message: fmt.Sprintf("writing %d discovery records", total)})
	current := 0
	for _, row := range sources {
		if err := emitter.UpsertState(ctx, records.StateUpsert{
			Source:         records.SourceRef{Kind: configstore.KindGoogleWorkspace, Name: i.customerID},
			Resource:       records.ResourceDiscoveryEvidence,
			Key:            "source:" + row.SourceAppID,
			ProviderID:     row.SourceAppID,
			ObservedAt:     row.SeenAt,
			DedupeKeyValue: fmt.Sprintf("%s:%s:discovery_source:%s", configstore.KindGoogleWorkspace, i.customerID, row.SourceAppID),
			Payload: records.DiscoveryEvidencePayload{
				ExternalID:       "source:" + row.SourceAppID,
				Kind:             records.DiscoveryEvidenceKindSource,
				CanonicalKey:     row.CanonicalKey,
				SourceAppID:      row.SourceAppID,
				SourceAppName:    row.SourceAppName,
				SourceAppDomain:  row.SourceAppDomain,
				SourceVendorName: row.SourceVendorName,
				SourceCategory:   row.SourceCategory,
				ObservedAt:       row.SeenAt,
			},
		}); err != nil {
			return fmt.Errorf("emit google discovery source %s state: %w", row.SourceAppID, err)
		}
		current++
		report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-discovery", Current: int64(current), Total: int64(total), Message: fmt.Sprintf("discovery records %d/%d", current, total)})
	}
	for _, row := range events {
		if err := emitter.UpsertState(ctx, records.StateUpsert{
			Source:         records.SourceRef{Kind: configstore.KindGoogleWorkspace, Name: i.customerID},
			Resource:       records.ResourceDiscoveryEvidence,
			Key:            "event:" + row.EventExternalID,
			ProviderID:     row.EventExternalID,
			ObservedAt:     row.ObservedAt,
			DedupeKeyValue: fmt.Sprintf("%s:%s:discovery_event:%s", configstore.KindGoogleWorkspace, i.customerID, row.EventExternalID),
			Payload: records.DiscoveryEvidencePayload{
				ExternalID:       "event:" + row.EventExternalID,
				Kind:             records.DiscoveryEvidenceKindEvent,
				CanonicalKey:     row.CanonicalKey,
				SignalKind:       row.SignalKind,
				EventExternalID:  row.EventExternalID,
				SourceAppID:      row.SourceAppID,
				SourceAppName:    row.SourceAppName,
				SourceAppDomain:  row.SourceAppDomain,
				SourceVendorName: row.SourceVendorName,
				SourceCategory:   row.SourceCategory,
				ActorExternalID:  row.ActorExternalID,
				ActorEmail:       row.ActorEmail,
				ActorDisplayName: row.ActorDisplayName,
				ObservedAt:       row.ObservedAt,
				Scopes:           row.Scopes,
				Raw:              records.MapFromJSON(row.RawJSON),
			},
		}); err != nil {
			return fmt.Errorf("emit google discovery event %s state: %w", row.EventExternalID, err)
		}
		current++
		report(registry.Event{Source: configstore.KindGoogleWorkspace, Stage: "write-discovery", Current: int64(current), Total: int64(total), Message: fmt.Sprintf("discovery records %d/%d", current, total)})
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

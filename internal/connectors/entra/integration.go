package entra

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	msgraphmodels "github.com/microsoftgraph/msgraph-sdk-go/models"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/discovery"
	"github.com/open-sspm/open-sspm/internal/metrics"
)

const (
	entraUserBatchSize       = 1000
	entraAppAssetBatchSize   = 1000
	entraOwnerBatchSize      = 2000
	entraCredentialBatchSize = 2000
	entraAuditEventBatchSize = 2000
)

var credentialGUIDPattern = regexp.MustCompile(`(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)

type EntraIntegration struct {
	client           entraClient
	tenantID         string
	discoveryEnabled bool
}

type entraClient interface {
	ListApplications(context.Context) ([]Application, error)
	ListServicePrincipals(context.Context) ([]ServicePrincipal, error)
	ListServicePrincipalAssignedTo(context.Context, string) ([]ServicePrincipalAppRoleAssignment, error)
	ListDirectoryAudits(context.Context, *time.Time) ([]DirectoryAuditEvent, error)
	ListDirectoryRoles(context.Context) ([]DirectoryRole, error)
	ListDirectoryRoleAssignments(context.Context) ([]DirectoryRoleAssignment, error)
	ListUsers(context.Context) ([]User, error)
	ListGroups(context.Context) ([]Group, error)
	ListGroupTransitiveUserMembers(context.Context, string) ([]User, error)
	ListApplicationOwners(context.Context, string) ([]DirectoryOwner, error)
	ListServicePrincipalOwners(context.Context, string) ([]DirectoryOwner, error)
	ListSignIns(context.Context, *time.Time) ([]SignInEvent, error)
	ListOAuth2PermissionGrants(context.Context) ([]OAuth2PermissionGrant, error)
	LookupUsersByIDs(context.Context, []string) ([]User, error)
}

type appAssetUpsertRow struct {
	AssetKind        string
	ExternalID       string
	ParentExternalID string
	DisplayName      string
	Status           string
	CreatedAtSource  pgtype.Timestamptz
	UpdatedAtSource  pgtype.Timestamptz
	RawJSON          []byte
}

type appAssetOwnerUpsertRow struct {
	AssetKind        string
	AssetExternalID  string
	OwnerKind        string
	OwnerExternalID  string
	OwnerDisplayName string
	OwnerEmail       string
	RawJSON          []byte
}

type credentialArtifactUpsertRow struct {
	AssetRefKind       string
	AssetRefExternalID string
	CredentialKind     string
	ExternalID         string
	DisplayName        string
	Fingerprint        string
	ScopeJSON          []byte
	Status             string
	CreatedAtSource    pgtype.Timestamptz
	ExpiresAtSource    pgtype.Timestamptz
	LastUsedAtSource   pgtype.Timestamptz
	RawJSON            []byte
}

type credentialAuditEventUpsertRow struct {
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

func NewEntraIntegration(client entraClient, tenantID string, discoveryEnabled bool) *EntraIntegration {
	return &EntraIntegration{
		client:           client,
		tenantID:         strings.ToLower(strings.TrimSpace(tenantID)),
		discoveryEnabled: discoveryEnabled,
	}
}

func (i *EntraIntegration) Kind() string { return "entra" }
func (i *EntraIntegration) Name() string { return i.tenantID }
func (i *EntraIntegration) Role() registry.IntegrationRole {
	return registry.RoleApp
}

func (i *EntraIntegration) SupportsRunMode(mode registry.RunMode) bool {
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

func (i *EntraIntegration) InitEvents() []registry.Event {
	return []registry.Event{
		{Source: "entra", Stage: "list-users", Current: 0, Total: 1, Message: "listing Entra users"},
		{Source: "entra", Stage: "list-groups", Current: 0, Total: 1, Message: "listing Entra groups"},
		{Source: "entra", Stage: "write-users", Current: 0, Total: registry.UnknownTotal, Message: "writing Entra users"},
		{Source: "entra", Stage: "list-app-assets", Current: 0, Total: 1, Message: "listing Entra applications and service principals"},
		{Source: "entra", Stage: "write-app-assets", Current: 0, Total: registry.UnknownTotal, Message: "writing Entra app assets"},
		{Source: "entra", Stage: "list-entitlements", Current: 0, Total: registry.UnknownTotal, Message: "listing Entra entitlements"},
		{Source: "entra", Stage: "write-entitlements", Current: 0, Total: registry.UnknownTotal, Message: "writing Entra entitlements"},
		{Source: "entra", Stage: "list-owners", Current: 0, Total: registry.UnknownTotal, Message: "listing Entra app owners"},
		{Source: "entra", Stage: "write-owners", Current: 0, Total: registry.UnknownTotal, Message: "writing Entra app owners"},
		{Source: "entra", Stage: "write-credentials", Current: 0, Total: registry.UnknownTotal, Message: "writing Entra credential metadata"},
		{Source: "entra", Stage: "list-audit-events", Current: 0, Total: 1, Message: "listing Entra directory audit events"},
		{Source: "entra", Stage: "write-audit-events", Current: 0, Total: registry.UnknownTotal, Message: "writing Entra credential audit events"},
		{Source: "entra", Stage: "list-discovery-events", Current: 0, Total: 1, Message: "listing Entra discovery signals"},
		{Source: "entra", Stage: "resolve-grant-actors", Current: 0, Total: 1, Message: "resolving Entra grant actors"},
		{Source: "entra", Stage: "normalize-discovery", Current: 0, Total: 1, Message: "normalizing discovery evidence"},
		{Source: "entra", Stage: "write-discovery", Current: 0, Total: registry.UnknownTotal, Message: "writing discovery data"},
	}
}

func (i *EntraIntegration) Run(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, report func(registry.Event), mode registry.RunMode) error {
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

func (i *EntraIntegration) runFull(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, report func(registry.Event)) error {
	started := time.Now()
	slog.Info("syncing Microsoft Entra ID")

	runID, err := registry.StartSyncRun(ctx, q, registry.SyncRunSourceKind("entra", registry.RunModeFull), i.tenantID)
	if err != nil {
		return err
	}

	usersWritten, err := i.syncUsers(ctx, q, report, runID)
	if err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindUnknown)
	}
	groupsWritten, err := i.syncGroups(ctx, q, report, runID)
	if err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindUnknown)
	}

	report(registry.Event{Source: "entra", Stage: "list-app-assets", Current: 0, Total: 1, Message: "listing applications and service principals"})
	applications, err := i.client.ListApplications(ctx)
	if err != nil {
		report(registry.Event{Source: "entra", Stage: "list-app-assets", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
	}
	servicePrincipals, err := i.client.ListServicePrincipals(ctx)
	if err != nil {
		report(registry.Event{Source: "entra", Stage: "list-app-assets", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
	}
	report(registry.Event{
		Source:  "entra",
		Stage:   "list-app-assets",
		Current: 1,
		Total:   1,
		Message: fmt.Sprintf("found %d applications and %d service principals", len(applications), len(servicePrincipals)),
	})

	servicePrincipalsWritten, err := i.syncServicePrincipalAccounts(ctx, q, report, runID, servicePrincipals)
	if err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}

	assetRows, credentialRows, err := buildEntraAssetAndCredentialRows(applications, servicePrincipals)
	if err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindUnknown)
	}
	if err := i.upsertAppAssets(ctx, q, report, runID, assetRows); err != nil {
		report(registry.Event{Source: "entra", Stage: "write-app-assets", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}

	ownerRows, err := i.collectAppAssetOwners(ctx, report, applications, servicePrincipals)
	if err != nil {
		report(registry.Event{Source: "entra", Stage: "list-owners", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
	}
	if err := i.upsertAppAssetOwners(ctx, q, report, runID, ownerRows); err != nil {
		report(registry.Event{Source: "entra", Stage: "write-owners", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}

	if err := i.upsertCredentialArtifacts(ctx, q, report, runID, credentialRows); err != nil {
		report(registry.Event{Source: "entra", Stage: "write-credentials", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}

	entitlementRows, err := i.collectEntraEntitlements(ctx, report, servicePrincipals)
	if err != nil {
		report(registry.Event{Source: "entra", Stage: "list-entitlements", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
	}
	if err := i.upsertEntraEntitlements(ctx, q, report, runID, entitlementRows); err != nil {
		report(registry.Event{Source: "entra", Stage: "write-entitlements", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}

	report(registry.Event{Source: "entra", Stage: "list-audit-events", Current: 0, Total: 1, Message: "listing directory audit events"})
	directoryAudits, err := i.client.ListDirectoryAudits(ctx, nil)
	if err != nil {
		report(registry.Event{Source: "entra", Stage: "list-audit-events", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
	}
	report(registry.Event{
		Source:  "entra",
		Stage:   "list-audit-events",
		Current: 1,
		Total:   1,
		Message: fmt.Sprintf("found %d directory audit events", len(directoryAudits)),
	})

	auditEventRows, err := buildCredentialAuditEventRows(directoryAudits)
	if err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindUnknown)
	}
	if err := i.upsertCredentialAuditEvents(ctx, q, report, auditEventRows); err != nil {
		report(registry.Event{Source: "entra", Stage: "write-audit-events", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}

	if err := registry.FinalizeAppRun(ctx, q, pool, runID, "entra", i.tenantID, time.Since(started), false); err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}

	slog.Info(
		"entra sync complete",
		"tenant", i.tenantID,
		"users", usersWritten,
		"groups", groupsWritten,
		"service_principals", servicePrincipalsWritten,
		"app_assets", len(assetRows),
		"owners", len(ownerRows),
		"credentials", len(credentialRows),
		"entitlements", len(entitlementRows),
		"audit_events", len(auditEventRows),
	)
	return nil
}

func (i *EntraIntegration) runDiscovery(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, report func(registry.Event)) error {
	started := time.Now()
	slog.Info("syncing Microsoft Entra ID discovery")

	runID, err := registry.StartSyncRun(ctx, q, registry.SyncRunSourceKind("entra", registry.RunModeDiscovery), i.tenantID)
	if err != nil {
		return err
	}

	applications, err := i.client.ListApplications(ctx)
	if err != nil {
		report(registry.Event{Source: "entra", Stage: "list-app-assets", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
	}
	servicePrincipals, err := i.client.ListServicePrincipals(ctx)
	if err != nil {
		report(registry.Event{Source: "entra", Stage: "list-app-assets", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
	}

	if err := i.syncDiscovery(ctx, q, report, runID, applications, servicePrincipals); err != nil {
		report(registry.Event{Source: "entra", Stage: "write-discovery", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindUnknown)
	}
	if err := registry.FinalizeDiscoveryRun(ctx, q, pool, runID, "entra", i.tenantID, time.Since(started)); err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}
	slog.Info("entra discovery sync complete", "tenant", i.tenantID)
	return nil
}

func (i *EntraIntegration) syncUsers(ctx context.Context, q *gen.Queries, report func(registry.Event), runID int64) (int, error) {
	users, err := i.client.ListUsers(ctx)
	if err != nil {
		report(registry.Event{Source: "entra", Stage: "list-users", Message: err.Error(), Err: err})
		return 0, err
	}
	report(registry.Event{Source: "entra", Stage: "list-users", Current: 1, Total: 1, Message: fmt.Sprintf("found %d users", len(users))})
	report(registry.Event{Source: "entra", Stage: "write-users", Current: 0, Total: int64(len(users)), Message: fmt.Sprintf("writing %d users", len(users))})

	externalIDs := make([]string, 0, len(users))
	emails := make([]string, 0, len(users))
	displayNames := make([]string, 0, len(users))
	accountKinds := make([]string, 0, len(users))
	entityCategories := make([]string, 0, len(users))
	rawJSONs := make([][]byte, 0, len(users))
	lastLoginAts := make([]pgtype.Timestamptz, 0, len(users))
	lastLoginIps := make([]string, 0, len(users))
	lastLoginRegions := make([]string, 0, len(users))

	for _, user := range users {
		externalID := entityID(user)
		if externalID == "" {
			continue
		}

		email := normalizeEmail(preferredEmail(user))

		display := stringValue(user.GetDisplayName())
		if display == "" {
			display = strings.TrimSpace(email)
		}
		if display == "" {
			display = externalID
		}

		raw, err := mergeSerializedSDKModel(user, map[string]any{
			"status": entraAccountStatus(user.GetAccountEnabled()),
		})
		if err != nil {
			return 0, fmt.Errorf("serialize entra user %s: %w", externalID, err)
		}

		externalIDs = append(externalIDs, externalID)
		emails = append(emails, email)
		displayNames = append(displayNames, display)
		accountKinds = append(accountKinds, entraUserAccountKind(user))
		entityCategories = append(entityCategories, registry.EntityCategoryUser)
		rawJSONs = append(rawJSONs, registry.WithEntityCategory(raw, registry.EntityCategoryUser))
		lastLoginAts = append(lastLoginAts, pgtype.Timestamptz{})
		lastLoginIps = append(lastLoginIps, "")
		lastLoginRegions = append(lastLoginRegions, "")
	}

	for start := 0; start < len(externalIDs); start += entraUserBatchSize {
		end := min(start+entraUserBatchSize, len(externalIDs))

		_, err := q.UpsertSourceAccountsBulkBySource(ctx, gen.UpsertSourceAccountsBulkBySourceParams{
			SourceKind:       "entra",
			SourceName:       i.tenantID,
			SeenInRunID:      runID,
			ExternalIds:      externalIDs[start:end],
			Emails:           emails[start:end],
			DisplayNames:     displayNames[start:end],
			AccountKinds:     accountKinds[start:end],
			EntityCategories: entityCategories[start:end],
			RawJsons:         rawJSONs[start:end],
			LastLoginAts:     lastLoginAts[start:end],
			LastLoginIps:     lastLoginIps[start:end],
			LastLoginRegions: lastLoginRegions[start:end],
		})
		if err != nil {
			return 0, err
		}

		report(registry.Event{
			Source:  "entra",
			Stage:   "write-users",
			Current: int64(end),
			Total:   int64(len(externalIDs)),
			Message: fmt.Sprintf("users %d/%d", end, len(externalIDs)),
		})
	}

	return len(externalIDs), nil
}

func (i *EntraIntegration) syncGroups(ctx context.Context, q *gen.Queries, report func(registry.Event), runID int64) (int, error) {
	groups, err := i.client.ListGroups(ctx)
	if err != nil {
		report(registry.Event{Source: "entra", Stage: "list-groups", Message: err.Error(), Err: err})
		return 0, err
	}
	report(registry.Event{Source: "entra", Stage: "list-groups", Current: 1, Total: 1, Message: fmt.Sprintf("found %d groups", len(groups))})
	if len(groups) == 0 {
		return 0, nil
	}

	report(registry.Event{Source: "entra", Stage: "write-users", Current: 0, Total: int64(len(groups)), Message: fmt.Sprintf("writing %d groups", len(groups))})

	externalIDs := make([]string, 0, len(groups))
	emails := make([]string, 0, len(groups))
	displayNames := make([]string, 0, len(groups))
	accountKinds := make([]string, 0, len(groups))
	entityCategories := make([]string, 0, len(groups))
	rawJSONs := make([][]byte, 0, len(groups))
	lastLoginAts := make([]pgtype.Timestamptz, 0, len(groups))
	lastLoginIps := make([]string, 0, len(groups))
	lastLoginRegions := make([]string, 0, len(groups))

	for _, group := range groups {
		externalID := entraGroupExternalID(entityID(group))
		if externalID == "" {
			continue
		}

		display := stringValue(group.GetDisplayName())
		if display == "" {
			display = externalID
		}

		externalIDs = append(externalIDs, externalID)
		emails = append(emails, normalizeEmail(stringValue(group.GetMail())))
		displayNames = append(displayNames, display)
		accountKinds = append(accountKinds, entraGroupAccountKind(group))
		entityCategories = append(entityCategories, registry.EntityCategoryGroup)
		raw, err := mergeSerializedSDKModel(group, map[string]any{
			"status": "",
		})
		if err != nil {
			return 0, fmt.Errorf("serialize entra group %s: %w", externalID, err)
		}
		rawJSONs = append(rawJSONs, registry.WithEntityCategory(raw, registry.EntityCategoryGroup))
		lastLoginAts = append(lastLoginAts, pgtype.Timestamptz{})
		lastLoginIps = append(lastLoginIps, "")
		lastLoginRegions = append(lastLoginRegions, "")
	}

	for start := 0; start < len(externalIDs); start += entraUserBatchSize {
		end := min(start+entraUserBatchSize, len(externalIDs))

		_, err := q.UpsertSourceAccountsBulkBySource(ctx, gen.UpsertSourceAccountsBulkBySourceParams{
			SourceKind:       "entra",
			SourceName:       i.tenantID,
			SeenInRunID:      runID,
			ExternalIds:      externalIDs[start:end],
			Emails:           emails[start:end],
			DisplayNames:     displayNames[start:end],
			AccountKinds:     accountKinds[start:end],
			EntityCategories: entityCategories[start:end],
			RawJsons:         rawJSONs[start:end],
			LastLoginAts:     lastLoginAts[start:end],
			LastLoginIps:     lastLoginIps[start:end],
			LastLoginRegions: lastLoginRegions[start:end],
		})
		if err != nil {
			return 0, err
		}

		report(registry.Event{
			Source:  "entra",
			Stage:   "write-users",
			Current: int64(end),
			Total:   int64(len(externalIDs)),
			Message: fmt.Sprintf("groups %d/%d", end, len(externalIDs)),
		})
	}

	return len(externalIDs), nil
}

func (i *EntraIntegration) syncServicePrincipalAccounts(ctx context.Context, q *gen.Queries, report func(registry.Event), runID int64, servicePrincipals []ServicePrincipal) (int, error) {
	if len(servicePrincipals) == 0 {
		return 0, nil
	}

	report(registry.Event{
		Source:  "entra",
		Stage:   "write-users",
		Current: 0,
		Total:   int64(len(servicePrincipals)),
		Message: fmt.Sprintf("writing %d service principals", len(servicePrincipals)),
	})

	externalIDs := make([]string, 0, len(servicePrincipals))
	emails := make([]string, 0, len(servicePrincipals))
	displayNames := make([]string, 0, len(servicePrincipals))
	accountKinds := make([]string, 0, len(servicePrincipals))
	entityCategories := make([]string, 0, len(servicePrincipals))
	rawJSONs := make([][]byte, 0, len(servicePrincipals))
	lastLoginAts := make([]pgtype.Timestamptz, 0, len(servicePrincipals))
	lastLoginIps := make([]string, 0, len(servicePrincipals))
	lastLoginRegions := make([]string, 0, len(servicePrincipals))

	for _, sp := range servicePrincipals {
		externalID := entraServicePrincipalExternalID(entityID(sp))
		if externalID == "" {
			continue
		}

		display := stringValue(sp.GetDisplayName())
		if display == "" {
			display = stringValue(sp.GetAppId())
		}
		if display == "" {
			display = externalID
		}

		externalIDs = append(externalIDs, externalID)
		emails = append(emails, "")
		displayNames = append(displayNames, display)
		accountKinds = append(accountKinds, entraServicePrincipalAccountKind(sp))
		entityCategories = append(entityCategories, registry.EntityCategoryServicePrincipal)
		raw, err := mergeSerializedSDKModel(sp, map[string]any{
			"status": entraAccountStatus(sp.GetAccountEnabled()),
		})
		if err != nil {
			return 0, fmt.Errorf("serialize entra service principal %s: %w", externalID, err)
		}
		rawJSONs = append(rawJSONs, registry.WithEntityCategory(raw, registry.EntityCategoryServicePrincipal))
		lastLoginAts = append(lastLoginAts, pgtype.Timestamptz{})
		lastLoginIps = append(lastLoginIps, "")
		lastLoginRegions = append(lastLoginRegions, "")
	}

	for start := 0; start < len(externalIDs); start += entraUserBatchSize {
		end := min(start+entraUserBatchSize, len(externalIDs))

		_, err := q.UpsertSourceAccountsBulkBySource(ctx, gen.UpsertSourceAccountsBulkBySourceParams{
			SourceKind:       "entra",
			SourceName:       i.tenantID,
			SeenInRunID:      runID,
			ExternalIds:      externalIDs[start:end],
			Emails:           emails[start:end],
			DisplayNames:     displayNames[start:end],
			AccountKinds:     accountKinds[start:end],
			EntityCategories: entityCategories[start:end],
			RawJsons:         rawJSONs[start:end],
			LastLoginAts:     lastLoginAts[start:end],
			LastLoginIps:     lastLoginIps[start:end],
			LastLoginRegions: lastLoginRegions[start:end],
		})
		if err != nil {
			return 0, err
		}

		report(registry.Event{
			Source:  "entra",
			Stage:   "write-users",
			Current: int64(end),
			Total:   int64(len(externalIDs)),
			Message: fmt.Sprintf("service principals %d/%d", end, len(externalIDs)),
		})
	}

	return len(externalIDs), nil
}

func buildEntraAssetAndCredentialRows(applications []Application, servicePrincipals []ServicePrincipal) ([]appAssetUpsertRow, []credentialArtifactUpsertRow, error) {
	assetRows := make([]appAssetUpsertRow, 0, len(applications)+len(servicePrincipals))
	credentialRows := make([]credentialArtifactUpsertRow, 0)

	for _, app := range applications {
		externalID := entityID(app)
		if externalID == "" {
			continue
		}
		displayName := stringValue(app.GetDisplayName())
		if displayName == "" {
			displayName = externalID
		}

		rawJSON, err := serializeSDKModel(app)
		if err != nil {
			return nil, nil, fmt.Errorf("serialize entra application %s: %w", externalID, err)
		}

		assetRows = append(assetRows, appAssetUpsertRow{
			AssetKind:        "entra_application",
			ExternalID:       externalID,
			ParentExternalID: "",
			DisplayName:      displayName,
			Status:           "",
			CreatedAtSource:  parseGraphTime(timeValueString(app.GetCreatedDateTime())),
			UpdatedAtSource:  pgtype.Timestamptz{},
			RawJSON:          registry.NormalizeJSON(rawJSON),
		})

		assetRefExternalID := appAssetRefExternalID("entra_application", externalID)
		for _, credential := range app.GetPasswordCredentials() {
			row, err := buildEntraPasswordCredentialRow("entra_application", externalID, assetRefExternalID, credential)
			if err != nil {
				return nil, nil, err
			}
			credentialRows = append(credentialRows, row)
		}
		for _, credential := range app.GetKeyCredentials() {
			row, err := buildEntraCertificateCredentialRow("entra_application", externalID, assetRefExternalID, credential)
			if err != nil {
				return nil, nil, err
			}
			credentialRows = append(credentialRows, row)
		}
	}

	for _, sp := range servicePrincipals {
		externalID := entityID(sp)
		if externalID == "" {
			continue
		}
		displayName := stringValue(sp.GetDisplayName())
		if displayName == "" {
			displayName = externalID
		}

		rawJSON, err := serializeSDKModel(sp)
		if err != nil {
			return nil, nil, fmt.Errorf("serialize entra service principal asset %s: %w", externalID, err)
		}

		assetRows = append(assetRows, appAssetUpsertRow{
			AssetKind:        "entra_service_principal",
			ExternalID:       externalID,
			ParentExternalID: stringValue(sp.GetAppId()),
			DisplayName:      displayName,
			Status:           entraAccountStatus(sp.GetAccountEnabled()),
			CreatedAtSource:  parseGraphTime(servicePrincipalCreatedDateTime(sp)),
			UpdatedAtSource:  pgtype.Timestamptz{},
			RawJSON:          registry.NormalizeJSON(rawJSON),
		})

		assetRefExternalID := appAssetRefExternalID("entra_service_principal", externalID)
		for _, credential := range sp.GetPasswordCredentials() {
			row, err := buildEntraPasswordCredentialRow("entra_service_principal", externalID, assetRefExternalID, credential)
			if err != nil {
				return nil, nil, err
			}
			credentialRows = append(credentialRows, row)
		}
		for _, credential := range sp.GetKeyCredentials() {
			row, err := buildEntraCertificateCredentialRow("entra_service_principal", externalID, assetRefExternalID, credential)
			if err != nil {
				return nil, nil, err
			}
			credentialRows = append(credentialRows, row)
		}
	}

	return assetRows, credentialRows, nil
}

func buildEntraPasswordCredentialRow(assetKind, assetExternalID, assetRefExternalID string, credential PasswordCredential) (credentialArtifactUpsertRow, error) {
	createdAt := parseGraphTime(timeValueString(credential.GetStartDateTime()))
	expiresAt := parseGraphTime(timeValueString(credential.GetEndDateTime()))
	externalID := uuidValueString(credential.GetKeyId())
	if externalID == "" {
		externalID = syntheticCredentialExternalID("entra_client_secret", assetExternalID,
			stringValue(credential.GetDisplayName()),
			timeValueString(credential.GetStartDateTime()),
			timeValueString(credential.GetEndDateTime()),
			stringValue(credential.GetHint()),
		)
	}
	displayName := stringValue(credential.GetDisplayName())
	if displayName == "" {
		displayName = externalID
	}
	fingerprint := uuidValueString(credential.GetKeyId())
	if fingerprint == "" {
		fingerprint = stringValue(credential.GetHint())
	}

	rawJSON, err := serializeSDKModel(credential)
	if err != nil {
		return credentialArtifactUpsertRow{}, fmt.Errorf("serialize entra password credential %s for %s %s: %w", externalID, assetKind, assetExternalID, err)
	}

	return credentialArtifactUpsertRow{
		AssetRefKind:       "app_asset",
		AssetRefExternalID: assetRefExternalID,
		CredentialKind:     "entra_client_secret",
		ExternalID:         externalID,
		DisplayName:        displayName,
		Fingerprint:        fingerprint,
		ScopeJSON: registry.MarshalJSON(map[string]string{
			"asset_kind":        assetKind,
			"asset_external_id": assetExternalID,
		}),
		Status:          credentialLifecycleStatus(createdAt, expiresAt),
		CreatedAtSource: createdAt,
		ExpiresAtSource: expiresAt,
		RawJSON:         registry.NormalizeJSON(rawJSON),
	}, nil
}

func buildEntraCertificateCredentialRow(assetKind, assetExternalID, assetRefExternalID string, credential KeyCredential) (credentialArtifactUpsertRow, error) {
	createdAt := parseGraphTime(timeValueString(credential.GetStartDateTime()))
	expiresAt := parseGraphTime(timeValueString(credential.GetEndDateTime()))
	externalID := uuidValueString(credential.GetKeyId())
	if externalID == "" {
		externalID = syntheticCredentialExternalID("entra_certificate", assetExternalID,
			stringValue(credential.GetDisplayName()),
			timeValueString(credential.GetStartDateTime()),
			timeValueString(credential.GetEndDateTime()),
			stringValue(credential.GetTypeEscaped()),
			stringValue(credential.GetUsage()),
			bytesValueString(credential.GetCustomKeyIdentifier()),
		)
	}
	displayName := stringValue(credential.GetDisplayName())
	if displayName == "" {
		displayName = externalID
	}
	fingerprint := bytesValueString(credential.GetCustomKeyIdentifier())
	if fingerprint == "" {
		fingerprint = uuidValueString(credential.GetKeyId())
	}

	rawJSON, err := serializeSDKModel(credential)
	if err != nil {
		return credentialArtifactUpsertRow{}, fmt.Errorf("serialize entra certificate credential %s for %s %s: %w", externalID, assetKind, assetExternalID, err)
	}

	return credentialArtifactUpsertRow{
		AssetRefKind:       "app_asset",
		AssetRefExternalID: assetRefExternalID,
		CredentialKind:     "entra_certificate",
		ExternalID:         externalID,
		DisplayName:        displayName,
		Fingerprint:        fingerprint,
		ScopeJSON: registry.MarshalJSON(map[string]string{
			"asset_kind":        assetKind,
			"asset_external_id": assetExternalID,
		}),
		Status:          credentialLifecycleStatus(createdAt, expiresAt),
		CreatedAtSource: createdAt,
		ExpiresAtSource: expiresAt,
		RawJSON:         registry.NormalizeJSON(rawJSON),
	}, nil
}

func (i *EntraIntegration) collectAppAssetOwners(ctx context.Context, report func(registry.Event), applications []Application, servicePrincipals []ServicePrincipal) ([]appAssetOwnerUpsertRow, error) {
	totalAssets := len(applications) + len(servicePrincipals)
	report(registry.Event{Source: "entra", Stage: "list-owners", Current: 0, Total: int64(totalAssets), Message: fmt.Sprintf("listing owners for %d app assets", totalAssets)})

	rows := make([]appAssetOwnerUpsertRow, 0)
	processed := 0

	for _, app := range applications {
		assetExternalID := entityID(app)
		if assetExternalID == "" {
			processed++
			continue
		}

		owners, err := i.client.ListApplicationOwners(ctx, assetExternalID)
		if err != nil {
			return nil, fmt.Errorf("entra application owners %s: %w", assetExternalID, err)
		}
		ownerRows, err := buildOwnerRows("entra_application", assetExternalID, owners)
		if err != nil {
			return nil, err
		}
		rows = append(rows, ownerRows...)
		processed++
		report(registry.Event{Source: "entra", Stage: "list-owners", Current: int64(processed), Total: int64(totalAssets), Message: fmt.Sprintf("owners for assets %d/%d", processed, totalAssets)})
	}

	for _, sp := range servicePrincipals {
		assetExternalID := entityID(sp)
		if assetExternalID == "" {
			processed++
			continue
		}

		owners, err := i.client.ListServicePrincipalOwners(ctx, assetExternalID)
		if err != nil {
			return nil, fmt.Errorf("entra service principal owners %s: %w", assetExternalID, err)
		}
		ownerRows, err := buildOwnerRows("entra_service_principal", assetExternalID, owners)
		if err != nil {
			return nil, err
		}
		rows = append(rows, ownerRows...)
		processed++
		report(registry.Event{Source: "entra", Stage: "list-owners", Current: int64(processed), Total: int64(totalAssets), Message: fmt.Sprintf("owners for assets %d/%d", processed, totalAssets)})
	}

	return rows, nil
}

func buildOwnerRows(assetKind, assetExternalID string, owners []DirectoryOwner) ([]appAssetOwnerUpsertRow, error) {
	rows := make([]appAssetOwnerUpsertRow, 0, len(owners))
	for _, owner := range owners {
		ownerExternalID := entraOwnerExternalID(owner)
		if ownerExternalID == "" {
			continue
		}

		ownerDisplayName := entraOwnerDisplayName(owner)
		if ownerDisplayName == "" {
			ownerDisplayName = ownerExternalID
		}
		ownerEmail := normalizeEmail(entraOwnerMail(owner))
		if ownerEmail == "" {
			ownerEmail = normalizeEmail(entraOwnerUserPrincipalName(owner))
		}

		rawJSON, err := serializeSDKModel(owner)
		if err != nil {
			return nil, fmt.Errorf("serialize entra owner %s for %s %s: %w", ownerExternalID, assetKind, assetExternalID, err)
		}

		rows = append(rows, appAssetOwnerUpsertRow{
			AssetKind:        assetKind,
			AssetExternalID:  assetExternalID,
			OwnerKind:        entraOwnerKind(stringValue(owner.GetOdataType())),
			OwnerExternalID:  ownerExternalID,
			OwnerDisplayName: ownerDisplayName,
			OwnerEmail:       ownerEmail,
			RawJSON:          registry.NormalizeJSON(rawJSON),
		})
	}
	return rows, nil
}

func (i *EntraIntegration) upsertAppAssets(ctx context.Context, q *gen.Queries, report func(registry.Event), runID int64, rows []appAssetUpsertRow) error {
	report(registry.Event{Source: "entra", Stage: "write-app-assets", Current: 0, Total: int64(len(rows)), Message: fmt.Sprintf("writing %d app assets", len(rows))})
	if len(rows) == 0 {
		return nil
	}

	for start := 0; start < len(rows); start += entraAppAssetBatchSize {
		end := min(start+entraAppAssetBatchSize, len(rows))
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
			SourceKind:        "entra",
			SourceName:        i.tenantID,
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
			return err
		}

		report(registry.Event{Source: "entra", Stage: "write-app-assets", Current: int64(end), Total: int64(len(rows)), Message: fmt.Sprintf("app assets %d/%d", end, len(rows))})
	}

	return nil
}

func (i *EntraIntegration) upsertAppAssetOwners(ctx context.Context, q *gen.Queries, report func(registry.Event), runID int64, rows []appAssetOwnerUpsertRow) error {
	report(registry.Event{Source: "entra", Stage: "write-owners", Current: 0, Total: int64(len(rows)), Message: fmt.Sprintf("writing %d owner rows", len(rows))})
	if len(rows) == 0 {
		return nil
	}

	for start := 0; start < len(rows); start += entraOwnerBatchSize {
		end := min(start+entraOwnerBatchSize, len(rows))
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
			SeenInRunID:       runID,
			SourceKind:        "entra",
			SourceName:        i.tenantID,
			AssetKinds:        assetKinds,
			AssetExternalIds:  assetExternalIDs,
			OwnerKinds:        ownerKinds,
			OwnerExternalIds:  ownerExternalIDs,
			OwnerDisplayNames: ownerDisplayNames,
			OwnerEmails:       ownerEmails,
			RawJsons:          rawJSONs,
		}); err != nil {
			return err
		}

		report(registry.Event{Source: "entra", Stage: "write-owners", Current: int64(end), Total: int64(len(rows)), Message: fmt.Sprintf("owners %d/%d", end, len(rows))})
	}

	return nil
}

func (i *EntraIntegration) upsertCredentialArtifacts(ctx context.Context, q *gen.Queries, report func(registry.Event), runID int64, rows []credentialArtifactUpsertRow) error {
	report(registry.Event{Source: "entra", Stage: "write-credentials", Current: 0, Total: int64(len(rows)), Message: fmt.Sprintf("writing %d credential rows", len(rows))})
	if len(rows) == 0 {
		return nil
	}

	for start := 0; start < len(rows); start += entraCredentialBatchSize {
		end := min(start+entraCredentialBatchSize, len(rows))
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
			createdByKinds = append(createdByKinds, "")
			createdByExternalIDs = append(createdByExternalIDs, "")
			createdByDisplayNames = append(createdByDisplayNames, "")
			approvedByKinds = append(approvedByKinds, "")
			approvedByExternalIDs = append(approvedByExternalIDs, "")
			approvedByDisplayNames = append(approvedByDisplayNames, "")
			rawJSONs = append(rawJSONs, row.RawJSON)
		}

		if _, err := q.UpsertCredentialArtifactsBulkBySource(ctx, gen.UpsertCredentialArtifactsBulkBySourceParams{
			SourceKind:             "entra",
			SourceName:             i.tenantID,
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
			return err
		}

		report(registry.Event{Source: "entra", Stage: "write-credentials", Current: int64(end), Total: int64(len(rows)), Message: fmt.Sprintf("credentials %d/%d", end, len(rows))})
	}

	return nil
}

func buildCredentialAuditEventRows(events []DirectoryAuditEvent) ([]credentialAuditEventUpsertRow, error) {
	rows := make([]credentialAuditEventUpsertRow, 0, len(events))
	for _, event := range events {
		if !isEntraGovernanceAuditEvent(event) {
			continue
		}

		eventTime := parseGraphTime(timeValueString(event.GetActivityDateTime()))
		if !eventTime.Valid {
			continue
		}

		eventExternalID := entityID(event)
		if eventExternalID == "" {
			eventExternalID = syntheticCredentialExternalID(
				"entra_audit_event",
				timeValueString(event.GetActivityDateTime()),
				stringValue(event.GetCategory()),
				stringValue(event.GetActivityDisplayName()),
				directoryAuditResultString(event),
			)
		}

		eventType := stringValue(event.GetActivityDisplayName())
		if eventType == "" {
			eventType = stringValue(event.GetCategory())
		}
		if eventType == "" {
			eventType = "entra_directory_audit"
		}

		actorKind, actorExternalID, actorDisplayName := entraAuditActor(event.GetInitiatedBy())
		targetKind, targetExternalID, targetDisplayName := entraAuditTarget(event.GetTargetResources())
		credentialKind, credentialExternalID := entraAuditCredential(event)

		rawJSON, err := serializeSDKModel(event)
		if err != nil {
			return nil, fmt.Errorf("serialize entra directory audit event %s: %w", eventExternalID, err)
		}

		rows = append(rows, credentialAuditEventUpsertRow{
			EventExternalID:      eventExternalID,
			EventType:            eventType,
			EventTime:            eventTime,
			ActorKind:            actorKind,
			ActorExternalID:      actorExternalID,
			ActorDisplayName:     actorDisplayName,
			TargetKind:           targetKind,
			TargetExternalID:     targetExternalID,
			TargetDisplayName:    targetDisplayName,
			CredentialKind:       credentialKind,
			CredentialExternalID: credentialExternalID,
			RawJSON:              registry.NormalizeJSON(rawJSON),
		})
	}
	return rows, nil
}

func isEntraGovernanceAuditEvent(event DirectoryAuditEvent) bool {
	category := strings.ToLower(stringValue(event.GetCategory()))
	if strings.Contains(category, "application") || strings.Contains(category, "serviceprincipal") {
		return true
	}

	activity := strings.ToLower(stringValue(event.GetActivityDisplayName()))
	for _, keyword := range []string{"credential", "certificate", "secret", "application", "service principal", "owner"} {
		if strings.Contains(activity, keyword) {
			return true
		}
	}

	for _, target := range event.GetTargetResources() {
		targetType := strings.ToLower(stringValue(target.GetTypeEscaped()))
		if strings.Contains(targetType, "application") || strings.Contains(targetType, "serviceprincipal") {
			return true
		}
	}
	return false
}

func entraAuditActor(initiatedBy DirectoryAuditInitiatedBy) (string, string, string) {
	if initiatedBy == nil {
		return "unknown", "", ""
	}

	if initiatedBy.GetUser() != nil {
		externalID := stringValue(initiatedBy.GetUser().GetId())
		if externalID == "" {
			externalID = normalizeEmail(stringValue(initiatedBy.GetUser().GetUserPrincipalName()))
		}
		displayName := stringValue(initiatedBy.GetUser().GetDisplayName())
		if displayName == "" {
			displayName = stringValue(initiatedBy.GetUser().GetUserPrincipalName())
		}
		if displayName == "" {
			displayName = externalID
		}
		return "entra_user", externalID, displayName
	}

	if initiatedBy.GetApp() != nil {
		externalID := stringValue(initiatedBy.GetApp().GetServicePrincipalId())
		if externalID == "" {
			externalID = stringValue(initiatedBy.GetApp().GetAppId())
		}
		displayName := stringValue(initiatedBy.GetApp().GetDisplayName())
		if displayName == "" {
			displayName = externalID
		}
		return "entra_service_principal", externalID, displayName
	}

	return "unknown", "", ""
}

func entraAuditTarget(targets []DirectoryAuditTargetResource) (string, string, string) {
	for _, target := range targets {
		targetExternalID := stringValue(target.GetId())
		targetDisplayName := stringValue(target.GetDisplayName())
		targetType := stringValue(target.GetTypeEscaped())
		if targetExternalID == "" && targetDisplayName == "" && targetType == "" {
			continue
		}
		if targetDisplayName == "" {
			targetDisplayName = targetExternalID
		}
		return entraAuditTargetKind(targetType), targetExternalID, targetDisplayName
	}
	return "unknown", "", ""
}

func entraAuditTargetKind(targetType string) string {
	targetType = strings.ToLower(strings.TrimSpace(targetType))
	switch {
	case strings.Contains(targetType, "application"):
		return "entra_application"
	case strings.Contains(targetType, "serviceprincipal"):
		return "entra_service_principal"
	case strings.Contains(targetType, "user"):
		return "entra_user"
	default:
		return "unknown"
	}
}

func entraAuditCredential(event DirectoryAuditEvent) (string, string) {
	kind := entraAuditCredentialKind(stringValue(event.GetActivityDisplayName()), stringValue(event.GetCategory()))
	externalID := ""

	for _, target := range event.GetTargetResources() {
		for _, prop := range target.GetModifiedProperties() {
			if id := extractCredentialExternalID(stringValue(prop.GetNewValue())); id != "" {
				externalID = id
				break
			}
			if id := extractCredentialExternalID(stringValue(prop.GetOldValue())); id != "" {
				externalID = id
				break
			}
		}
		if externalID != "" {
			break
		}
	}

	return kind, externalID
}

func entraAuditCredentialKind(activityDisplayName, category string) string {
	raw := strings.ToLower(strings.TrimSpace(activityDisplayName + " " + category))
	switch {
	case strings.Contains(raw, "password credential"), strings.Contains(raw, "client secret"), strings.Contains(raw, "secret credential"):
		return "entra_client_secret"
	case strings.Contains(raw, "key credential"), strings.Contains(raw, "certificate credential"), strings.Contains(raw, "certificate"):
		return "entra_certificate"
	default:
		return ""
	}
}

func extractCredentialExternalID(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.EqualFold(raw, "null") {
		return ""
	}

	var parsed any
	if err := json.Unmarshal([]byte(raw), &parsed); err == nil {
		if id := extractCredentialExternalIDFromValue(parsed); id != "" {
			return id
		}
	}

	if match := credentialGUIDPattern.FindString(raw); match != "" {
		return strings.TrimSpace(match)
	}

	return ""
}

func extractCredentialExternalIDFromValue(v any) string {
	switch typed := v.(type) {
	case map[string]any:
		for _, key := range []string{"keyId", "key_id", "keyIdentifier", "credentialId"} {
			if candidate, ok := typed[key]; ok {
				if value := strings.TrimSpace(fmt.Sprint(candidate)); value != "" && value != "<nil>" {
					return value
				}
			}
		}
		for _, candidate := range typed {
			if id := extractCredentialExternalIDFromValue(candidate); id != "" {
				return id
			}
		}
	case []any:
		for _, candidate := range typed {
			if id := extractCredentialExternalIDFromValue(candidate); id != "" {
				return id
			}
		}
	case string:
		candidate := strings.TrimSpace(typed)
		if candidate == "" {
			return ""
		}
		if match := credentialGUIDPattern.FindString(candidate); match != "" {
			return strings.TrimSpace(match)
		}
		if strings.HasPrefix(candidate, "{") || strings.HasPrefix(candidate, "[") || strings.HasPrefix(candidate, "\"") {
			if id := extractCredentialExternalID(candidate); id != "" {
				return id
			}
		}
	}
	return ""
}

func (i *EntraIntegration) upsertCredentialAuditEvents(ctx context.Context, q *gen.Queries, report func(registry.Event), rows []credentialAuditEventUpsertRow) error {
	report(registry.Event{Source: "entra", Stage: "write-audit-events", Current: 0, Total: int64(len(rows)), Message: fmt.Sprintf("writing %d credential audit events", len(rows))})
	if len(rows) == 0 {
		return nil
	}

	for start := 0; start < len(rows); start += entraAuditEventBatchSize {
		end := min(start+entraAuditEventBatchSize, len(rows))
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
			SourceKind:            "entra",
			SourceName:            i.tenantID,
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
			return err
		}

		report(registry.Event{
			Source:  "entra",
			Stage:   "write-audit-events",
			Current: int64(end),
			Total:   int64(len(rows)),
			Message: fmt.Sprintf("audit events %d/%d", end, len(rows)),
		})
	}

	return nil
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

func (i *EntraIntegration) syncDiscovery(ctx context.Context, q *gen.Queries, report func(registry.Event), runID int64, applications []Application, servicePrincipals []ServicePrincipal) error {
	now := time.Now().UTC()

	report(registry.Event{Source: "entra", Stage: "list-discovery-events", Current: 0, Total: 1, Message: "listing sign-ins and oauth grants"})
	since := now.Add(-7 * 24 * time.Hour)
	latestObservedAt, err := q.GetLatestSaaSDiscoveryObservedAtBySource(ctx, gen.GetLatestSaaSDiscoveryObservedAtBySourceParams{
		SourceKind: "entra",
		SourceName: i.tenantID,
	})
	if err != nil {
		metrics.DiscoveryIngestFailuresTotal.WithLabelValues("entra", "idp_sso", "watermark_query_error").Inc()
		return fmt.Errorf("query latest discovery watermark: %w", err)
	}
	if latestObservedAt.Valid {
		candidate := latestObservedAt.Time.UTC().Add(-15 * time.Minute)
		if candidate.After(since) {
			since = candidate
		}
	}

	signIns, err := i.client.ListSignIns(ctx, &since)
	if err != nil {
		metrics.DiscoveryIngestFailuresTotal.WithLabelValues("entra", "idp_sso", "api_error").Inc()
		return fmt.Errorf("list entra sign-ins: %w", err)
	}
	grants, err := i.client.ListOAuth2PermissionGrants(ctx)
	if err != nil {
		metrics.DiscoveryIngestFailuresTotal.WithLabelValues("entra", "oauth_grant", "api_error").Inc()
		return fmt.Errorf("list oauth2 permission grants: %w", err)
	}
	users := i.resolveGrantActors(ctx, report, grants)
	report(registry.Event{
		Source:  "entra",
		Stage:   "list-discovery-events",
		Current: 1,
		Total:   1,
		Message: fmt.Sprintf("found %d sign-ins and %d oauth grants", len(signIns), len(grants)),
	})

	report(registry.Event{Source: "entra", Stage: "normalize-discovery", Current: 0, Total: 1, Message: "normalizing discovery evidence"})
	sources, events, err := normalizeEntraDiscovery(signIns, grants, applications, servicePrincipals, users, i.tenantID, now)
	if err != nil {
		return fmt.Errorf("normalize entra discovery evidence: %w", err)
	}
	report(registry.Event{
		Source:  "entra",
		Stage:   "normalize-discovery",
		Current: 1,
		Total:   1,
		Message: fmt.Sprintf("normalized %d source rows and %d events", len(sources), len(events)),
	})

	if err := i.writeDiscoveryRows(ctx, q, report, runID, sources, events); err != nil {
		metrics.DiscoveryIngestFailuresTotal.WithLabelValues("entra", "idp_sso", "db_error").Inc()
		return err
	}
	if err := i.seedEntraAutoBindings(ctx, q); err != nil {
		return err
	}
	return nil
}

func (i *EntraIntegration) resolveGrantActors(ctx context.Context, report func(registry.Event), grants []OAuth2PermissionGrant) []User {
	actorIDs := make([]string, 0, len(grants))
	for _, grant := range grants {
		actorIDs = append(actorIDs, stringValue(grant.GetPrincipalId()))
	}
	actorIDs = distinctNonEmptyStrings(actorIDs)
	if len(actorIDs) == 0 {
		return nil
	}

	report(registry.Event{
		Source:  "entra",
		Stage:   "resolve-grant-actors",
		Current: 0,
		Total:   1,
		Message: fmt.Sprintf("resolving %d Entra grant actors", len(actorIDs)),
	})
	users, err := i.client.LookupUsersByIDs(ctx, actorIDs)
	if err != nil {
		report(registry.Event{
			Source:  "entra",
			Stage:   "resolve-grant-actors",
			Current: 1,
			Total:   1,
			Message: "skipping grant actor enrichment after lookup failure; continuing with principal ids",
			Err:     err,
		})
		return nil
	}
	report(registry.Event{
		Source:  "entra",
		Stage:   "resolve-grant-actors",
		Current: 1,
		Total:   1,
		Message: fmt.Sprintf("resolved %d Entra grant actors", len(users)),
	})
	return users
}

func normalizeEntraDiscovery(signIns []SignInEvent, grants []OAuth2PermissionGrant, applications []Application, servicePrincipals []ServicePrincipal, users []User, tenantID string, now time.Time) ([]normalizedDiscoverySource, []normalizedDiscoveryEvent, error) {
	sourceByID := map[string]normalizedDiscoverySource{}
	events := make([]normalizedDiscoveryEvent, 0, len(signIns)+len(grants))

	appDisplayByAppID := make(map[string]string, len(applications))
	appVendorByAppID := make(map[string]string, len(applications))
	for _, app := range applications {
		appID := stringValue(app.GetAppId())
		if appID == "" {
			continue
		}
		name := stringValue(app.GetDisplayName())
		if name == "" {
			name = appID
		}
		appDisplayByAppID[appID] = name

		vendorName := ""
		if app.GetVerifiedPublisher() != nil {
			vendorName = stringValue(app.GetVerifiedPublisher().GetDisplayName())
		}
		if vendorName == "" {
			vendorName = discovery.VendorLabelFromDomain(stringValue(app.GetPublisherDomain()))
		}
		if vendorName != "" {
			appVendorByAppID[appID] = vendorName
		}
	}

	servicePrincipalByID := make(map[string]ServicePrincipal, len(servicePrincipals))
	servicePrincipalVendorByAppID := make(map[string]string, len(servicePrincipals))
	for _, servicePrincipal := range servicePrincipals {
		spID := entityID(servicePrincipal)
		if spID == "" {
			continue
		}
		servicePrincipalByID[spID] = servicePrincipal
		appID := stringValue(servicePrincipal.GetAppId())
		if appID == "" {
			continue
		}
		vendorName := servicePrincipalVendorName(servicePrincipal)
		if vendorName == "" {
			continue
		}
		if _, exists := servicePrincipalVendorByAppID[appID]; !exists {
			servicePrincipalVendorByAppID[appID] = vendorName
		}
	}

	userByID := make(map[string]User, len(users))
	for _, user := range users {
		userID := entityID(user)
		if userID == "" {
			continue
		}
		userByID[userID] = user
	}

	for _, signIn := range signIns {
		sourceAppID := stringValue(signIn.GetAppId())
		if sourceAppID == "" {
			sourceAppID = stringValue(signIn.GetAppDisplayName())
		}
		if sourceAppID == "" {
			continue
		}

		sourceAppName := stringValue(signIn.GetAppDisplayName())
		if sourceAppName == "" {
			sourceAppName = appDisplayByAppID[sourceAppID]
		}
		if sourceAppName == "" {
			sourceAppName = sourceAppID
		}

		observedAt := graphObservedAtOrNow(timeValueString(signIn.GetCreatedDateTime()), now)
		sourceVendorName := appVendorByAppID[sourceAppID]
		if sourceVendorName == "" {
			sourceVendorName = servicePrincipalVendorByAppID[sourceAppID]
		}

		metadata := discovery.BuildMetadata(discovery.CanonicalInput{
			SourceKind:       "entra",
			SourceName:       tenantID,
			SourceAppID:      sourceAppID,
			SourceAppName:    sourceAppName,
			SourceDomain:     "",
			SourceVendorName: sourceVendorName,
			EntraAppID:       stringValue(signIn.GetAppId()),
		})

		current := sourceByID[sourceAppID]
		if current.SourceAppID == "" || observedAt.After(current.SeenAt) {
			sourceByID[sourceAppID] = normalizedDiscoverySource{
				CanonicalKey:     metadata.CanonicalKey,
				SourceAppID:      sourceAppID,
				SourceAppName:    sourceAppName,
				SourceAppDomain:  metadata.Domain,
				SourceVendorName: metadata.VendorName,
				SeenAt:           observedAt,
			}
		}

		eventExternalID := entityID(signIn)
		if eventExternalID == "" {
			eventExternalID = fmt.Sprintf("signin:%s:%s:%s", sourceAppID, stringValue(signIn.GetUserId()), observedAt.Format(time.RFC3339Nano))
		}

		rawJSON, err := serializeSDKModel(signIn)
		if err != nil {
			return nil, nil, fmt.Errorf("serialize entra sign-in %s: %w", eventExternalID, err)
		}

		events = append(events, normalizedDiscoveryEvent{
			CanonicalKey:     metadata.CanonicalKey,
			SignalKind:       discovery.SignalKindIDPSSO,
			EventExternalID:  eventExternalID,
			SourceAppID:      sourceAppID,
			SourceAppName:    sourceAppName,
			SourceAppDomain:  metadata.Domain,
			SourceVendorName: metadata.VendorName,
			ActorExternalID:  stringValue(signIn.GetUserId()),
			ActorEmail:       normalizeEmail(stringValue(signIn.GetUserPrincipalName())),
			ActorDisplayName: stringValue(signIn.GetUserDisplayName()),
			ObservedAt:       observedAt,
			Scopes:           nil,
			RawJSON:          registry.NormalizeJSON(rawJSON),
		})
	}

	for _, grant := range grants {
		spID := stringValue(grant.GetClientId())
		servicePrincipal := servicePrincipalByID[spID]

		entraAppID := stringValue(servicePrincipal.GetAppId())
		if entraAppID == "" {
			entraAppID = stringValue(grant.GetClientId())
		}
		sourceAppID := entraAppID
		if sourceAppID == "" {
			sourceAppID = stringValue(grant.GetClientId())
		}
		if sourceAppID == "" {
			continue
		}

		sourceAppName := stringValue(servicePrincipal.GetDisplayName())
		if sourceAppName == "" {
			sourceAppName = appDisplayByAppID[entraAppID]
		}
		if sourceAppName == "" {
			sourceAppName = sourceAppID
		}

		observedAt := now
		scopes := discovery.NormalizeScopes(strings.Fields(strings.ReplaceAll(stringValue(grant.GetScope()), ",", " ")))
		sourceVendorName := appVendorByAppID[entraAppID]
		if sourceVendorName == "" {
			sourceVendorName = servicePrincipalVendorName(servicePrincipal)
		}
		if sourceVendorName == "" {
			sourceVendorName = servicePrincipalVendorByAppID[entraAppID]
		}

		metadata := discovery.BuildMetadata(discovery.CanonicalInput{
			SourceKind:       "entra",
			SourceName:       tenantID,
			SourceAppID:      sourceAppID,
			SourceAppName:    sourceAppName,
			SourceDomain:     "",
			SourceVendorName: sourceVendorName,
			EntraAppID:       entraAppID,
		})

		current := sourceByID[sourceAppID]
		if current.SourceAppID == "" || observedAt.After(current.SeenAt) {
			sourceByID[sourceAppID] = normalizedDiscoverySource{
				CanonicalKey:     metadata.CanonicalKey,
				SourceAppID:      sourceAppID,
				SourceAppName:    sourceAppName,
				SourceAppDomain:  metadata.Domain,
				SourceVendorName: metadata.VendorName,
				SeenAt:           observedAt,
			}
		}

		eventExternalID := entityID(grant)
		if eventExternalID == "" {
			eventExternalID = fmt.Sprintf("grant:%s:%s:%s:%s", sourceAppID, stringValue(grant.GetPrincipalId()), stringValue(grant.GetScope()), observedAt.Format(time.RFC3339Nano))
		}

		actorExternalID := stringValue(grant.GetPrincipalId())
		actorEmail := ""
		actorDisplayName := ""
		if actorExternalID != "" {
			if user, ok := userByID[actorExternalID]; ok {
				actorEmail = normalizeEmail(preferredEmail(user))
				actorDisplayName = stringValue(user.GetDisplayName())
				if actorDisplayName == "" {
					actorDisplayName = actorEmail
				}
			} else if servicePrincipal, ok := servicePrincipalByID[actorExternalID]; ok {
				actorDisplayName = stringValue(servicePrincipal.GetDisplayName())
				if actorDisplayName == "" {
					actorDisplayName = stringValue(servicePrincipal.GetAppId())
				}
			}
		}
		if actorDisplayName == "" && strings.EqualFold(stringValue(grant.GetConsentType()), "AllPrincipals") {
			actorDisplayName = "All principals"
		}
		if actorDisplayName == "" {
			actorDisplayName = actorExternalID
		}

		rawJSON, err := serializeSDKModel(grant)
		if err != nil {
			return nil, nil, fmt.Errorf("serialize entra oauth grant %s: %w", eventExternalID, err)
		}

		events = append(events, normalizedDiscoveryEvent{
			CanonicalKey:     metadata.CanonicalKey,
			SignalKind:       discovery.SignalKindOAuth,
			EventExternalID:  eventExternalID,
			SourceAppID:      sourceAppID,
			SourceAppName:    sourceAppName,
			SourceAppDomain:  metadata.Domain,
			SourceVendorName: metadata.VendorName,
			ActorExternalID:  actorExternalID,
			ActorEmail:       actorEmail,
			ActorDisplayName: actorDisplayName,
			ObservedAt:       observedAt,
			Scopes:           scopes,
			RawJSON:          registry.NormalizeJSON(rawJSON),
		})
	}

	sourceRows := make([]normalizedDiscoverySource, 0, len(sourceByID))
	for _, sourceRow := range sourceByID {
		sourceRows = append(sourceRows, sourceRow)
	}
	return sourceRows, events, nil
}

func (i *EntraIntegration) writeDiscoveryRows(ctx context.Context, q *gen.Queries, report func(registry.Event), runID int64, sources []normalizedDiscoverySource, events []normalizedDiscoveryEvent) error {
	total := len(sources) + len(events)
	report(registry.Event{
		Source:  "entra",
		Stage:   "write-discovery",
		Current: 0,
		Total:   int64(total),
		Message: fmt.Sprintf("writing %d discovery records", total),
	})

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
			SourceKind:       "entra",
			SourceName:       i.tenantID,
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
			SourceKind:       "entra",
			SourceName:       i.tenantID,
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
			SourceKind:       "entra",
			SourceName:       i.tenantID,
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
		report(registry.Event{
			Source:  "entra",
			Stage:   "write-discovery",
			Current: int64(written),
			Total:   int64(total),
			Message: fmt.Sprintf("sources %d/%d", written, total),
		})
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
			SourceKind:        "entra",
			SourceName:        i.tenantID,
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
			metrics.DiscoveryEventsIngestedTotal.WithLabelValues("entra", signalKind).Add(float64(count))
		}
		written += len(events)
		report(registry.Event{
			Source:  "entra",
			Stage:   "write-discovery",
			Current: int64(written),
			Total:   int64(total),
			Message: fmt.Sprintf("events %d/%d", written, total),
		})
	}

	if written == 0 {
		report(registry.Event{
			Source:  "entra",
			Stage:   "write-discovery",
			Current: 0,
			Total:   0,
			Message: "no discovery records to write",
		})
	}
	return nil
}

func (i *EntraIntegration) seedEntraAutoBindings(ctx context.Context, q *gen.Queries) error {
	appIDs, err := q.ListEntraDiscoveryAppIDsWithManagedAssetsBySource(ctx, i.tenantID)
	if err != nil {
		return fmt.Errorf("list entra discovery auto-bind candidates: %w", err)
	}
	for _, appID := range appIDs {
		if err := q.UpsertSaaSAppBinding(ctx, gen.UpsertSaaSAppBindingParams{
			SaasAppID:           appID,
			ConnectorKind:       "entra",
			ConnectorSourceName: i.tenantID,
			BindingSource:       "auto",
			Confidence:          0.8,
			IsPrimary:           false,
			CreatedByAuthUserID: pgtype.Int8{},
		}); err != nil {
			return fmt.Errorf("upsert entra auto binding for app %d: %w", appID, err)
		}
	}
	if len(appIDs) > 0 {
		if _, err := q.RecomputePrimarySaaSAppBindingsForAll(ctx); err != nil {
			return fmt.Errorf("recompute primary bindings: %w", err)
		}
	}
	return nil
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

func directoryAuditResultString(event DirectoryAuditEvent) string {
	if event.GetResult() == nil {
		return ""
	}
	return strings.TrimSpace((*event.GetResult()).String())
}

func parseGraphTime(raw string) pgtype.Timestamptz {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return pgtype.Timestamptz{}
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		parsed, err = time.Parse(time.RFC3339, raw)
		if err != nil {
			return pgtype.Timestamptz{}
		}
	}
	return pgtype.Timestamptz{Time: parsed, Valid: true}
}

func graphObservedAtOrNow(raw string, fallback time.Time) time.Time {
	parsed := parseGraphTime(raw)
	if !parsed.Valid {
		return fallback.UTC()
	}
	return parsed.Time.UTC()
}

func credentialLifecycleStatus(start, end pgtype.Timestamptz) string {
	now := time.Now().UTC()
	if end.Valid && end.Time.UTC().Before(now) {
		return "expired"
	}
	if start.Valid && start.Time.UTC().After(now) {
		return "inactive"
	}
	return "active"
}

func syntheticCredentialExternalID(prefix, assetExternalID string, fields ...string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(strings.TrimSpace(prefix)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strings.TrimSpace(assetExternalID)))
	for _, field := range fields {
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(strings.TrimSpace(field)))
	}
	return fmt.Sprintf("%s:%s:%x", strings.TrimSpace(prefix), strings.TrimSpace(assetExternalID), h.Sum64())
}

func servicePrincipalVendorName(servicePrincipal ServicePrincipal) string {
	if servicePrincipal == nil {
		return ""
	}

	verifiedPublisherName := ""
	if servicePrincipal.GetVerifiedPublisher() != nil {
		verifiedPublisherName = stringValue(servicePrincipal.GetVerifiedPublisher().GetDisplayName())
	}

	return firstNonEmptyTrimmed(verifiedPublisherName, entityAdditionalString(servicePrincipal, "publisherName"))
}

func servicePrincipalCreatedDateTime(servicePrincipal ServicePrincipal) string {
	return entityAdditionalTimeString(servicePrincipal, "createdDateTime")
}

func entityAdditionalString(entity msgraphmodels.Entityable, key string) string {
	if entity == nil {
		return ""
	}

	value, ok := entity.GetAdditionalData()[key]
	if !ok {
		return ""
	}

	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case *string:
		return stringValue(typed)
	case []byte:
		return strings.TrimSpace(string(typed))
	default:
		return strings.TrimSpace(fmt.Sprint(typed))
	}
}

func entityAdditionalTimeString(entity msgraphmodels.Entityable, key string) string {
	if entity == nil {
		return ""
	}

	value, ok := entity.GetAdditionalData()[key]
	if !ok {
		return ""
	}

	switch typed := value.(type) {
	case time.Time:
		return typed.UTC().Format(time.RFC3339)
	case *time.Time:
		return timeValueString(typed)
	case string:
		return strings.TrimSpace(typed)
	case *string:
		return stringValue(typed)
	default:
		return strings.TrimSpace(fmt.Sprint(typed))
	}
}

func entraOwnerKind(odataType string) string {
	t := strings.ToLower(strings.TrimSpace(odataType))
	switch {
	case strings.Contains(t, "user"):
		return "entra_user"
	case strings.Contains(t, "serviceprincipal"):
		return "entra_service_principal"
	default:
		return "unknown"
	}
}

func entraOwnerDisplayName(owner DirectoryOwner) string {
	switch typed := owner.(type) {
	case User:
		return stringValue(typed.GetDisplayName())
	case ServicePrincipal:
		return stringValue(typed.GetDisplayName())
	default:
		return ""
	}
}

func entraOwnerExternalID(owner DirectoryOwner) string {
	switch typed := owner.(type) {
	case ServicePrincipal:
		if id := entityID(typed); id != "" {
			return entraServicePrincipalExternalID(id)
		}
	default:
		if id := entityID(owner); id != "" {
			return id
		}
	}
	return entraOwnerAppID(owner)
}

func entraOwnerMail(owner DirectoryOwner) string {
	if typed, ok := owner.(User); ok {
		return stringValue(typed.GetMail())
	}
	return ""
}

func entraOwnerUserPrincipalName(owner DirectoryOwner) string {
	if typed, ok := owner.(User); ok {
		return stringValue(typed.GetUserPrincipalName())
	}
	return ""
}

func entraOwnerAppID(owner DirectoryOwner) string {
	if typed, ok := owner.(ServicePrincipal); ok {
		return stringValue(typed.GetAppId())
	}
	return ""
}

func entraAccountStatus(accountEnabled *bool) string {
	if accountEnabled == nil {
		return ""
	}
	if *accountEnabled {
		return "Active"
	}
	return "Inactive"
}

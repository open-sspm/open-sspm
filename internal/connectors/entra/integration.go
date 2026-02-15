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
	client           *Client
	tenantID         string
	discoveryEnabled bool
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

func NewEntraIntegration(client *Client, tenantID string, discoveryEnabled bool) *EntraIntegration {
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
		{Source: "entra", Stage: "write-users", Current: 0, Total: registry.UnknownTotal, Message: "writing Entra users"},
		{Source: "entra", Stage: "list-app-assets", Current: 0, Total: 1, Message: "listing Entra applications and service principals"},
		{Source: "entra", Stage: "write-app-assets", Current: 0, Total: registry.UnknownTotal, Message: "writing Entra app assets"},
		{Source: "entra", Stage: "list-owners", Current: 0, Total: registry.UnknownTotal, Message: "listing Entra app owners"},
		{Source: "entra", Stage: "write-owners", Current: 0, Total: registry.UnknownTotal, Message: "writing Entra app owners"},
		{Source: "entra", Stage: "write-credentials", Current: 0, Total: registry.UnknownTotal, Message: "writing Entra credential metadata"},
		{Source: "entra", Stage: "list-audit-events", Current: 0, Total: 1, Message: "listing Entra directory audit events"},
		{Source: "entra", Stage: "write-audit-events", Current: 0, Total: registry.UnknownTotal, Message: "writing Entra credential audit events"},
		{Source: "entra", Stage: "list-discovery-events", Current: 0, Total: 1, Message: "listing Entra discovery signals"},
		{Source: "entra", Stage: "normalize-discovery", Current: 0, Total: 1, Message: "normalizing discovery evidence"},
		{Source: "entra", Stage: "write-discovery", Current: 0, Total: registry.UnknownTotal, Message: "writing discovery data"},
	}
}

func (i *EntraIntegration) Run(ctx context.Context, deps registry.IntegrationDeps) error {
	switch deps.Mode.Normalize() {
	case registry.RunModeDiscovery:
		if !i.SupportsRunMode(registry.RunModeDiscovery) {
			return nil
		}
		return i.runDiscovery(ctx, deps)
	default:
		return i.runFull(ctx, deps)
	}
}

func (i *EntraIntegration) runFull(ctx context.Context, deps registry.IntegrationDeps) error {
	started := time.Now()
	slog.Info("syncing Microsoft Entra ID")

	runID, err := deps.Q.CreateSyncRun(ctx, gen.CreateSyncRunParams{
		SourceKind: registry.SyncRunSourceKind("entra", registry.RunModeFull),
		SourceName: i.tenantID,
	})
	if err != nil {
		return err
	}

	usersWritten, err := i.syncUsers(ctx, deps, runID)
	if err != nil {
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindUnknown)
		return err
	}

	deps.Report(registry.Event{Source: "entra", Stage: "list-app-assets", Current: 0, Total: 1, Message: "listing applications and service principals"})
	applications, err := i.client.ListApplications(ctx)
	if err != nil {
		deps.Report(registry.Event{Source: "entra", Stage: "list-app-assets", Message: err.Error(), Err: err})
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindAPI)
		return err
	}
	servicePrincipals, err := i.client.ListServicePrincipals(ctx)
	if err != nil {
		deps.Report(registry.Event{Source: "entra", Stage: "list-app-assets", Message: err.Error(), Err: err})
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindAPI)
		return err
	}
	deps.Report(registry.Event{
		Source:  "entra",
		Stage:   "list-app-assets",
		Current: 1,
		Total:   1,
		Message: fmt.Sprintf("found %d applications and %d service principals", len(applications), len(servicePrincipals)),
	})

	assetRows, credentialRows := buildEntraAssetAndCredentialRows(applications, servicePrincipals)
	if err := i.upsertAppAssets(ctx, deps, runID, assetRows); err != nil {
		deps.Report(registry.Event{Source: "entra", Stage: "write-app-assets", Message: err.Error(), Err: err})
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
		return err
	}

	ownerRows, err := i.collectAppAssetOwners(ctx, deps, applications, servicePrincipals)
	if err != nil {
		deps.Report(registry.Event{Source: "entra", Stage: "list-owners", Message: err.Error(), Err: err})
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindAPI)
		return err
	}
	if err := i.upsertAppAssetOwners(ctx, deps, runID, ownerRows); err != nil {
		deps.Report(registry.Event{Source: "entra", Stage: "write-owners", Message: err.Error(), Err: err})
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
		return err
	}

	if err := i.upsertCredentialArtifacts(ctx, deps, runID, credentialRows); err != nil {
		deps.Report(registry.Event{Source: "entra", Stage: "write-credentials", Message: err.Error(), Err: err})
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
		return err
	}

	deps.Report(registry.Event{Source: "entra", Stage: "list-audit-events", Current: 0, Total: 1, Message: "listing directory audit events"})
	directoryAudits, err := i.client.ListDirectoryAudits(ctx, nil)
	if err != nil {
		deps.Report(registry.Event{Source: "entra", Stage: "list-audit-events", Message: err.Error(), Err: err})
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindAPI)
		return err
	}
	deps.Report(registry.Event{
		Source:  "entra",
		Stage:   "list-audit-events",
		Current: 1,
		Total:   1,
		Message: fmt.Sprintf("found %d directory audit events", len(directoryAudits)),
	})

	auditEventRows := buildCredentialAuditEventRows(directoryAudits)
	if err := i.upsertCredentialAuditEvents(ctx, deps, auditEventRows); err != nil {
		deps.Report(registry.Event{Source: "entra", Stage: "write-audit-events", Message: err.Error(), Err: err})
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
		return err
	}

	if err := registry.FinalizeAppRun(ctx, deps, runID, "entra", i.tenantID, time.Since(started), false); err != nil {
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
		return err
	}

	slog.Info(
		"entra sync complete",
		"tenant", i.tenantID,
		"users", usersWritten,
		"app_assets", len(assetRows),
		"owners", len(ownerRows),
		"credentials", len(credentialRows),
		"audit_events", len(auditEventRows),
	)
	return nil
}

func (i *EntraIntegration) runDiscovery(ctx context.Context, deps registry.IntegrationDeps) error {
	started := time.Now()
	slog.Info("syncing Microsoft Entra ID discovery")

	runID, err := deps.Q.CreateSyncRun(ctx, gen.CreateSyncRunParams{
		SourceKind: registry.SyncRunSourceKind("entra", registry.RunModeDiscovery),
		SourceName: i.tenantID,
	})
	if err != nil {
		return err
	}

	applications, err := i.client.ListApplications(ctx)
	if err != nil {
		deps.Report(registry.Event{Source: "entra", Stage: "list-app-assets", Message: err.Error(), Err: err})
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindAPI)
		return err
	}
	servicePrincipals, err := i.client.ListServicePrincipals(ctx)
	if err != nil {
		deps.Report(registry.Event{Source: "entra", Stage: "list-app-assets", Message: err.Error(), Err: err})
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindAPI)
		return err
	}

	if err := i.syncDiscovery(ctx, deps, runID, applications, servicePrincipals); err != nil {
		deps.Report(registry.Event{Source: "entra", Stage: "write-discovery", Message: err.Error(), Err: err})
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindUnknown)
		return err
	}
	if err := registry.FinalizeDiscoveryRun(ctx, deps, runID, "entra", i.tenantID, time.Since(started)); err != nil {
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
		return err
	}
	slog.Info("entra discovery sync complete", "tenant", i.tenantID)
	return nil
}

func (i *EntraIntegration) syncUsers(ctx context.Context, deps registry.IntegrationDeps, runID int64) (int, error) {
	users, err := i.client.ListUsers(ctx)
	if err != nil {
		deps.Report(registry.Event{Source: "entra", Stage: "list-users", Message: err.Error(), Err: err})
		return 0, err
	}
	deps.Report(registry.Event{Source: "entra", Stage: "list-users", Current: 1, Total: 1, Message: fmt.Sprintf("found %d users", len(users))})
	deps.Report(registry.Event{Source: "entra", Stage: "write-users", Current: 0, Total: int64(len(users)), Message: fmt.Sprintf("writing %d users", len(users))})

	externalIDs := make([]string, 0, len(users))
	emails := make([]string, 0, len(users))
	displayNames := make([]string, 0, len(users))
	rawJSONs := make([][]byte, 0, len(users))
	lastLoginAts := make([]pgtype.Timestamptz, 0, len(users))
	lastLoginIps := make([]string, 0, len(users))
	lastLoginRegions := make([]string, 0, len(users))

	for _, user := range users {
		externalID := strings.TrimSpace(user.ID)
		if externalID == "" {
			continue
		}

		email := normalizeEmail(preferredEmail(user))

		display := strings.TrimSpace(user.DisplayName)
		if display == "" {
			display = strings.TrimSpace(email)
		}
		if display == "" {
			display = externalID
		}

		raw, err := json.Marshal(sanitizedUser{
			ID:                 externalID,
			DisplayName:        strings.TrimSpace(user.DisplayName),
			Mail:               strings.TrimSpace(user.Mail),
			UserPrincipalName:  strings.TrimSpace(user.UserPrincipalName),
			OtherMails:         user.OtherMails,
			ProxyAddresses:     user.ProxyAddresses,
			UserType:           strings.TrimSpace(user.UserType),
			AccountEnabled:     user.AccountEnabled,
			Status:             entraAccountStatus(user.AccountEnabled),
			CreatedDateTimeRaw: strings.TrimSpace(user.CreatedDateTimeRaw),
		})
		if err != nil {
			return 0, err
		}

		externalIDs = append(externalIDs, externalID)
		emails = append(emails, email)
		displayNames = append(displayNames, display)
		rawJSONs = append(rawJSONs, raw)
		lastLoginAts = append(lastLoginAts, pgtype.Timestamptz{})
		lastLoginIps = append(lastLoginIps, "")
		lastLoginRegions = append(lastLoginRegions, "")
	}

	for start := 0; start < len(externalIDs); start += entraUserBatchSize {
		end := min(start+entraUserBatchSize, len(externalIDs))

		_, err := deps.Q.UpsertAppUsersBulkBySource(ctx, gen.UpsertAppUsersBulkBySourceParams{
			SourceKind:       "entra",
			SourceName:       i.tenantID,
			SeenInRunID:      runID,
			ExternalIds:      externalIDs[start:end],
			Emails:           emails[start:end],
			DisplayNames:     displayNames[start:end],
			RawJsons:         rawJSONs[start:end],
			LastLoginAts:     lastLoginAts[start:end],
			LastLoginIps:     lastLoginIps[start:end],
			LastLoginRegions: lastLoginRegions[start:end],
		})
		if err != nil {
			return 0, err
		}

		deps.Report(registry.Event{
			Source:  "entra",
			Stage:   "write-users",
			Current: int64(end),
			Total:   int64(len(externalIDs)),
			Message: fmt.Sprintf("users %d/%d", end, len(externalIDs)),
		})
	}

	return len(externalIDs), nil
}

func buildEntraAssetAndCredentialRows(applications []Application, servicePrincipals []ServicePrincipal) ([]appAssetUpsertRow, []credentialArtifactUpsertRow) {
	assetRows := make([]appAssetUpsertRow, 0, len(applications)+len(servicePrincipals))
	credentialRows := make([]credentialArtifactUpsertRow, 0)

	for _, app := range applications {
		externalID := strings.TrimSpace(app.ID)
		if externalID == "" {
			continue
		}
		displayName := strings.TrimSpace(app.DisplayName)
		if displayName == "" {
			displayName = externalID
		}

		assetRows = append(assetRows, appAssetUpsertRow{
			AssetKind:        "entra_application",
			ExternalID:       externalID,
			ParentExternalID: "",
			DisplayName:      displayName,
			Status:           "",
			CreatedAtSource:  parseGraphTime(app.CreatedDateTimeRaw),
			UpdatedAtSource:  pgtype.Timestamptz{},
			RawJSON: registry.MarshalJSON(map[string]any{
				"id":                              externalID,
				"app_id":                          strings.TrimSpace(app.AppID),
				"display_name":                    strings.TrimSpace(app.DisplayName),
				"publisher_domain":                strings.TrimSpace(app.PublisherDomain),
				"verified_publisher_display_name": strings.TrimSpace(app.VerifiedPublisher.DisplayName),
				"created_date_time":               strings.TrimSpace(app.CreatedDateTimeRaw),
				"password_credentials":            sanitizePasswordCredentials(app.PasswordCredentials),
				"key_credentials":                 sanitizeKeyCredentials(app.KeyCredentials),
			}),
		})

		assetRefExternalID := appAssetRefExternalID("entra_application", externalID)
		for _, credential := range app.PasswordCredentials {
			credentialRows = append(credentialRows, buildEntraPasswordCredentialRow("entra_application", externalID, assetRefExternalID, credential))
		}
		for _, credential := range app.KeyCredentials {
			credentialRows = append(credentialRows, buildEntraCertificateCredentialRow("entra_application", externalID, assetRefExternalID, credential))
		}
	}

	for _, sp := range servicePrincipals {
		externalID := strings.TrimSpace(sp.ID)
		if externalID == "" {
			continue
		}
		displayName := strings.TrimSpace(sp.DisplayName)
		if displayName == "" {
			displayName = externalID
		}

		assetRows = append(assetRows, appAssetUpsertRow{
			AssetKind:        "entra_service_principal",
			ExternalID:       externalID,
			ParentExternalID: strings.TrimSpace(sp.AppID),
			DisplayName:      displayName,
			Status:           entraAccountStatus(sp.AccountEnabled),
			CreatedAtSource:  parseGraphTime(sp.CreatedDateTimeRaw),
			UpdatedAtSource:  pgtype.Timestamptz{},
			RawJSON: registry.MarshalJSON(map[string]any{
				"id":                     externalID,
				"app_id":                 strings.TrimSpace(sp.AppID),
				"display_name":           strings.TrimSpace(sp.DisplayName),
				"publisher_name":         strings.TrimSpace(sp.PublisherName),
				"account_enabled":        sp.AccountEnabled,
				"service_principal_type": strings.TrimSpace(sp.ServicePrincipalType),
				"created_date_time":      strings.TrimSpace(sp.CreatedDateTimeRaw),
				"password_credentials":   sanitizePasswordCredentials(sp.PasswordCredentials),
				"key_credentials":        sanitizeKeyCredentials(sp.KeyCredentials),
			}),
		})

		assetRefExternalID := appAssetRefExternalID("entra_service_principal", externalID)
		for _, credential := range sp.PasswordCredentials {
			credentialRows = append(credentialRows, buildEntraPasswordCredentialRow("entra_service_principal", externalID, assetRefExternalID, credential))
		}
		for _, credential := range sp.KeyCredentials {
			credentialRows = append(credentialRows, buildEntraCertificateCredentialRow("entra_service_principal", externalID, assetRefExternalID, credential))
		}
	}

	return assetRows, credentialRows
}

func buildEntraPasswordCredentialRow(assetKind, assetExternalID, assetRefExternalID string, credential PasswordCredential) credentialArtifactUpsertRow {
	createdAt := parseGraphTime(credential.StartDateTimeRaw)
	expiresAt := parseGraphTime(credential.EndDateTimeRaw)
	externalID := strings.TrimSpace(credential.KeyID)
	if externalID == "" {
		externalID = syntheticCredentialExternalID("entra_client_secret", assetExternalID,
			credential.DisplayName,
			credential.StartDateTimeRaw,
			credential.EndDateTimeRaw,
			credential.Hint,
		)
	}
	displayName := strings.TrimSpace(credential.DisplayName)
	if displayName == "" {
		displayName = externalID
	}
	fingerprint := strings.TrimSpace(credential.KeyID)
	if fingerprint == "" {
		fingerprint = strings.TrimSpace(credential.Hint)
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
		RawJSON: registry.MarshalJSON(map[string]string{
			"key_id":            strings.TrimSpace(credential.KeyID),
			"display_name":      strings.TrimSpace(credential.DisplayName),
			"start_date_time":   strings.TrimSpace(credential.StartDateTimeRaw),
			"end_date_time":     strings.TrimSpace(credential.EndDateTimeRaw),
			"hint":              strings.TrimSpace(credential.Hint),
			"asset_kind":        assetKind,
			"asset_external_id": assetExternalID,
		}),
	}
}

func buildEntraCertificateCredentialRow(assetKind, assetExternalID, assetRefExternalID string, credential KeyCredential) credentialArtifactUpsertRow {
	createdAt := parseGraphTime(credential.StartDateTimeRaw)
	expiresAt := parseGraphTime(credential.EndDateTimeRaw)
	externalID := strings.TrimSpace(credential.KeyID)
	if externalID == "" {
		externalID = syntheticCredentialExternalID("entra_certificate", assetExternalID,
			credential.DisplayName,
			credential.StartDateTimeRaw,
			credential.EndDateTimeRaw,
			credential.Type,
			credential.Usage,
			credential.CustomKeyIdentifier,
		)
	}
	displayName := strings.TrimSpace(credential.DisplayName)
	if displayName == "" {
		displayName = externalID
	}
	fingerprint := strings.TrimSpace(credential.CustomKeyIdentifier)
	if fingerprint == "" {
		fingerprint = strings.TrimSpace(credential.KeyID)
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
		RawJSON: registry.MarshalJSON(map[string]string{
			"key_id":                strings.TrimSpace(credential.KeyID),
			"display_name":          strings.TrimSpace(credential.DisplayName),
			"type":                  strings.TrimSpace(credential.Type),
			"usage":                 strings.TrimSpace(credential.Usage),
			"start_date_time":       strings.TrimSpace(credential.StartDateTimeRaw),
			"end_date_time":         strings.TrimSpace(credential.EndDateTimeRaw),
			"custom_key_identifier": strings.TrimSpace(credential.CustomKeyIdentifier),
			"asset_kind":            assetKind,
			"asset_external_id":     assetExternalID,
		}),
	}
}

func (i *EntraIntegration) collectAppAssetOwners(ctx context.Context, deps registry.IntegrationDeps, applications []Application, servicePrincipals []ServicePrincipal) ([]appAssetOwnerUpsertRow, error) {
	totalAssets := len(applications) + len(servicePrincipals)
	deps.Report(registry.Event{Source: "entra", Stage: "list-owners", Current: 0, Total: int64(totalAssets), Message: fmt.Sprintf("listing owners for %d app assets", totalAssets)})

	rows := make([]appAssetOwnerUpsertRow, 0)
	processed := 0

	for _, app := range applications {
		assetExternalID := strings.TrimSpace(app.ID)
		if assetExternalID == "" {
			processed++
			continue
		}

		owners, err := i.client.ListApplicationOwners(ctx, assetExternalID)
		if err != nil {
			return nil, fmt.Errorf("entra application owners %s: %w", assetExternalID, err)
		}
		rows = append(rows, buildOwnerRows("entra_application", assetExternalID, owners)...)
		processed++
		deps.Report(registry.Event{Source: "entra", Stage: "list-owners", Current: int64(processed), Total: int64(totalAssets), Message: fmt.Sprintf("owners for assets %d/%d", processed, totalAssets)})
	}

	for _, sp := range servicePrincipals {
		assetExternalID := strings.TrimSpace(sp.ID)
		if assetExternalID == "" {
			processed++
			continue
		}

		owners, err := i.client.ListServicePrincipalOwners(ctx, assetExternalID)
		if err != nil {
			return nil, fmt.Errorf("entra service principal owners %s: %w", assetExternalID, err)
		}
		rows = append(rows, buildOwnerRows("entra_service_principal", assetExternalID, owners)...)
		processed++
		deps.Report(registry.Event{Source: "entra", Stage: "list-owners", Current: int64(processed), Total: int64(totalAssets), Message: fmt.Sprintf("owners for assets %d/%d", processed, totalAssets)})
	}

	return rows, nil
}

func buildOwnerRows(assetKind, assetExternalID string, owners []DirectoryOwner) []appAssetOwnerUpsertRow {
	rows := make([]appAssetOwnerUpsertRow, 0, len(owners))
	for _, owner := range owners {
		ownerExternalID := strings.TrimSpace(owner.ID)
		if ownerExternalID == "" {
			ownerExternalID = strings.TrimSpace(owner.AppID)
		}
		if ownerExternalID == "" {
			continue
		}

		ownerDisplayName := strings.TrimSpace(owner.DisplayName)
		if ownerDisplayName == "" {
			ownerDisplayName = ownerExternalID
		}
		ownerEmail := normalizeEmail(strings.TrimSpace(owner.Mail))
		if ownerEmail == "" {
			ownerEmail = normalizeEmail(strings.TrimSpace(owner.UserPrincipalName))
		}

		rows = append(rows, appAssetOwnerUpsertRow{
			AssetKind:        assetKind,
			AssetExternalID:  assetExternalID,
			OwnerKind:        entraOwnerKind(owner.ODataType),
			OwnerExternalID:  ownerExternalID,
			OwnerDisplayName: ownerDisplayName,
			OwnerEmail:       ownerEmail,
			RawJSON: registry.MarshalJSON(map[string]string{
				"id":                  strings.TrimSpace(owner.ID),
				"odata_type":          strings.TrimSpace(owner.ODataType),
				"display_name":        strings.TrimSpace(owner.DisplayName),
				"mail":                strings.TrimSpace(owner.Mail),
				"user_principal_name": strings.TrimSpace(owner.UserPrincipalName),
				"app_id":              strings.TrimSpace(owner.AppID),
			}),
		})
	}
	return rows
}

func (i *EntraIntegration) upsertAppAssets(ctx context.Context, deps registry.IntegrationDeps, runID int64, rows []appAssetUpsertRow) error {
	deps.Report(registry.Event{Source: "entra", Stage: "write-app-assets", Current: 0, Total: int64(len(rows)), Message: fmt.Sprintf("writing %d app assets", len(rows))})
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

		if _, err := deps.Q.UpsertAppAssetsBulkBySource(ctx, gen.UpsertAppAssetsBulkBySourceParams{
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

		deps.Report(registry.Event{Source: "entra", Stage: "write-app-assets", Current: int64(end), Total: int64(len(rows)), Message: fmt.Sprintf("app assets %d/%d", end, len(rows))})
	}

	return nil
}

func (i *EntraIntegration) upsertAppAssetOwners(ctx context.Context, deps registry.IntegrationDeps, runID int64, rows []appAssetOwnerUpsertRow) error {
	deps.Report(registry.Event{Source: "entra", Stage: "write-owners", Current: 0, Total: int64(len(rows)), Message: fmt.Sprintf("writing %d owner rows", len(rows))})
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

		if _, err := deps.Q.UpsertAppAssetOwnersBulkBySource(ctx, gen.UpsertAppAssetOwnersBulkBySourceParams{
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

		deps.Report(registry.Event{Source: "entra", Stage: "write-owners", Current: int64(end), Total: int64(len(rows)), Message: fmt.Sprintf("owners %d/%d", end, len(rows))})
	}

	return nil
}

func (i *EntraIntegration) upsertCredentialArtifacts(ctx context.Context, deps registry.IntegrationDeps, runID int64, rows []credentialArtifactUpsertRow) error {
	deps.Report(registry.Event{Source: "entra", Stage: "write-credentials", Current: 0, Total: int64(len(rows)), Message: fmt.Sprintf("writing %d credential rows", len(rows))})
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

		if _, err := deps.Q.UpsertCredentialArtifactsBulkBySource(ctx, gen.UpsertCredentialArtifactsBulkBySourceParams{
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

		deps.Report(registry.Event{Source: "entra", Stage: "write-credentials", Current: int64(end), Total: int64(len(rows)), Message: fmt.Sprintf("credentials %d/%d", end, len(rows))})
	}

	return nil
}

func buildCredentialAuditEventRows(events []DirectoryAuditEvent) []credentialAuditEventUpsertRow {
	rows := make([]credentialAuditEventUpsertRow, 0, len(events))
	for _, event := range events {
		if !isEntraGovernanceAuditEvent(event) {
			continue
		}

		eventTime := parseGraphTime(event.ActivityDateTimeRaw)
		if !eventTime.Valid {
			continue
		}

		eventExternalID := strings.TrimSpace(event.ID)
		if eventExternalID == "" {
			eventExternalID = syntheticCredentialExternalID(
				"entra_audit_event",
				strings.TrimSpace(event.ActivityDateTimeRaw),
				strings.TrimSpace(event.Category),
				strings.TrimSpace(event.ActivityDisplayName),
				strings.TrimSpace(event.Result),
			)
		}

		eventType := strings.TrimSpace(event.ActivityDisplayName)
		if eventType == "" {
			eventType = strings.TrimSpace(event.Category)
		}
		if eventType == "" {
			eventType = "entra_directory_audit"
		}

		actorKind, actorExternalID, actorDisplayName := entraAuditActor(event.InitiatedBy)
		targetKind, targetExternalID, targetDisplayName := entraAuditTarget(event.TargetResources)
		credentialKind, credentialExternalID := entraAuditCredential(event)

		rawJSON := event.RawJSON
		if len(rawJSON) == 0 {
			rawJSON = registry.MarshalJSON(map[string]any{
				"id":                    strings.TrimSpace(event.ID),
				"category":              strings.TrimSpace(event.Category),
				"result":                strings.TrimSpace(event.Result),
				"activity_display_name": strings.TrimSpace(event.ActivityDisplayName),
				"activity_date_time":    strings.TrimSpace(event.ActivityDateTimeRaw),
			})
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
			RawJSON:              rawJSON,
		})
	}
	return rows
}

func isEntraGovernanceAuditEvent(event DirectoryAuditEvent) bool {
	category := strings.ToLower(strings.TrimSpace(event.Category))
	if strings.Contains(category, "application") || strings.Contains(category, "serviceprincipal") {
		return true
	}

	activity := strings.ToLower(strings.TrimSpace(event.ActivityDisplayName))
	for _, keyword := range []string{"credential", "certificate", "secret", "application", "service principal", "owner"} {
		if strings.Contains(activity, keyword) {
			return true
		}
	}

	for _, target := range event.TargetResources {
		targetType := strings.ToLower(strings.TrimSpace(target.Type))
		if strings.Contains(targetType, "application") || strings.Contains(targetType, "serviceprincipal") {
			return true
		}
	}
	return false
}

func entraAuditActor(initiatedBy DirectoryAuditInitiatedBy) (string, string, string) {
	if initiatedBy.User != nil {
		externalID := strings.TrimSpace(initiatedBy.User.ID)
		if externalID == "" {
			externalID = normalizeEmail(strings.TrimSpace(initiatedBy.User.UserPrincipalName))
		}
		displayName := strings.TrimSpace(initiatedBy.User.DisplayName)
		if displayName == "" {
			displayName = strings.TrimSpace(initiatedBy.User.UserPrincipalName)
		}
		if displayName == "" {
			displayName = externalID
		}
		return "entra_user", externalID, displayName
	}

	if initiatedBy.App != nil {
		externalID := strings.TrimSpace(initiatedBy.App.ServicePrincipalID)
		if externalID == "" {
			externalID = strings.TrimSpace(initiatedBy.App.AppID)
		}
		displayName := strings.TrimSpace(initiatedBy.App.DisplayName)
		if displayName == "" {
			displayName = externalID
		}
		return "entra_service_principal", externalID, displayName
	}

	return "unknown", "", ""
}

func entraAuditTarget(targets []DirectoryAuditTargetResource) (string, string, string) {
	for _, target := range targets {
		targetExternalID := strings.TrimSpace(target.ID)
		targetDisplayName := strings.TrimSpace(target.DisplayName)
		targetType := strings.TrimSpace(target.Type)
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
	kind := entraAuditCredentialKind(event.ActivityDisplayName, event.Category)
	externalID := ""

	for _, target := range event.TargetResources {
		for _, prop := range target.ModifiedProperties {
			if id := extractCredentialExternalID(prop.NewValue); id != "" {
				externalID = id
				break
			}
			if id := extractCredentialExternalID(prop.OldValue); id != "" {
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

func (i *EntraIntegration) upsertCredentialAuditEvents(ctx context.Context, deps registry.IntegrationDeps, rows []credentialAuditEventUpsertRow) error {
	deps.Report(registry.Event{Source: "entra", Stage: "write-audit-events", Current: 0, Total: int64(len(rows)), Message: fmt.Sprintf("writing %d credential audit events", len(rows))})
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

		if _, err := deps.Q.UpsertCredentialAuditEventsBulkBySource(ctx, gen.UpsertCredentialAuditEventsBulkBySourceParams{
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

		deps.Report(registry.Event{
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

func (i *EntraIntegration) syncDiscovery(ctx context.Context, deps registry.IntegrationDeps, runID int64, applications []Application, servicePrincipals []ServicePrincipal) error {
	now := time.Now().UTC()

	deps.Report(registry.Event{Source: "entra", Stage: "list-discovery-events", Current: 0, Total: 1, Message: "listing sign-ins and oauth grants"})
	since := now.Add(-7 * 24 * time.Hour)
	latestObservedAt, err := deps.Q.GetLatestSaaSDiscoveryObservedAtBySource(ctx, gen.GetLatestSaaSDiscoveryObservedAtBySourceParams{
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
	deps.Report(registry.Event{
		Source:  "entra",
		Stage:   "list-discovery-events",
		Current: 1,
		Total:   1,
		Message: fmt.Sprintf("found %d sign-ins and %d oauth grants", len(signIns), len(grants)),
	})

	deps.Report(registry.Event{Source: "entra", Stage: "normalize-discovery", Current: 0, Total: 1, Message: "normalizing discovery evidence"})
	sources, events := normalizeEntraDiscovery(signIns, grants, applications, servicePrincipals, i.tenantID, now)
	deps.Report(registry.Event{
		Source:  "entra",
		Stage:   "normalize-discovery",
		Current: 1,
		Total:   1,
		Message: fmt.Sprintf("normalized %d source rows and %d events", len(sources), len(events)),
	})

	if err := i.writeDiscoveryRows(ctx, deps, runID, sources, events); err != nil {
		metrics.DiscoveryIngestFailuresTotal.WithLabelValues("entra", "idp_sso", "db_error").Inc()
		return err
	}
	if err := i.seedEntraAutoBindings(ctx, deps); err != nil {
		return err
	}
	return nil
}

func normalizeEntraDiscovery(signIns []SignInEvent, grants []OAuth2PermissionGrant, applications []Application, servicePrincipals []ServicePrincipal, tenantID string, now time.Time) ([]normalizedDiscoverySource, []normalizedDiscoveryEvent) {
	sourceByID := map[string]normalizedDiscoverySource{}
	events := make([]normalizedDiscoveryEvent, 0, len(signIns)+len(grants))

	appDisplayByAppID := make(map[string]string, len(applications))
	appVendorByAppID := make(map[string]string, len(applications))
	for _, app := range applications {
		appID := strings.TrimSpace(app.AppID)
		if appID == "" {
			continue
		}
		name := strings.TrimSpace(app.DisplayName)
		if name == "" {
			name = appID
		}
		appDisplayByAppID[appID] = name

		vendorName := strings.TrimSpace(app.VerifiedPublisher.DisplayName)
		if vendorName == "" {
			vendorName = discovery.VendorLabelFromDomain(app.PublisherDomain)
		}
		if vendorName != "" {
			appVendorByAppID[appID] = vendorName
		}
	}

	servicePrincipalByID := make(map[string]ServicePrincipal, len(servicePrincipals))
	servicePrincipalVendorByAppID := make(map[string]string, len(servicePrincipals))
	for _, servicePrincipal := range servicePrincipals {
		spID := strings.TrimSpace(servicePrincipal.ID)
		if spID == "" {
			continue
		}
		servicePrincipalByID[spID] = servicePrincipal
		appID := strings.TrimSpace(servicePrincipal.AppID)
		if appID == "" {
			continue
		}
		vendorName := strings.TrimSpace(servicePrincipal.PublisherName)
		if vendorName == "" {
			continue
		}
		if _, exists := servicePrincipalVendorByAppID[appID]; !exists {
			servicePrincipalVendorByAppID[appID] = vendorName
		}
	}

	for _, signIn := range signIns {
		sourceAppID := strings.TrimSpace(signIn.AppID)
		if sourceAppID == "" {
			sourceAppID = strings.TrimSpace(signIn.AppDisplayName)
		}
		if sourceAppID == "" {
			continue
		}

		sourceAppName := strings.TrimSpace(signIn.AppDisplayName)
		if sourceAppName == "" {
			sourceAppName = appDisplayByAppID[sourceAppID]
		}
		if sourceAppName == "" {
			sourceAppName = sourceAppID
		}

		observedAt := graphObservedAtOrNow(signIn.CreatedDateTimeRaw, now)
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
			EntraAppID:       strings.TrimSpace(signIn.AppID),
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

		eventExternalID := strings.TrimSpace(signIn.ID)
		if eventExternalID == "" {
			eventExternalID = fmt.Sprintf("signin:%s:%s:%s", sourceAppID, strings.TrimSpace(signIn.UserID), observedAt.Format(time.RFC3339Nano))
		}

		events = append(events, normalizedDiscoveryEvent{
			CanonicalKey:     metadata.CanonicalKey,
			SignalKind:       discovery.SignalKindIDPSSO,
			EventExternalID:  eventExternalID,
			SourceAppID:      sourceAppID,
			SourceAppName:    sourceAppName,
			SourceAppDomain:  metadata.Domain,
			SourceVendorName: metadata.VendorName,
			ActorExternalID:  strings.TrimSpace(signIn.UserID),
			ActorEmail:       normalizeEmail(strings.TrimSpace(signIn.UserPrincipalName)),
			ActorDisplayName: strings.TrimSpace(signIn.UserDisplayName),
			ObservedAt:       observedAt,
			Scopes:           nil,
			RawJSON:          registry.NormalizeJSON(signIn.RawJSON),
		})
	}

	for _, grant := range grants {
		spID := strings.TrimSpace(grant.ClientID)
		servicePrincipal := servicePrincipalByID[spID]

		entraAppID := strings.TrimSpace(servicePrincipal.AppID)
		if entraAppID == "" {
			entraAppID = strings.TrimSpace(grant.ClientID)
		}
		sourceAppID := entraAppID
		if sourceAppID == "" {
			sourceAppID = strings.TrimSpace(grant.ClientID)
		}
		if sourceAppID == "" {
			continue
		}

		sourceAppName := strings.TrimSpace(servicePrincipal.DisplayName)
		if sourceAppName == "" {
			sourceAppName = appDisplayByAppID[entraAppID]
		}
		if sourceAppName == "" {
			sourceAppName = sourceAppID
		}

		observedAt := graphObservedAtOrNow(grant.CreatedDateTimeRaw, now)
		scopes := discovery.NormalizeScopes(strings.Fields(strings.ReplaceAll(grant.Scope, ",", " ")))
		sourceVendorName := appVendorByAppID[entraAppID]
		if sourceVendorName == "" {
			sourceVendorName = strings.TrimSpace(servicePrincipal.PublisherName)
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

		eventExternalID := strings.TrimSpace(grant.ID)
		if eventExternalID == "" {
			eventExternalID = fmt.Sprintf("grant:%s:%s:%s:%s", sourceAppID, strings.TrimSpace(grant.PrincipalID), strings.TrimSpace(grant.Scope), observedAt.Format(time.RFC3339Nano))
		}

		events = append(events, normalizedDiscoveryEvent{
			CanonicalKey:     metadata.CanonicalKey,
			SignalKind:       discovery.SignalKindOAuth,
			EventExternalID:  eventExternalID,
			SourceAppID:      sourceAppID,
			SourceAppName:    sourceAppName,
			SourceAppDomain:  metadata.Domain,
			SourceVendorName: metadata.VendorName,
			ActorExternalID:  strings.TrimSpace(grant.PrincipalID),
			ActorEmail:       "",
			ActorDisplayName: strings.TrimSpace(grant.PrincipalID),
			ObservedAt:       observedAt,
			Scopes:           scopes,
			RawJSON:          registry.NormalizeJSON(grant.RawJSON),
		})
	}

	sourceRows := make([]normalizedDiscoverySource, 0, len(sourceByID))
	for _, sourceRow := range sourceByID {
		sourceRows = append(sourceRows, sourceRow)
	}
	return sourceRows, events
}

func (i *EntraIntegration) writeDiscoveryRows(ctx context.Context, deps registry.IntegrationDeps, runID int64, sources []normalizedDiscoverySource, events []normalizedDiscoveryEvent) error {
	total := len(sources) + len(events)
	deps.Report(registry.Event{
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
		if _, err := deps.Q.UpsertSaaSAppsBulk(ctx, gen.UpsertSaaSAppsBulkParams{
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
		if _, err := deps.Q.UpsertSaaSAppSourcesBulkBySource(ctx, gen.UpsertSaaSAppSourcesBulkBySourceParams{
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
		deps.Report(registry.Event{
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
		if _, err := deps.Q.UpsertSaaSAppEventsBulkBySource(ctx, gen.UpsertSaaSAppEventsBulkBySourceParams{
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
		deps.Report(registry.Event{
			Source:  "entra",
			Stage:   "write-discovery",
			Current: int64(written),
			Total:   int64(total),
			Message: fmt.Sprintf("events %d/%d", written, total),
		})
	}

	if written == 0 {
		deps.Report(registry.Event{
			Source:  "entra",
			Stage:   "write-discovery",
			Current: 0,
			Total:   0,
			Message: "no discovery records to write",
		})
	}
	return nil
}

func (i *EntraIntegration) seedEntraAutoBindings(ctx context.Context, deps registry.IntegrationDeps) error {
	appIDs, err := deps.Q.ListEntraDiscoveryAppIDsWithManagedAssetsBySource(ctx, i.tenantID)
	if err != nil {
		return fmt.Errorf("list entra discovery auto-bind candidates: %w", err)
	}
	for _, appID := range appIDs {
		if err := deps.Q.UpsertSaaSAppBinding(ctx, gen.UpsertSaaSAppBindingParams{
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
		if _, err := deps.Q.RecomputePrimarySaaSAppBindingsForAll(ctx); err != nil {
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

func sanitizePasswordCredentials(credentials []PasswordCredential) []map[string]string {
	out := make([]map[string]string, 0, len(credentials))
	for _, credential := range credentials {
		out = append(out, map[string]string{
			"key_id":          strings.TrimSpace(credential.KeyID),
			"display_name":    strings.TrimSpace(credential.DisplayName),
			"start_date_time": strings.TrimSpace(credential.StartDateTimeRaw),
			"end_date_time":   strings.TrimSpace(credential.EndDateTimeRaw),
			"hint":            strings.TrimSpace(credential.Hint),
		})
	}
	return out
}

func sanitizeKeyCredentials(credentials []KeyCredential) []map[string]string {
	out := make([]map[string]string, 0, len(credentials))
	for _, credential := range credentials {
		out = append(out, map[string]string{
			"key_id":                strings.TrimSpace(credential.KeyID),
			"display_name":          strings.TrimSpace(credential.DisplayName),
			"type":                  strings.TrimSpace(credential.Type),
			"usage":                 strings.TrimSpace(credential.Usage),
			"start_date_time":       strings.TrimSpace(credential.StartDateTimeRaw),
			"end_date_time":         strings.TrimSpace(credential.EndDateTimeRaw),
			"custom_key_identifier": strings.TrimSpace(credential.CustomKeyIdentifier),
		})
	}
	return out
}

type sanitizedUser struct {
	ID                string   `json:"id"`
	DisplayName       string   `json:"display_name,omitempty"`
	Mail              string   `json:"mail,omitempty"`
	UserPrincipalName string   `json:"user_principal_name,omitempty"`
	OtherMails        []string `json:"other_mails,omitempty"`
	ProxyAddresses    []string `json:"proxy_addresses,omitempty"`
	UserType          string   `json:"user_type,omitempty"`
	AccountEnabled    *bool    `json:"account_enabled,omitempty"`
	Status            string   `json:"status,omitempty"`
	// Kept as-is to avoid timezone parsing/format churn until needed.
	CreatedDateTimeRaw string `json:"created_date_time,omitempty"`
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

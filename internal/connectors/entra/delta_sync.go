package entra

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	absser "github.com/microsoft/kiota-abstractions-go/serialization"
	msgraphmodels "github.com/microsoftgraph/msgraph-sdk-go/models"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

const (
	entraDeltaResourceUsers             = "users"
	entraDeltaResourceGroups            = "groups"
	entraDeltaResourceApplications      = "applications"
	entraDeltaResourceServicePrincipals = "service_principals"
)

var entraInventoryDeltaResources = []string{
	entraDeltaResourceUsers,
	entraDeltaResourceGroups,
	entraDeltaResourceApplications,
	entraDeltaResourceServicePrincipals,
}

type entraDeltaClient interface {
	DeltaUsers(context.Context, string) (DeltaResult[User], error)
	DeltaGroups(context.Context, string) (DeltaResult[Group], error)
	DeltaApplications(context.Context, string) (DeltaResult[Application], error)
	DeltaServicePrincipals(context.Context, string) (DeltaResult[ServicePrincipal], error)
}

type entraInventoryDelta struct {
	Bootstrap         bool
	Users             []User
	Groups            []Group
	Applications      []Application
	ServicePrincipals []ServicePrincipal

	DeletedAccountExternalIDs []string
	DeletedApplications       []string
	DeletedServicePrincipals  []string
	CredentialAssetRefs       []string
	GraphDeltaCursors         []registry.ConnectorGraphDeltaCursorUpdate
}

func (i *EntraIntegration) runDeltaFull(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, report func(registry.Event)) error {
	started := time.Now()
	slog.Info("syncing Microsoft Entra ID with delta", "tenant", i.tenantID)

	runID, err := registry.StartSyncRun(ctx, q, "entra", i.tenantID)
	if err != nil {
		return err
	}

	deltaClient, ok := i.client.(entraDeltaClient)
	if !ok {
		err := errors.New("entra client does not support delta sync")
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindUnknown)
	}

	states, err := i.deltaStatesByResource(ctx, q)
	if err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}
	bootstrap := shouldBootstrapEntraDelta(states)
	resetDeltaCursors := false

	delta, err := i.fetchInventoryDelta(ctx, report, deltaClient, states, bootstrap)
	if errors.Is(err, ErrDeltaCursorExpired) {
		report(registry.Event{Source: "entra", Stage: "list-app-assets", Message: "Entra delta cursor expired; restarting delta bootstrap", Err: err})
		// Keep the four inventory streams in one delta epoch. Old cursors are
		// deleted only if the bootstrap retry and finalization both commit.
		resetDeltaCursors = true
		delta, err = i.fetchInventoryDelta(ctx, report, deltaClient, nil, true)
	}
	if err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
	}

	usersWritten, err := i.writeUsers(ctx, q, report, runID, delta.Users)
	if err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}
	groupsWritten, err := i.writeGroups(ctx, q, report, runID, delta.Groups)
	if err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}
	servicePrincipalsWritten, err := i.writeServicePrincipalAccounts(ctx, q, report, runID, delta.ServicePrincipals)
	if err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}

	assetRows, credentialRows, err := buildEntraAssetAndCredentialRows(delta.Applications, delta.ServicePrincipals)
	if err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindUnknown)
	}
	if err := i.upsertAppAssets(ctx, q, report, runID, assetRows); err != nil {
		report(registry.Event{Source: "entra", Stage: "write-app-assets", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}
	if err := i.upsertCredentialArtifacts(ctx, q, report, runID, credentialRows); err != nil {
		report(registry.Event{Source: "entra", Stage: "write-credentials", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}

	applicationsForReconcile := delta.Applications
	servicePrincipalsForReconcile := delta.ServicePrincipals
	if !delta.Bootstrap {
		applicationsForReconcile, servicePrincipalsForReconcile, err = i.loadCurrentAppAssetsForReconcile(ctx, q, runID, delta.DeletedApplications, delta.DeletedServicePrincipals)
		if err != nil {
			return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
		}
	}

	ownerRows, err := i.collectAppAssetOwners(ctx, report, applicationsForReconcile, servicePrincipalsForReconcile)
	if err != nil {
		report(registry.Event{Source: "entra", Stage: "list-owners", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
	}
	if err := i.upsertAppAssetOwners(ctx, q, report, runID, ownerRows); err != nil {
		report(registry.Event{Source: "entra", Stage: "write-owners", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}

	entitlementRows, err := i.collectEntraEntitlements(ctx, report, servicePrincipalsForReconcile)
	if err != nil {
		report(registry.Event{Source: "entra", Stage: "list-entitlements", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
	}
	if err := i.upsertEntraEntitlements(ctx, q, report, runID, entitlementRows); err != nil {
		report(registry.Event{Source: "entra", Stage: "write-entitlements", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}

	auditEventRows, err := i.syncCredentialAuditEvents(ctx, q, report)
	if err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
	}

	if err := registry.FinalizeAppDeltaRun(ctx, q, pool, runID, "entra", i.tenantID, time.Since(started), registry.AppDeltaFinalizeOptions{
		ResetDeltaCursors:             resetDeltaCursors,
		ExpireAbsent:                  delta.Bootstrap,
		DeletedAccountExternalIDs:     delta.DeletedAccountExternalIDs,
		DeletedAppAssets:              delta.deletedAppAssetOptions(),
		CredentialAssetRefExternalIDs: delta.CredentialAssetRefs,
		GraphDeltaCursors:             delta.GraphDeltaCursors,
	}); err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}

	slog.Info(
		"entra delta sync complete",
		"tenant", i.tenantID,
		"bootstrap", delta.Bootstrap,
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

func (i *EntraIntegration) deltaStatesByResource(ctx context.Context, q *gen.Queries) (map[string]string, error) {
	rows, err := q.ListConnectorCursorStatesBySourceAndKind(ctx, gen.ListConnectorCursorStatesBySourceAndKindParams{
		SourceKind: "entra",
		SourceName: i.tenantID,
		CursorKind: registry.ConnectorCursorKindGraphDelta,
	})
	if err != nil {
		return nil, fmt.Errorf("list entra delta states: %w", err)
	}
	states := make(map[string]string, len(rows))
	for _, row := range rows {
		deltaLink, err := registry.ConnectorGraphDeltaLinkFromCursorState(row)
		if err != nil {
			return nil, err
		}
		states[strings.TrimSpace(row.Resource)] = deltaLink
	}
	return states, nil
}

func shouldBootstrapEntraDelta(states map[string]string) bool {
	for _, resource := range entraInventoryDeltaResources {
		if strings.TrimSpace(states[resource]) == "" {
			return true
		}
	}
	return false
}

func (i *EntraIntegration) fetchInventoryDelta(ctx context.Context, report func(registry.Event), client entraDeltaClient, states map[string]string, bootstrap bool) (entraInventoryDelta, error) {
	out := entraInventoryDelta{Bootstrap: bootstrap}

	report(registry.Event{Source: "entra", Stage: "list-users", Current: 0, Total: 1, Message: entraDeltaListMessage("users", bootstrap)})
	users, err := client.DeltaUsers(ctx, entraDeltaCursor(states, entraDeltaResourceUsers, bootstrap))
	if err != nil {
		report(registry.Event{Source: "entra", Stage: "list-users", Message: err.Error(), Err: err})
		return out, err
	}
	out.Users = users.Items
	out.DeletedAccountExternalIDs = append(out.DeletedAccountExternalIDs, users.RemovedIDs...)
	out.GraphDeltaCursors = append(out.GraphDeltaCursors, registry.ConnectorGraphDeltaCursorUpdate{Resource: entraDeltaResourceUsers, DeltaLink: users.DeltaLink})

	report(registry.Event{Source: "entra", Stage: "list-groups", Current: 0, Total: 1, Message: entraDeltaListMessage("groups", bootstrap)})
	groups, err := client.DeltaGroups(ctx, entraDeltaCursor(states, entraDeltaResourceGroups, bootstrap))
	if err != nil {
		report(registry.Event{Source: "entra", Stage: "list-groups", Message: err.Error(), Err: err})
		return out, err
	}
	out.Groups = groups.Items
	for _, id := range groups.RemovedIDs {
		out.DeletedAccountExternalIDs = append(out.DeletedAccountExternalIDs, entraGroupExternalID(id))
	}
	out.GraphDeltaCursors = append(out.GraphDeltaCursors, registry.ConnectorGraphDeltaCursorUpdate{Resource: entraDeltaResourceGroups, DeltaLink: groups.DeltaLink})

	report(registry.Event{Source: "entra", Stage: "list-app-assets", Current: 0, Total: 1, Message: entraDeltaListMessage("applications and service principals", bootstrap)})
	applications, err := client.DeltaApplications(ctx, entraDeltaCursor(states, entraDeltaResourceApplications, bootstrap))
	if err != nil {
		report(registry.Event{Source: "entra", Stage: "list-app-assets", Message: err.Error(), Err: err})
		return out, err
	}
	out.Applications = applications.Items
	out.DeletedApplications = applications.RemovedIDs
	out.GraphDeltaCursors = append(out.GraphDeltaCursors, registry.ConnectorGraphDeltaCursorUpdate{Resource: entraDeltaResourceApplications, DeltaLink: applications.DeltaLink})

	servicePrincipals, err := client.DeltaServicePrincipals(ctx, entraDeltaCursor(states, entraDeltaResourceServicePrincipals, bootstrap))
	if err != nil {
		report(registry.Event{Source: "entra", Stage: "list-app-assets", Message: err.Error(), Err: err})
		return out, err
	}
	out.ServicePrincipals = servicePrincipals.Items
	out.DeletedServicePrincipals = servicePrincipals.RemovedIDs
	for _, id := range servicePrincipals.RemovedIDs {
		out.DeletedAccountExternalIDs = append(out.DeletedAccountExternalIDs, entraServicePrincipalExternalID(id))
	}
	out.GraphDeltaCursors = append(out.GraphDeltaCursors, registry.ConnectorGraphDeltaCursorUpdate{Resource: entraDeltaResourceServicePrincipals, DeltaLink: servicePrincipals.DeltaLink})
	out.CredentialAssetRefs = out.changedCredentialAssetRefs()

	report(registry.Event{
		Source:  "entra",
		Stage:   "list-app-assets",
		Current: 1,
		Total:   1,
		Message: fmt.Sprintf("found %d application changes and %d service principal changes", len(out.Applications), len(out.ServicePrincipals)),
	})

	if err := validateDeltaCursors(out.GraphDeltaCursors); err != nil {
		return out, err
	}
	return out, nil
}

func entraDeltaListMessage(resource string, bootstrap bool) string {
	if bootstrap {
		return "bootstrapping Entra " + resource + " delta"
	}
	return "pulling Entra " + resource + " delta"
}

func entraDeltaCursor(states map[string]string, resource string, bootstrap bool) string {
	if bootstrap {
		return ""
	}
	return strings.TrimSpace(states[resource])
}

func validateDeltaCursors(cursors []registry.ConnectorGraphDeltaCursorUpdate) error {
	for _, cursor := range cursors {
		if strings.TrimSpace(cursor.Resource) == "" {
			return errors.New("entra delta cursor resource is empty")
		}
		if strings.TrimSpace(cursor.DeltaLink) == "" {
			return fmt.Errorf("entra delta response for %s did not include a delta link", cursor.Resource)
		}
	}
	return nil
}

func (d entraInventoryDelta) deletedAppAssetOptions() []registry.AppAssetDeltaDelete {
	return []registry.AppAssetDeltaDelete{
		{AssetKind: "entra_application", ExternalIDs: d.DeletedApplications},
		{AssetKind: "entra_service_principal", ExternalIDs: d.DeletedServicePrincipals},
	}
}

func (d entraInventoryDelta) changedCredentialAssetRefs() []string {
	refs := make([]string, 0, len(d.Applications)+len(d.ServicePrincipals)+len(d.DeletedApplications)+len(d.DeletedServicePrincipals))
	for _, app := range d.Applications {
		if id := entityID(app); id != "" {
			refs = append(refs, appAssetRefExternalID("entra_application", id))
		}
	}
	for _, sp := range d.ServicePrincipals {
		if id := entityID(sp); id != "" {
			refs = append(refs, appAssetRefExternalID("entra_service_principal", id))
		}
	}
	for _, id := range d.DeletedApplications {
		refs = append(refs, appAssetRefExternalID("entra_application", id))
	}
	for _, id := range d.DeletedServicePrincipals {
		refs = append(refs, appAssetRefExternalID("entra_service_principal", id))
	}
	return distinctNonEmptyStrings(refs)
}

func (i *EntraIntegration) loadCurrentAppAssetsForReconcile(ctx context.Context, q *gen.Queries, runID int64, deletedApplications, deletedServicePrincipals []string) ([]Application, []ServicePrincipal, error) {
	appRows, err := q.ListAppAssetsForDeltaReconcileBySourceAndKind(ctx, gen.ListAppAssetsForDeltaReconcileBySourceAndKindParams{
		SourceKind:  "entra",
		SourceName:  i.tenantID,
		AssetKind:   "entra_application",
		SeenInRunID: runID,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("list entra applications for reconcile: %w", err)
	}
	spRows, err := q.ListAppAssetsForDeltaReconcileBySourceAndKind(ctx, gen.ListAppAssetsForDeltaReconcileBySourceAndKindParams{
		SourceKind:  "entra",
		SourceName:  i.tenantID,
		AssetKind:   "entra_service_principal",
		SeenInRunID: runID,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("list entra service principals for reconcile: %w", err)
	}

	applications, err := decodeGraphAssetRows[Application](appRows, deletedApplications, msgraphmodels.CreateApplicationFromDiscriminatorValue, "application")
	if err != nil {
		return nil, nil, err
	}
	servicePrincipals, err := decodeGraphAssetRows[ServicePrincipal](spRows, deletedServicePrincipals, msgraphmodels.CreateServicePrincipalFromDiscriminatorValue, "service principal")
	if err != nil {
		return nil, nil, err
	}

	return applications, servicePrincipals, nil
}

func decodeGraphAssetRows[T any](rows []gen.AppAsset, deletedExternalIDs []string, factory absser.ParsableFactory, modelName string) ([]T, error) {
	deleted := stringSet(deletedExternalIDs)
	models := make([]T, 0, len(rows))
	for _, row := range rows {
		if _, ok := deleted[row.ExternalID]; ok {
			continue
		}
		model, err := deserializeGraphModel[T](row.RawJson, factory)
		if err != nil {
			return nil, fmt.Errorf("decode entra %s %s: %w", modelName, row.ExternalID, err)
		}
		models = append(models, model)
	}
	return models, nil
}

func deserializeGraphModel[T any](raw []byte, factory absser.ParsableFactory) (T, error) {
	var zero T
	if len(raw) == 0 {
		return zero, errors.New("raw graph payload is empty")
	}
	if err := ensureGraphSerializationRegistered(); err != nil {
		return zero, err
	}
	model, err := absser.Deserialize("application/json", raw, factory)
	if err != nil {
		return zero, err
	}
	typed, ok := model.(T)
	if !ok {
		return zero, fmt.Errorf("unexpected graph model type %T", model)
	}
	return typed, nil
}

func (i *EntraIntegration) syncCredentialAuditEvents(ctx context.Context, q *gen.Queries, report func(registry.Event)) ([]credentialAuditEventUpsertRow, error) {
	report(registry.Event{Source: "entra", Stage: "list-audit-events", Current: 0, Total: 1, Message: "listing directory audit events"})
	since, err := i.latestCredentialAuditSince(ctx, q)
	if err != nil {
		return nil, err
	}
	directoryAudits, err := i.client.ListDirectoryAudits(ctx, since)
	if err != nil {
		report(registry.Event{Source: "entra", Stage: "list-audit-events", Message: err.Error(), Err: err})
		return nil, err
	}
	report(registry.Event{
		Source:  "entra",
		Stage:   "list-audit-events",
		Current: 1,
		Total:   1,
		Message: fmt.Sprintf("found %d directory audit events", len(directoryAudits)),
	})

	rows, err := buildCredentialAuditEventRows(directoryAudits)
	if err != nil {
		return nil, err
	}
	if err := i.upsertCredentialAuditEvents(ctx, q, report, rows); err != nil {
		report(registry.Event{Source: "entra", Stage: "write-audit-events", Message: err.Error(), Err: err})
		return nil, err
	}
	return rows, nil
}

func (i *EntraIntegration) latestCredentialAuditSince(ctx context.Context, q *gen.Queries) (*time.Time, error) {
	latest, err := q.GetLatestCredentialAuditEventTimeBySource(ctx, gen.GetLatestCredentialAuditEventTimeBySourceParams{
		SourceKind: "entra",
		SourceName: i.tenantID,
	})
	if err != nil {
		return nil, fmt.Errorf("query latest entra credential audit event time: %w", err)
	}
	if !latest.Valid {
		return nil, nil
	}
	since := latest.Time.UTC().Add(-15 * time.Minute)
	return &since, nil
}

func stringSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		set[value] = struct{}{}
	}
	return set
}

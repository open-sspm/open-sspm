package sync

import (
	"context"
	"errors"

	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

type ResyncQueueRunner struct {
	store   syncJobStore
	planner manualSyncPlanner
	lane    string
	mode    registry.RunMode
}

type manualSyncPlanner interface {
	Prepare(context.Context) error
}

func NewResyncQueueRunnerWithPlanner(dbStore syncJobStore, planner manualSyncPlanner, mode registry.RunMode) Runner {
	return &ResyncQueueRunner{
		store:   dbStore,
		planner: planner,
		lane:    SyncJobLaneForMode(mode),
		mode:    mode.Normalize(),
	}
}

func (r *ResyncQueueRunner) RunOnce(ctx context.Context) error {
	if r == nil || r.store == nil {
		return errors.New("sync runner is not configured")
	}

	request := TriggerRequest{}
	if connectorKind, sourceName, ok := ConnectorScopeFromContext(ctx); ok {
		request = TriggerRequest{
			ConnectorKind: connectorKind,
			SourceName:    sourceName,
		}.Normalized()
	}
	if r.mode == registry.RunModeDiscovery && request.HasConnectorScope() && !supportsDiscoveryScopedConnectorKind(request.ConnectorKind) {
		return ErrNoConnectorsDue
	}
	if r.planner != nil && request.HasConnectorScope() {
		// Unscoped manual resyncs stay fire-and-forget so one lane's preflight
		// failure does not mask another lane that successfully accepted work.
		planCtx := WithConnectorScope(WithForcedSync(ctx), request.ConnectorKind, request.SourceName)
		if err := r.planner.Prepare(planCtx); err != nil {
			return err
		}
	}
	if err := r.store.EnqueueManualSyncJob(ctx, r.lane, request.ConnectorKind, request.SourceName); err != nil {
		if errors.Is(err, errPendingSyncJobExists) {
			return ErrSyncQueued
		}
		if errors.Is(err, errActiveSyncJobExists) {
			return ErrSyncAlreadyRunning
		}
		return err
	}
	return ErrSyncQueued
}

func supportsDiscoveryScopedConnectorKind(kind string) bool {
	switch kind {
	case configstore.KindOkta, configstore.KindEntra, configstore.KindGoogleWorkspace:
		return true
	default:
		return false
	}
}

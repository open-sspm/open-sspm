package sync

import (
	"context"
	"errors"
	"log/slog"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

type ScheduledQueueRunner struct {
	planner scheduledSyncPlanner
	store   syncJobStore
	lane    string
}

type scheduledSyncPlanner interface {
	PlannedConnectorScopes(context.Context) ([]TriggerRequest, error)
}

func NewScheduledQueueRunner(store syncJobStore, planner scheduledSyncPlanner, mode registry.RunMode) Runner {
	return &ScheduledQueueRunner{
		planner: planner,
		store:   store,
		lane:    SyncJobLaneForMode(mode),
	}
}

func (r *ScheduledQueueRunner) RunOnce(ctx context.Context) error {
	if r == nil || r.store == nil || r.planner == nil {
		return errors.New("sync runner is not configured")
	}

	requests, err := r.planner.PlannedConnectorScopes(ctx)
	if err != nil {
		return err
	}

	var enqueueErrs []error
	for _, request := range requests {
		if !request.HasConnectorScope() {
			continue
		}
		if err := r.store.EnqueueScheduledSyncJob(ctx, r.lane, request.ConnectorKind, request.SourceName); err != nil {
			enqueueErrs = append(enqueueErrs, err)
		}
	}
	if len(enqueueErrs) > 0 {
		return errors.Join(enqueueErrs...)
	}
	if len(requests) == 0 {
		slog.Warn("scheduled sync planning returned no connector scopes", "lane", r.lane)
		return ErrNoConnectorsDue
	}
	return nil
}

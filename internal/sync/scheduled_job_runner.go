package sync

import (
	"context"
	"errors"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

type scheduledSyncJobRunner struct {
	store syncJobStore
	lane  string
}

func NewScheduledSyncJobRunner(store syncJobStore, mode registry.RunMode) Runner {
	return &scheduledSyncJobRunner{
		store: store,
		lane:  SyncJobLaneForMode(mode.Normalize()),
	}
}

func (r *scheduledSyncJobRunner) RunOnce(ctx context.Context) error {
	if r == nil || r.store == nil {
		return errors.New("scheduled sync job runner is not configured")
	}
	return r.store.EnqueueScheduledSyncJob(ctx, r.lane, "", "")
}

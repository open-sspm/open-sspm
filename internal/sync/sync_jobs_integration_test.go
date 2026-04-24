package sync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/testdb"
)

func TestSyncJobStore_ManualPromotesPendingScheduledJob(t *testing.T) {
	t.Parallel()

	withSyncJobsTestDB(t, func(ctx context.Context, store *dbSyncJobStore) {
		if err := store.EnqueueScheduledSyncJob(ctx, syncJobLaneFull, "", ""); err != nil {
			t.Fatalf("EnqueueScheduledSyncJob() err = %v", err)
		}
		if err := store.EnqueueManualSyncJob(ctx, syncJobLaneFull, "", ""); err != nil {
			t.Fatalf("EnqueueManualSyncJob() err = %v", err)
		}

		job, ok, err := store.getActiveSyncJobByScope(ctx, syncJobLaneFull, "", "")
		if err != nil {
			t.Fatalf("getActiveSyncJobByScope() err = %v", err)
		}
		if !ok {
			t.Fatalf("expected active sync job")
		}
		if job.TriggerKind != syncJobTriggerKindManual || job.Status != syncJobStatusPending || job.RerunRequested {
			t.Fatalf("active job = %+v", job)
		}
	})
}

func TestSyncJobStore_ClaimSkipsDelayedScheduledJobs(t *testing.T) {
	t.Parallel()

	withSyncJobsTestDB(t, func(ctx context.Context, store *dbSyncJobStore) {
		if err := store.createSyncJob(ctx, syncJobLaneFull, "", "", syncJobTriggerKindScheduled, time.Now().Add(time.Hour), 0, false); err != nil {
			t.Fatalf("createSyncJob(delayed scheduled) err = %v", err)
		}
		if err := store.createSyncJob(ctx, syncJobLaneFull, "github", "Acme", syncJobTriggerKindManual, time.Now(), 0, false); err != nil {
			t.Fatalf("createSyncJob(manual) err = %v", err)
		}

		job, ok, err := store.ClaimNextSyncJobByLane(ctx, syncJobLaneFull, "claimant", 60)
		if err != nil {
			t.Fatalf("ClaimNextSyncJobByLane() err = %v", err)
		}
		if !ok {
			t.Fatalf("expected claimable sync job")
		}
		if job.TriggerKind != syncJobTriggerKindManual || job.ConnectorKind != "github" || job.SourceName != "Acme" {
			t.Fatalf("claimed job = %+v", job)
		}
	})
}

func TestSyncJobStore_RunningScheduledJobCanRerunAsManual(t *testing.T) {
	t.Parallel()

	withSyncJobsTestDB(t, func(ctx context.Context, store *dbSyncJobStore) {
		if err := store.EnqueueScheduledSyncJob(ctx, syncJobLaneFull, "", ""); err != nil {
			t.Fatalf("EnqueueScheduledSyncJob() err = %v", err)
		}

		job, ok, err := store.ClaimNextSyncJobByLane(ctx, syncJobLaneFull, "claimant", 60)
		if err != nil {
			t.Fatalf("ClaimNextSyncJobByLane() err = %v", err)
		}
		if !ok {
			t.Fatalf("expected claimable sync job")
		}

		runningJob, marked, err := store.MarkSyncJobRunning(ctx, job.ID, "claimant")
		if err != nil {
			t.Fatalf("MarkSyncJobRunning() err = %v", err)
		}
		if !marked || runningJob.TriggerKind != syncJobTriggerKindScheduled {
			t.Fatalf("running job = %+v, marked=%v", runningJob, marked)
		}

		if err := store.EnqueueManualSyncJob(ctx, syncJobLaneFull, "", ""); err != nil {
			t.Fatalf("EnqueueManualSyncJob() err = %v", err)
		}
		if marked, err := store.CompleteScheduledSyncJobSuccess(ctx, job.ID, "claimant"); err != nil {
			t.Fatalf("CompleteScheduledSyncJobSuccess() err = %v", err)
		} else if !marked {
			t.Fatalf("expected scheduled completion to update row")
		}

		active, ok, err := store.getActiveSyncJobByScope(ctx, syncJobLaneFull, "", "")
		if err != nil {
			t.Fatalf("getActiveSyncJobByScope() err = %v", err)
		}
		if !ok {
			t.Fatalf("expected pending rerun job")
		}
		if active.TriggerKind != syncJobTriggerKindManual || active.Status != syncJobStatusPending || active.RerunRequested || active.AttemptCount != 0 {
			t.Fatalf("active rerun job = %+v", active)
		}
	})
}

func TestSyncJobStore_DuplicatePendingManualJobReturnsQueuedError(t *testing.T) {
	t.Parallel()

	withSyncJobsTestDB(t, func(ctx context.Context, store *dbSyncJobStore) {
		if err := store.EnqueueManualSyncJob(ctx, syncJobLaneFull, "", ""); err != nil {
			t.Fatalf("EnqueueManualSyncJob() err = %v", err)
		}
		if err := store.EnqueueManualSyncJob(ctx, syncJobLaneFull, "", ""); !errors.Is(err, errPendingSyncJobExists) {
			t.Fatalf("EnqueueManualSyncJob() err = %v, want errPendingSyncJobExists", err)
		}
	})
}

func TestSyncJobStore_DuplicateClaimedManualJobReturnsBusyError(t *testing.T) {
	t.Parallel()

	withSyncJobsTestDB(t, func(ctx context.Context, store *dbSyncJobStore) {
		if err := store.EnqueueManualSyncJob(ctx, syncJobLaneFull, "", ""); err != nil {
			t.Fatalf("EnqueueManualSyncJob() err = %v", err)
		}

		job, ok, err := store.ClaimNextSyncJobByLane(ctx, syncJobLaneFull, "claimant", 60)
		if err != nil {
			t.Fatalf("ClaimNextSyncJobByLane() err = %v", err)
		}
		if !ok {
			t.Fatalf("expected claimable sync job")
		}
		if !job.ID.Valid {
			t.Fatalf("claimed job = %+v", job)
		}

		if err := store.EnqueueManualSyncJob(ctx, syncJobLaneFull, "", ""); !errors.Is(err, errActiveSyncJobExists) {
			t.Fatalf("EnqueueManualSyncJob() err = %v, want errActiveSyncJobExists", err)
		}
	})
}

func withSyncJobsTestDB(t *testing.T, fn func(context.Context, *dbSyncJobStore)) {
	t.Helper()

	testdb.WithDatabase(t, testdb.Options{NamePrefix: "opensspm_syncjobs"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		store, ok := NewSyncJobStore(pool).(*dbSyncJobStore)
		if !ok || store == nil {
			t.Fatalf("NewSyncJobStore() did not return *dbSyncJobStore")
		}

		fn(ctx, store)
	})
}

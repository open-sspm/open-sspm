package sync

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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

	baseURL := strings.TrimSpace(os.Getenv("OPENSSPM_TEST_DATABASE_URL"))
	if baseURL == "" {
		t.Skip("OPENSSPM_TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	adminURL, err := testDatabaseAdminURL(baseURL)
	if err != nil {
		t.Fatalf("testDatabaseAdminURL() err = %v", err)
	}

	adminConn, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("pgx.Connect(admin) err = %v", err)
	}
	defer adminConn.Close(ctx)

	dbName := "opensspm_syncjobs_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := adminConn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize()); err != nil {
		t.Fatalf("CREATE DATABASE err = %v", err)
	}
	defer func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		_, _ = adminConn.Exec(dropCtx, "DROP DATABASE IF EXISTS "+pgx.Identifier{dbName}.Sanitize()+" WITH (FORCE)")
	}()

	testURL, err := testDatabaseURLWithName(baseURL, dbName)
	if err != nil {
		t.Fatalf("testDatabaseURLWithName() err = %v", err)
	}
	if err := applyTestMigrations(testURL); err != nil {
		t.Fatalf("applyTestMigrations() err = %v", err)
	}

	pool, err := pgxpool.New(ctx, testURL)
	if err != nil {
		t.Fatalf("pgxpool.New() err = %v", err)
	}
	defer pool.Close()

	store, ok := NewSyncJobStore(pool).(*dbSyncJobStore)
	if !ok || store == nil {
		t.Fatalf("NewSyncJobStore() did not return *dbSyncJobStore")
	}

	fn(ctx, store)
}

func testDatabaseAdminURL(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	parsed.Path = "/postgres"
	return parsed.String(), nil
}

func testDatabaseURLWithName(raw, dbName string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	parsed.Path = "/" + dbName
	return parsed.String(), nil
}

func applyTestMigrations(databaseURL string) error {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return fmt.Errorf("runtime.Caller failed")
	}
	migrationsDir := filepath.Join(filepath.Dir(file), "..", "..", "db", "migrations")
	m, err := migrate.New("file://"+migrationsDir, databaseURL)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = m.Close()
	}()
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return err
	}
	return nil
}

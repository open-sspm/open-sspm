package inbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/records"
	"github.com/open-sspm/open-sspm/internal/testdb"
)

func TestDeliveryDedupeHashIncludesChannel(t *testing.T) {
	source := records.SourceRef{Kind: "okta", Name: "example.okta.com"}
	eventHook := DeliveryDedupeHash(source, "event_hook", "provider:evt-1")
	eventBridge := DeliveryDedupeHash(source, "eventbridge", "provider:evt-1")
	if string(eventHook) == string(eventBridge) {
		t.Fatal("expected channel to be part of delivery dedupe hash")
	}
}

func TestStoreEnqueueClaimAndProcessor(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "event_inbox"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)
		store := NewStore(gen.New(pool))

		delivery := Delivery{
			Source:    records.SourceRef{Kind: "okta", Name: "example.okta.com"},
			Channel:   "event_hook",
			DedupeKey: "delivery:evt-1",
			RawBody:   []byte(`{"eventId":"evt-1"}`),
		}
		first, err := store.Enqueue(ctx, delivery)
		if err != nil {
			t.Fatalf("enqueue first delivery: %v", err)
		}
		second, err := store.Enqueue(ctx, delivery)
		if err != nil {
			t.Fatalf("enqueue duplicate delivery: %v", err)
		}
		if first.ID != second.ID {
			t.Fatal("expected duplicate enqueue to return existing inbox id")
		}

		processor := NewProcessor(store, testHandler{}, ProcessorConfig{LeaseOwner: "test-worker"})
		result, err := processor.RunOnce(ctx)
		if err != nil {
			t.Fatalf("processor run once: %v", err)
		}
		if result.Claimed != 1 || result.Processed != 1 {
			t.Fatalf("result = %s, want one processed delivery", result.String())
		}

		var processed int64
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM event_inbox WHERE status = 'processed'").Scan(&processed); err != nil {
			t.Fatalf("count processed inbox rows: %v", err)
		}
		if processed != 1 {
			t.Fatalf("processed rows = %d, want 1", processed)
		}
	})
}

func TestProcessorCountsDeadLetterAfterMaxAttempts(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "event_inbox"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)
		store := NewStore(gen.New(pool))

		if _, err := store.Enqueue(ctx, Delivery{
			Source:    records.SourceRef{Kind: "okta", Name: "example.okta.com"},
			Channel:   "event_hook",
			DedupeKey: "delivery:evt-dead",
			RawBody:   []byte(`{"eventId":"evt-dead"}`),
		}); err != nil {
			t.Fatalf("enqueue delivery: %v", err)
		}

		processor := NewProcessor(store, failingHandler{}, ProcessorConfig{
			LeaseOwner:  "test-worker",
			MaxAttempts: 1,
		})
		result, err := processor.RunOnce(ctx)
		if err != nil {
			t.Fatalf("processor run once: %v", err)
		}
		if result.Claimed != 1 || result.Dead != 1 || result.Retried != 0 {
			t.Fatalf("result = %s, want one dead-lettered delivery", result.String())
		}

		var status, lastError string
		if err := pool.QueryRow(ctx, `
			SELECT status::text, last_error
			FROM event_inbox
			WHERE dedupe_key = 'delivery:evt-dead'
		`).Scan(&status, &lastError); err != nil {
			t.Fatalf("select dead-lettered inbox row: %v", err)
		}
		if status != StatusDead {
			t.Fatalf("status = %q, want %q", status, StatusDead)
		}
		if lastError != "processor failed" {
			t.Fatalf("last_error = %q, want processor failed", lastError)
		}
	})
}

func TestRenewEventInboxLeaseCanBeatExpiredRequeue(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "event_inbox"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)
		q := gen.New(pool)
		store := NewStore(q)

		if _, err := store.Enqueue(ctx, Delivery{
			Source:    records.SourceRef{Kind: "okta", Name: "example.okta.com"},
			Channel:   "event_hook",
			DedupeKey: "delivery:evt-renew",
			RawBody:   []byte(`{"eventId":"evt-renew"}`),
		}); err != nil {
			t.Fatalf("enqueue delivery: %v", err)
		}
		claimed, err := store.ClaimQueued(ctx, 1, "worker-1", 1)
		if err != nil {
			t.Fatalf("claim delivery: %v", err)
		}
		if len(claimed) != 1 {
			t.Fatalf("claimed rows = %d, want 1", len(claimed))
		}
		if _, err := pool.Exec(ctx, `
			UPDATE event_inbox
			SET lease_until = now() - interval '1 second'
			WHERE id = $1
		`, claimed[0].ID); err != nil {
			t.Fatalf("expire lease: %v", err)
		}

		renewed, err := q.RenewEventInboxLease(ctx, gen.RenewEventInboxLeaseParams{
			Ids:          []int64{claimed[0].ID},
			LeaseOwner:   "worker-1",
			LeaseSeconds: 60,
		})
		if err != nil {
			t.Fatalf("RenewEventInboxLease(): %v", err)
		}
		if renewed != 1 {
			t.Fatalf("renewed rows = %d, want 1", renewed)
		}

		var leaseActive bool
		if err := pool.QueryRow(ctx, `
			SELECT status = 'processing' AND lease_owner = 'worker-1' AND lease_until > now()
			FROM event_inbox
			WHERE id = $1
		`, claimed[0].ID).Scan(&leaseActive); err != nil {
			t.Fatalf("select renewed lease: %v", err)
		}
		if !leaseActive {
			t.Fatal("expected expired processing lease to be renewed before requeue")
		}
	})
}

func TestProcessorRenewsLeaseWhileProcessingSlowDelivery(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "event_inbox_heartbeat"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)
		store := NewStore(gen.New(pool))

		if _, err := store.Enqueue(ctx, Delivery{
			Source:    records.SourceRef{Kind: "okta", Name: "example.okta.com"},
			Channel:   "event_hook",
			DedupeKey: "delivery:evt-slow",
			RawBody:   []byte(`{"eventId":"evt-slow"}`),
		}); err != nil {
			t.Fatalf("enqueue delivery: %v", err)
		}

		processor := NewProcessor(store, slowHandler{delay: 1500 * time.Millisecond}, ProcessorConfig{
			LeaseOwner: "test-worker",
			LeaseTTL:   time.Second,
		})
		result, err := processor.RunOnce(ctx)
		if err != nil {
			t.Fatalf("processor run once: %v", err)
		}
		if result.Claimed != 1 || result.Processed != 1 {
			t.Fatalf("result = %s, want one processed delivery", result.String())
		}
	})
}

type testHandler struct{}

func (testHandler) ProcessInboxDelivery(context.Context, Delivery) (ProcessResult, error) {
	return ProcessResult{
		Status:         ProcessStatusProcessed,
		DecodedSummary: map[string]any{"processed": true},
	}, nil
}

type failingHandler struct{}

func (failingHandler) ProcessInboxDelivery(context.Context, Delivery) (ProcessResult, error) {
	return ProcessResult{}, errors.New("processor failed")
}

type slowHandler struct {
	delay time.Duration
}

func (h slowHandler) ProcessInboxDelivery(ctx context.Context, _ Delivery) (ProcessResult, error) {
	timer := time.NewTimer(h.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ProcessResult{}, ctx.Err()
	case <-timer.C:
		return ProcessResult{
			Status:         ProcessStatusProcessed,
			DecodedSummary: map[string]any{"processed": true},
		}, nil
	}
}

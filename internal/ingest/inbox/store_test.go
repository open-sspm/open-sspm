package inbox

import (
	"context"
	"testing"

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

type testHandler struct{}

func (testHandler) ProcessInboxDelivery(context.Context, Delivery) (ProcessResult, error) {
	return ProcessResult{
		Status:         ProcessStatusProcessed,
		DecodedSummary: map[string]any{"processed": true},
	}, nil
}

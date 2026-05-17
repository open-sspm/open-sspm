package events

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/records"
	"github.com/open-sspm/open-sspm/internal/testdb"
)

func TestEventDedupeHashPrefersStableSourceID(t *testing.T) {
	id := int64(42)
	withNameA := EventDedupeHash(records.SourceRef{Kind: "Okta", ID: &id, Name: "a.example"}, "provider:1")
	withNameB := EventDedupeHash(records.SourceRef{Kind: "okta", ID: &id, Name: "b.example"}, "provider:1")
	if string(withNameA) != string(withNameB) {
		t.Fatal("expected source id to provide stable dedupe identity")
	}

	withoutID := EventDedupeHash(records.SourceRef{Kind: "okta", Name: "b.example"}, "provider:1")
	if string(withNameA) == string(withoutID) {
		t.Fatal("expected source name fallback to differ from source id identity")
	}
}

func TestWriteEventRejectsMissingRawPayload(t *testing.T) {
	writer := NewWriter(nil)
	_, err := writer.WriteEvent(context.Background(), records.EventRecord{}, WriteOptions{})
	if err == nil {
		t.Fatal("expected unconfigured writer to fail")
	}
}

func TestWriteEventInsertsOnceAndPreservesTargets(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "events_writer"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		writer := NewWriter(pool)
		record := records.EventRecord{
			Source: records.SourceRef{
				Kind: "okta",
				Name: "example.okta.com",
			},
			Channel:         "event_hook",
			ProviderEventID: "evt-1",
			DedupeKeyValue:  "provider:evt-1",
			EventType:       "user.lifecycle.create",
			Category:        "identity",
			Action:          "create",
			OccurredAt:      time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC),
			Actor: records.ActorRef{
				Kind:  "user",
				ID:    "00u-admin",
				Email: "admin@example.com",
			},
			Targets: []records.TargetRef{
				{Kind: "user", ID: "00u-1", Email: "user1@example.com"},
				{Kind: "group", ID: "00g-1", Name: "Admins"},
			},
			Raw: map[string]any{"uuid": "evt-1"},
		}

		first, err := writer.WriteEvent(ctx, record, WriteOptions{})
		if err != nil {
			t.Fatalf("first write failed: %v", err)
		}
		if !first.Inserted {
			t.Fatal("expected first write to insert")
		}
		second, err := writer.WriteEvent(ctx, record, WriteOptions{})
		if err != nil {
			t.Fatalf("duplicate write failed: %v", err)
		}
		if second.Inserted {
			t.Fatal("expected duplicate write to be deduped")
		}
		if first.EventID != second.EventID {
			t.Fatal("expected duplicate result to return original event id")
		}

		var eventCount, targetCount, duplicateCount int64
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM events").Scan(&eventCount); err != nil {
			t.Fatalf("count events: %v", err)
		}
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM event_targets").Scan(&targetCount); err != nil {
			t.Fatalf("count event targets: %v", err)
		}
		if err := pool.QueryRow(ctx, "SELECT duplicate_count FROM event_dedupe_keys WHERE event_id = $1", first.EventID).Scan(&duplicateCount); err != nil {
			t.Fatalf("get duplicate count: %v", err)
		}
		if eventCount != 1 {
			t.Fatalf("events count = %d, want 1", eventCount)
		}
		if targetCount != 2 {
			t.Fatalf("event targets count = %d, want 2", targetCount)
		}
		if duplicateCount != 1 {
			t.Fatalf("duplicate count = %d, want 1", duplicateCount)
		}
	})
}

func TestWriteEventConcurrentDuplicatesReturnOriginalEvent(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "events_writer_concurrent"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		record := records.EventRecord{
			Source:          records.SourceRef{Kind: "okta", Name: "example.okta.com"},
			Channel:         "event_hook",
			ProviderEventID: "evt-concurrent",
			DedupeKeyValue:  "provider:evt-concurrent",
			EventType:       "user.authentication.sso",
			Category:        "discovery.idp_sso",
			Action:          "user.authentication.sso",
			OccurredAt:      time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC),
			Raw:             map[string]any{"uuid": "evt-concurrent"},
		}

		const writers = 8
		results := make([]WriteResult, writers)
		errs := make([]error, writers)
		var wg sync.WaitGroup
		for i := range writers {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				results[idx], errs[idx] = NewWriter(pool).WriteEvent(ctx, record, WriteOptions{})
			}(i)
		}
		wg.Wait()

		var inserted int
		eventID := results[0].EventID
		for i, err := range errs {
			if err != nil {
				t.Fatalf("WriteEvent(%d) err = %v", i, err)
			}
			if results[i].Inserted {
				inserted++
				eventID = results[i].EventID
			}
		}
		if inserted != 1 {
			t.Fatalf("inserted = %d, want 1", inserted)
		}
		for i, result := range results {
			if result.EventID != eventID {
				t.Fatalf("result %d event id = %s, want %s", i, result.EventID, eventID)
			}
		}

		var eventCount, duplicateCount int64
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM events").Scan(&eventCount); err != nil {
			t.Fatalf("count events: %v", err)
		}
		if err := pool.QueryRow(ctx, "SELECT duplicate_count FROM event_dedupe_keys WHERE event_id = $1", eventID).Scan(&duplicateCount); err != nil {
			t.Fatalf("get duplicate count: %v", err)
		}
		if eventCount != 1 {
			t.Fatalf("events count = %d, want 1", eventCount)
		}
		if duplicateCount != writers-1 {
			t.Fatalf("duplicate count = %d, want %d", duplicateCount, writers-1)
		}
	})
}

package findings

import (
	"context"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/evaluator"
	canonevents "github.com/open-sspm/open-sspm/internal/events"
	"github.com/open-sspm/open-sspm/internal/records"
	"github.com/open-sspm/open-sspm/internal/testdb"
)

func TestEventEvaluationQueueWritesSignalsToCanonicalFindings(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "event_evaluation_findings"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		writer := canonevents.NewWriter(pool)
		writeEventEvaluationEvent(t, ctx, writer, "evt-oauth", time.Date(2026, time.May, 16, 12, 0, 0, 0, time.UTC))

		q := gen.New(pool)
		processor, err := evaluator.NewEventQueueProcessor(q, evaluator.EventQueueProcessorConfig{
			ClaimedBy:  "test",
			SignalSink: NewEventEvaluationFindingSink(q),
		})
		if err != nil {
			t.Fatalf("NewEventQueueProcessor() err = %v", err)
		}
		result, err := processor.ProcessQueued(ctx, 10)
		if err != nil {
			t.Fatalf("ProcessQueued() err = %v", err)
		}
		if result.Claimed != 1 || result.Processed != 1 || result.Signals != 1 || result.Findings != 1 {
			t.Fatalf("result = %+v, want claimed=1 processed=1 signals=1 findings=1", result)
		}

		count := countCanonicalEventEvaluationFindings(t, ctx, pool, "open")
		if count != 1 {
			t.Fatalf("canonical event evaluation findings = %d, want 1", count)
		}
	})
}

func TestEventEvaluationFindingSinkSkipsInvalidCanonicalFindingRows(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "event_evaluation_findings_skip_invalid"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		sink := NewEventEvaluationFindingSink(gen.New(pool))
		written, err := sink.WriteEventSignal(ctx, evaluator.EventSignal{
			EventReceivedAt: pgtype.Timestamptz{Time: time.Date(2026, time.May, 16, 14, 0, 0, 0, time.UTC), Valid: true},
			EventID:         pgtype.UUID{Bytes: [16]byte{0, 0, 0, 0, 0, 0, 0x40, 0, 0x80, 0, 0, 0, 0, 0, 0, 1}, Valid: true},
			SourceKind:      "okta",
			SourceName:      "",
			EntityKind:      "okta_user",
			EntityID:        "00u1",
			EntityName:      "Alice",
			Signal: evaluator.RiskSignal{
				ID:                "sig-poison",
				PolicyPackID:      "pack",
				PolicyPackVersion: "v1",
				Severity:          "high",
				Title:             "Poison signal",
				Evidence:          "missing source name",
			},
			Output:      []byte(`{"signal_id":"sig-poison"}`),
			EvaluatedAt: time.Date(2026, time.May, 16, 14, 0, 0, 0, time.UTC),
		})
		if err != nil {
			t.Fatalf("WriteEventSignal() err = %v", err)
		}
		if written {
			t.Fatal("WriteEventSignal() wrote invalid finding, want skipped")
		}
		if count := countCanonicalEventEvaluationFindings(t, ctx, pool, "open"); count != 0 {
			t.Fatalf("canonical event evaluation findings = %d, want 0", count)
		}
	})
}

func writeEventEvaluationEvent(t *testing.T, ctx context.Context, writer *canonevents.Writer, providerEventID string, occurredAt time.Time) {
	t.Helper()
	if _, err := writer.WriteEvent(ctx, records.EventRecord{
		Source:          records.SourceRef{Kind: "okta", Name: "example.okta.com"},
		Channel:         "event_hook",
		ProviderEventID: providerEventID,
		DedupeKeyValue:  "provider:" + providerEventID,
		EventType:       "app.oauth2.signon",
		Category:        "discovery.oauth_grant",
		Action:          "app.oauth2.signon",
		OccurredAt:      occurredAt,
		Targets:         []records.TargetRef{{Kind: "okta_app", ID: "0oa1", Name: "Payroll"}},
		Raw:             map[string]any{"uuid": providerEventID},
	}, canonevents.WriteOptions{}); err != nil {
		t.Fatalf("WriteEvent(%s) err = %v", providerEventID, err)
	}
}

func countCanonicalEventEvaluationFindings(t *testing.T, ctx context.Context, pool *pgxpool.Pool, status string) int64 {
	t.Helper()

	var count int64
	if err := pool.QueryRow(ctx, `
		SELECT count(*)::bigint
		FROM findings
		WHERE finding_key LIKE 'event_evaluation:%'
		  AND status = $1
	`, status).Scan(&count); err != nil {
		t.Fatalf("count canonical event evaluation findings: %v", err)
	}
	return count
}

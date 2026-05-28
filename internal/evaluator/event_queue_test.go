package evaluator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	canonevents "github.com/open-sspm/open-sspm/internal/events"
	"github.com/open-sspm/open-sspm/internal/records"
	"github.com/open-sspm/open-sspm/internal/testdb"
)

func TestEventQueueProcessorEvaluatesQueuedCanonicalEvent(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "event_evaluation_queue"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		writer := canonevents.NewWriter(pool)
		writeResult, err := writer.WriteEvent(ctx, records.EventRecord{
			Source:          records.SourceRef{Kind: "okta", Name: "example.okta.com"},
			Channel:         "event_hook",
			ProviderEventID: "evt-oauth",
			DedupeKeyValue:  "provider:evt-oauth",
			EventType:       "app.oauth2.signon",
			Category:        "discovery.oauth_grant",
			Action:          "app.oauth2.signon",
			OccurredAt:      time.Date(2026, time.May, 16, 12, 0, 0, 0, time.UTC),
			Outcome:         records.OutcomeSuccess,
			Targets: []records.TargetRef{
				{Kind: "okta_app", ID: "0oa1", Name: "Payroll"},
			},
			Raw: map[string]any{"uuid": "evt-oauth"},
		}, canonevents.WriteOptions{})
		if err != nil {
			t.Fatalf("WriteEvent() err = %v", err)
		}

		q := gen.New(pool)
		var queued int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_evaluation_queue WHERE event_id = $1`, writeResult.EventID).Scan(&queued); err != nil {
			t.Fatalf("count queue rows: %v", err)
		}
		if queued != 1 {
			t.Fatalf("queued = %d, want 1", queued)
		}

		sink := &recordingEventSignalSink{}
		processor, err := NewEventQueueProcessor(q, EventQueueProcessorConfig{ClaimedBy: "test", SignalSink: sink})
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

		if len(sink.signals) != 1 {
			t.Fatalf("captured signals = %d, want 1", len(sink.signals))
		}
		if got := sink.signals[0].Signal.ID; got != "event.oauth_grant" {
			t.Fatalf("signal id = %q, want event.oauth_grant", got)
		}
	})
}

func TestEventQueueProcessorDeadLettersAfterMaxAttempts(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "event_evaluation_queue_dead"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		writer := canonevents.NewWriter(pool)
		writeResult, err := writer.WriteEvent(ctx, records.EventRecord{
			Source:          records.SourceRef{Kind: "okta", Name: "example.okta.com"},
			Channel:         "event_hook",
			ProviderEventID: "evt-poison",
			DedupeKeyValue:  "provider:evt-poison",
			EventType:       "system.event",
			Category:        "okta.system_log",
			Action:          "system.event",
			OccurredAt:      time.Date(2026, time.May, 16, 12, 0, 0, 0, time.UTC),
			Severity:        0,
			Raw:             map[string]any{"uuid": "evt-poison"},
		}, canonevents.WriteOptions{})
		if err != nil {
			t.Fatalf("WriteEvent() err = %v", err)
		}

		q := gen.New(pool)
		if _, err := pool.Exec(ctx, `
			UPDATE event_evaluation_queue
			SET attempts = 1
			WHERE event_received_at = $1
			  AND event_id = $2
		`, writeResult.ReceivedAt, writeResult.EventID); err != nil {
			t.Fatalf("seed attempts: %v", err)
		}

		evaluator, err := NewEventEvaluator([]EventRule{
			{
				ID:       "event.poison",
				Severity: SeverityLow,
				Title:    "Poison event",
				Match: func(EventInput) (bool, error) {
					return false, errors.New("poison event")
				},
			},
		})
		if err != nil {
			t.Fatalf("NewEventEvaluator() err = %v", err)
		}
		processor, err := NewEventQueueProcessor(q, EventQueueProcessorConfig{
			ClaimedBy:   "test",
			MaxAttempts: 2,
			Evaluator:   evaluator,
		})
		if err != nil {
			t.Fatalf("NewEventQueueProcessor() err = %v", err)
		}

		result, err := processor.ProcessQueued(ctx, 10)
		if err != nil {
			t.Fatalf("ProcessQueued() err = %v", err)
		}
		if result.Claimed != 1 || result.Dead != 1 || result.Retried != 0 || result.Processed != 0 {
			t.Fatalf("result = %+v, want claimed=1 dead=1", result)
		}

		var status, lastError string
		var attempts int32
		if err := pool.QueryRow(ctx, `
			SELECT status, attempts, last_error
			FROM event_evaluation_queue
			WHERE event_received_at = $1
			  AND event_id = $2
		`, writeResult.ReceivedAt, writeResult.EventID).Scan(&status, &attempts, &lastError); err != nil {
			t.Fatalf("load queue row: %v", err)
		}
		if status != "dead" {
			t.Fatalf("status = %q, want dead", status)
		}
		if attempts != 2 {
			t.Fatalf("attempts = %d, want 2", attempts)
		}
		if lastError == "" {
			t.Fatalf("last_error is empty, want evaluation error")
		}
	})
}

func TestEventQueueProcessorRenewsLeaseWhileProcessingSlowEvent(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "event_evaluation_queue_heartbeat"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		writer := canonevents.NewWriter(pool)
		if _, err := writer.WriteEvent(ctx, records.EventRecord{
			Source:          records.SourceRef{Kind: "okta", Name: "example.okta.com"},
			Channel:         "event_hook",
			ProviderEventID: "evt-slow-eval",
			DedupeKeyValue:  "provider:evt-slow-eval",
			EventType:       "app.oauth2.signon",
			Category:        "discovery.oauth_grant",
			Action:          "app.oauth2.signon",
			OccurredAt:      time.Date(2026, time.May, 16, 12, 0, 0, 0, time.UTC),
			Raw:             map[string]any{"uuid": "evt-slow-eval"},
		}, canonevents.WriteOptions{}); err != nil {
			t.Fatalf("WriteEvent() err = %v", err)
		}

		evaluator, err := NewEventEvaluator([]EventRule{
			{
				ID:       "event.slow",
				Severity: SeverityLow,
				Title:    "Slow event",
				Match: func(EventInput) (bool, error) {
					time.Sleep(1500 * time.Millisecond)
					return true, nil
				},
			},
		})
		if err != nil {
			t.Fatalf("NewEventEvaluator() err = %v", err)
		}
		processor, err := NewEventQueueProcessor(gen.New(pool), EventQueueProcessorConfig{
			ClaimedBy: "test",
			Lease:     time.Second,
			Evaluator: evaluator,
		})
		if err != nil {
			t.Fatalf("NewEventQueueProcessor() err = %v", err)
		}

		result, err := processor.ProcessQueued(ctx, 10)
		if err != nil {
			t.Fatalf("ProcessQueued() err = %v", err)
		}
		if result.Claimed != 1 || result.Processed != 1 || result.LeaseLost != 0 {
			t.Fatalf("result = %+v, want claimed=1 processed=1 lease_lost=0", result)
		}
	})
}

type recordingEventSignalSink struct {
	signals []EventSignal
}

func (s *recordingEventSignalSink) WriteEventSignal(_ context.Context, signal EventSignal) (bool, error) {
	s.signals = append(s.signals, signal)
	return true, nil
}

func TestEventQueueProcessorDoesNotMarkExpiredLease(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "event_evaluation_queue_lease"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		writer := canonevents.NewWriter(pool)
		writeResult, err := writer.WriteEvent(ctx, records.EventRecord{
			Source:          records.SourceRef{Kind: "okta", Name: "example.okta.com"},
			Channel:         "event_hook",
			ProviderEventID: "evt-expired-lease",
			DedupeKeyValue:  "provider:evt-expired-lease",
			EventType:       "app.oauth2.signon",
			Category:        "discovery.oauth_grant",
			Action:          "app.oauth2.signon",
			OccurredAt:      time.Date(2026, time.May, 16, 12, 0, 0, 0, time.UTC),
			Raw:             map[string]any{"uuid": "evt-expired-lease"},
		}, canonevents.WriteOptions{})
		if err != nil {
			t.Fatalf("WriteEvent() err = %v", err)
		}

		q := gen.New(pool)
		rows, err := q.ClaimEventEvaluations(ctx, gen.ClaimEventEvaluationsParams{
			ClaimedBy:    "test",
			LeaseSeconds: 1,
			LimitRows:    10,
		})
		if err != nil {
			t.Fatalf("ClaimEventEvaluations() err = %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("claimed rows = %d, want 1", len(rows))
		}
		if _, err := pool.Exec(ctx, `
			UPDATE event_evaluation_queue
			SET lease_until = clock_timestamp() - interval '1 second'
			WHERE event_received_at = $1
			  AND event_id = $2
		`, writeResult.ReceivedAt, writeResult.EventID); err != nil {
			t.Fatalf("expire lease: %v", err)
		}

		updated, err := q.MarkEventEvaluationProcessed(ctx, gen.MarkEventEvaluationProcessedParams{
			ID:        rows[0].ID,
			ClaimedBy: "test",
		})
		if err != nil {
			t.Fatalf("MarkEventEvaluationProcessed() err = %v", err)
		}
		if updated != 0 {
			t.Fatalf("processed rows = %d, want 0 for expired lease", updated)
		}

		processor, err := NewEventQueueProcessor(q, EventQueueProcessorConfig{ClaimedBy: "test"})
		if err != nil {
			t.Fatalf("NewEventQueueProcessor() err = %v", err)
		}
		_, marked, err := processor.markFailedRow(ctx, rows[0], errors.New("evaluation failed"))
		if err != nil {
			t.Fatalf("markFailedRow() err = %v", err)
		}
		if marked {
			t.Fatal("markFailedRow() marked an expired lease")
		}
	})
}

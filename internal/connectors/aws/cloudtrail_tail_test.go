package aws

import (
	"context"
	"testing"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	cloudtrailtypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/testdb"
)

type fakeCloudTrail struct {
	events []cloudtrailtypes.Event
	err    error
}

func (f fakeCloudTrail) LookupEvents(ctx context.Context, in *cloudtrail.LookupEventsInput, optFns ...func(*cloudtrail.Options)) (*cloudtrail.LookupEventsOutput, error) {
	_ = ctx
	_ = in
	_ = optFns
	if f.err != nil {
		return nil, f.err
	}
	return &cloudtrail.LookupEventsOutput{Events: f.events}, nil
}

func TestAWSCloudTrailTailWritesCanonicalEventsAndAdvancesCursor(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "aws_cloudtrail_tail"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)
		q := gen.New(pool)

		sourceName := "prod"
		runID, err := registry.StartSyncRun(ctx, q, registry.SyncRunSourceKind("aws", registry.RunModeTail), sourceName)
		if err != nil {
			t.Fatalf("StartSyncRun() err = %v", err)
		}

		occurredAt := time.Date(2026, time.May, 16, 12, 0, 0, 0, time.UTC)
		client := &Client{
			region: "us-east-1",
			cloudtrail: fakeCloudTrail{events: []cloudtrailtypes.Event{
				{
					EventId:         awssdk.String("evt-1"),
					EventName:       awssdk.String("CreateAccountAssignment"),
					EventSource:     awssdk.String("sso.amazonaws.com"),
					EventTime:       awssdk.Time(occurredAt),
					Username:        awssdk.String("alice"),
					CloudTrailEvent: awssdk.String(`{"eventID":"evt-1","eventSource":"sso.amazonaws.com","eventName":"CreateAccountAssignment"}`),
				},
			}},
		}
		integration := NewAWSIntegration(client, sourceName)

		stats, err := integration.tailCloudTrailWithCursor(ctx, pool, runID, AWSCloudTrailTailResource)
		if err != nil {
			t.Fatalf("tailCloudTrailWithCursor() err = %v", err)
		}
		if stats.Fetched != 1 || stats.Written != 1 || stats.Duplicates != 0 {
			t.Fatalf("tail stats = %+v, want fetched=1 written=1 duplicates=0", stats)
		}

		var eventCount, targetCount int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE source_kind = 'aws' AND source_name = $1`, sourceName).Scan(&eventCount); err != nil {
			t.Fatalf("count canonical events: %v", err)
		}
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_targets`).Scan(&targetCount); err != nil {
			t.Fatalf("count event targets: %v", err)
		}
		if eventCount != 1 {
			t.Fatalf("event count = %d, want 1", eventCount)
		}
		if targetCount != 1 {
			t.Fatalf("target count = %d, want 1", targetCount)
		}

		cursor, err := q.GetConnectorCursorState(ctx, gen.GetConnectorCursorStateParams{
			SourceKind: "aws",
			SourceName: sourceName,
			Resource:   AWSCloudTrailTailResource,
		})
		if err != nil {
			t.Fatalf("GetConnectorCursorState() err = %v", err)
		}
		if !cursor.Watermark.Valid || !cursor.Watermark.Time.Equal(occurredAt) {
			t.Fatalf("cursor watermark = %v, want %v", cursor.Watermark, occurredAt)
		}
		if !cursor.LastRunID.Valid || cursor.LastRunID.Int64 != runID {
			t.Fatalf("cursor last_run_id = %+v, want %d", cursor.LastRunID, runID)
		}
		if cursor.LastProviderEventID != "evt-1" {
			t.Fatalf("last provider event id = %q, want evt-1", cursor.LastProviderEventID)
		}
	})
}

func TestAWSCloudTrailTailDoesNotAdvanceCursorAfterPartialWriteFailure(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "aws_cloudtrail_tail_failure"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)
		q := gen.New(pool)

		sourceName := "prod"
		runID, err := registry.StartSyncRun(ctx, q, registry.SyncRunSourceKind("aws", registry.RunModeTail), sourceName)
		if err != nil {
			t.Fatalf("StartSyncRun() err = %v", err)
		}

		occurredAt := time.Date(2026, time.May, 16, 12, 0, 0, 0, time.UTC)
		client := &Client{
			region: "us-east-1",
			cloudtrail: fakeCloudTrail{events: []cloudtrailtypes.Event{
				{
					EventId:         awssdk.String("evt-good"),
					EventName:       awssdk.String("CreateAccountAssignment"),
					EventSource:     awssdk.String("sso.amazonaws.com"),
					EventTime:       awssdk.Time(occurredAt),
					Username:        awssdk.String("alice"),
					CloudTrailEvent: awssdk.String(`{"eventID":"evt-good","eventSource":"sso.amazonaws.com","eventName":"CreateAccountAssignment"}`),
				},
				{
					EventId:         awssdk.String("evt-bad"),
					EventName:       awssdk.String("DeleteAccountAssignment"),
					EventSource:     awssdk.String("sso.amazonaws.com"),
					EventTime:       awssdk.Time(occurredAt.Add(time.Minute)),
					Username:        awssdk.String("alice"),
					CloudTrailEvent: awssdk.String(`{`),
				},
			}},
		}
		integration := NewAWSIntegration(client, sourceName)

		stats, err := integration.tailCloudTrailWithCursor(ctx, pool, runID, AWSCloudTrailTailResource)
		if err == nil {
			t.Fatalf("tailCloudTrailWithCursor() err = nil, want bad raw JSON error")
		}
		if stats.Fetched != 2 || stats.Written != 1 {
			t.Fatalf("tail stats = %+v, want fetched=2 written=1 before failure", stats)
		}

		var eventCount int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE source_kind = 'aws' AND source_name = $1`, sourceName).Scan(&eventCount); err != nil {
			t.Fatalf("count canonical events: %v", err)
		}
		if eventCount != 1 {
			t.Fatalf("event count = %d, want first event to remain durable", eventCount)
		}

		cursor, err := q.GetConnectorCursorState(ctx, gen.GetConnectorCursorStateParams{
			SourceKind: "aws",
			SourceName: sourceName,
			Resource:   AWSCloudTrailTailResource,
		})
		if err != nil {
			t.Fatalf("GetConnectorCursorState() err = %v", err)
		}
		if cursor.Watermark.Valid {
			t.Fatalf("cursor watermark = %+v, want no success watermark after failure", cursor.Watermark)
		}
		if cursor.LastSuccessAt.Valid {
			t.Fatalf("cursor last_success_at = %+v, want invalid after failure", cursor.LastSuccessAt)
		}
		if cursor.LastError == "" {
			t.Fatalf("cursor last_error is empty, want failure recorded")
		}
	})
}

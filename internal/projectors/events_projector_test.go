package projectors

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/discovery"
	canonevents "github.com/open-sspm/open-sspm/internal/events"
	"github.com/open-sspm/open-sspm/internal/records"
	"github.com/open-sspm/open-sspm/internal/testdb"
)

func TestCompareDiscoveryEventKeys(t *testing.T) {
	t.Parallel()

	projected := []DiscoveryEventKey{
		{SourceKind: "Okta", SourceName: "example.okta.com", SignalKind: discovery.SignalKindIDPSSO, EventExternalID: "evt-1"},
		{SourceKind: "okta", SourceName: "example.okta.com", SignalKind: discovery.SignalKindOAuth, EventExternalID: "evt-2"},
	}
	baseline := []DiscoveryEventKey{
		{SourceKind: "okta", SourceName: "example.okta.com", SignalKind: discovery.SignalKindIDPSSO, EventExternalID: "evt-1"},
		{SourceKind: "okta", SourceName: "example.okta.com", SignalKind: discovery.SignalKindAssignment, EventExternalID: "evt-3"},
	}

	diff := CompareDiscoveryEventKeys(projected, baseline)
	if diff.MatchingCount != 1 || len(diff.MissingInProjection) != 1 || len(diff.MissingInBaseline) != 1 {
		t.Fatalf("diff = %+v, want one match and one miss on each side", diff)
	}
}

func TestEventProjectorProjectsCanonicalDiscoveryAndDiffsBaseline(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "event_projector"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		q := gen.New(pool)
		sourceName := "example.okta.com"
		observedAt := time.Date(2026, time.May, 16, 12, 0, 0, 0, time.UTC)
		runID, err := registry.StartSyncRun(ctx, q, registry.SyncRunSourceKind("okta", registry.RunModeDiscovery), sourceName)
		if err != nil {
			t.Fatalf("StartSyncRun() err = %v", err)
		}
		if err := discovery.WriteRows(ctx, q, discovery.WriteRowsParams{
			SourceKind: "okta",
			SourceName: sourceName,
			RunID:      runID,
			Sources: []discovery.SourceRow{
				{
					CanonicalKey:     "okta:example:0oa1",
					SourceAppID:      "0oa1",
					SourceAppName:    "Payroll",
					SourceAppDomain:  "https://payroll.example.com",
					SourceVendorName: "Payroll",
					SeenAt:           observedAt,
				},
			},
			Events: []discovery.EventRow{
				{
					CanonicalKey:     "okta:example:0oa1",
					SignalKind:       discovery.SignalKindIDPSSO,
					EventExternalID:  "evt-1",
					SourceAppID:      "0oa1",
					SourceAppName:    "Payroll",
					SourceAppDomain:  "https://payroll.example.com",
					ActorExternalID:  "00u1",
					ActorEmail:       "alice@example.com",
					ActorDisplayName: "Alice",
					ObservedAt:       observedAt,
					RawJSON:          []byte(`{"uuid":"evt-1"}`),
				},
			},
		}); err != nil {
			t.Fatalf("WriteRows() err = %v", err)
		}
		if _, err := q.PromoteSaaSAppEventsSeenInRunBySource(ctx, gen.PromoteSaaSAppEventsSeenInRunBySourceParams{
			SourceKind:        "okta",
			SourceName:        sourceName,
			LastObservedRunID: runID,
		}); err != nil {
			t.Fatalf("PromoteSaaSAppEventsSeenInRunBySource() err = %v", err)
		}

		writer := canonevents.NewWriter(pool)
		if _, err := writer.WriteEvent(ctx, records.EventRecord{
			Source:          records.SourceRef{Kind: "okta", Name: sourceName},
			Channel:         "event_hook",
			ProviderEventID: "evt-1",
			DedupeKeyValue:  "provider:evt-1",
			EventType:       "user.authentication.sso",
			Category:        "discovery.idp_sso",
			Action:          "user.authentication.sso",
			OccurredAt:      observedAt,
			Actor: records.ActorRef{
				Kind:        "okta_actor",
				ID:          "00u1",
				Email:       "alice@example.com",
				DisplayName: "Alice",
			},
			Targets: []records.TargetRef{
				{
					Kind: "okta_app",
					ID:   "0oa1",
					Name: "Payroll",
					Envelope: map[string]any{
						"domain": "https://payroll.example.com",
					},
				},
			},
			Raw: map[string]any{"uuid": "evt-1"},
		}, canonevents.WriteOptions{ReceivedAt: observedAt}); err != nil {
			t.Fatalf("WriteEvent() err = %v", err)
		}

		projector := NewEventProjector(q)
		result, err := projector.ProjectDiscovery(ctx, DiscoveryProjectionParams{
			SourceKind: "okta",
			SourceName: sourceName,
		})
		if err != nil {
			t.Fatalf("ProjectDiscovery() err = %v", err)
		}
		if result.Projected != 1 {
			t.Fatalf("projected = %d, want 1", result.Projected)
		}

		diff, err := projector.DiffDiscovery(ctx, DiscoveryProjectionParams{
			SourceKind: "okta",
			SourceName: sourceName,
		})
		if err != nil {
			t.Fatalf("DiffDiscovery() err = %v", err)
		}
		if diff.BaselineCount != 1 || diff.ProjectedCount != 1 || diff.MatchingCount != 1 {
			t.Fatalf("diff = %+v, want 1/1/1 parity", diff)
		}
		if len(diff.MissingInBaseline) != 0 || len(diff.MissingInProjection) != 0 {
			t.Fatalf("diff misses = baseline:%+v projection:%+v, want none", diff.MissingInBaseline, diff.MissingInProjection)
		}
	})
}

func TestEventProjectorResumesFromCheckpoint(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "event_projector_checkpoint"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		q := gen.New(pool)
		sourceName := "example.okta.com"
		occurredAt := time.Date(2026, time.May, 16, 12, 0, 0, 0, time.UTC)
		receivedAt1 := occurredAt.Add(time.Second)
		receivedAt2 := occurredAt.Add(2 * time.Second)
		writer := canonevents.NewWriter(pool)
		for idx, receivedAt := range []time.Time{receivedAt1, receivedAt2} {
			providerEventID := fmt.Sprintf("evt-checkpoint-%d", idx+1)
			if _, err := writer.WriteEvent(ctx, records.EventRecord{
				Source:          records.SourceRef{Kind: "okta", Name: sourceName},
				Channel:         "event_hook",
				ProviderEventID: providerEventID,
				DedupeKeyValue:  "provider:" + providerEventID,
				EventType:       "user.authentication.sso",
				Category:        "discovery.idp_sso",
				Action:          "user.authentication.sso",
				OccurredAt:      occurredAt.Add(time.Duration(idx) * time.Second),
				Targets: []records.TargetRef{
					{Kind: "okta_app", ID: fmt.Sprintf("0oa%d", idx+1), Name: "App"},
				},
				Raw: map[string]any{"uuid": providerEventID},
			}, canonevents.WriteOptions{ReceivedAt: receivedAt}); err != nil {
				t.Fatalf("WriteEvent(%s) err = %v", providerEventID, err)
			}
		}

		projector := NewEventProjector(q)
		first, err := projector.ProjectDiscovery(ctx, DiscoveryProjectionParams{
			SourceKind:           "okta",
			SourceName:           sourceName,
			Limit:                1,
			ResumeFromCheckpoint: true,
		})
		if err != nil {
			t.Fatalf("first ProjectDiscovery() err = %v", err)
		}
		if first.Projected != 1 || !first.LastReceivedAt.Equal(receivedAt1) {
			t.Fatalf("first result = %+v, want one row through %s", first, receivedAt1)
		}

		second, err := projector.ProjectDiscovery(ctx, DiscoveryProjectionParams{
			SourceKind:           "okta",
			SourceName:           sourceName,
			Limit:                10,
			ResumeFromCheckpoint: true,
		})
		if err != nil {
			t.Fatalf("second ProjectDiscovery() err = %v", err)
		}
		if second.Projected != 1 || !second.ResumedFrom.Equal(receivedAt1) || !second.LastReceivedAt.Equal(receivedAt2) {
			t.Fatalf("second result = %+v, want resume from %s through %s", second, receivedAt1, receivedAt2)
		}

		third, err := projector.ProjectDiscovery(ctx, DiscoveryProjectionParams{
			SourceKind:           "okta",
			SourceName:           sourceName,
			Limit:                10,
			ResumeFromCheckpoint: true,
		})
		if err != nil {
			t.Fatalf("third ProjectDiscovery() err = %v", err)
		}
		if third.Projected != 0 || !third.ResumedFrom.Equal(receivedAt2) || !third.LastReceivedAt.IsZero() {
			t.Fatalf("third result = %+v, want no new rows from %s", third, receivedAt2)
		}
	})
}

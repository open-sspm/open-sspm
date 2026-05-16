package findings

import (
	"context"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	canonevents "github.com/open-sspm/open-sspm/internal/events"
	"github.com/open-sspm/open-sspm/internal/records"
	"github.com/open-sspm/open-sspm/internal/riskpolicy"
	"github.com/open-sspm/open-sspm/internal/testdb"
)

func TestRiskpolicyProjectorProjectsEventShadowSignalsToFindings(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "riskpolicy_findings"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		writer := canonevents.NewWriter(pool)
		if _, err := writer.WriteEvent(ctx, records.EventRecord{
			Source:          records.SourceRef{Kind: "okta", Name: "example.okta.com"},
			Channel:         "event_hook",
			ProviderEventID: "evt-oauth",
			DedupeKeyValue:  "provider:evt-oauth",
			EventType:       "app.oauth2.signon",
			Category:        "discovery.oauth_grant",
			Action:          "app.oauth2.signon",
			OccurredAt:      time.Date(2026, time.May, 16, 12, 0, 0, 0, time.UTC),
			Targets:         []records.TargetRef{{Kind: "okta_app", ID: "0oa1", Name: "Payroll"}},
			Raw:             map[string]any{"uuid": "evt-oauth"},
		}, canonevents.WriteOptions{}); err != nil {
			t.Fatalf("WriteEvent() err = %v", err)
		}

		q := gen.New(pool)
		processor, err := riskpolicy.NewEventQueueProcessor(q, riskpolicy.EventQueueProcessorConfig{ClaimedBy: "test"})
		if err != nil {
			t.Fatalf("NewEventQueueProcessor() err = %v", err)
		}
		if _, err := processor.ProcessQueued(ctx, 10); err != nil {
			t.Fatalf("ProcessQueued() err = %v", err)
		}

		projector := NewRiskpolicyProjector(q)
		result, err := projector.ProjectEventShadowFindings(ctx, RiskpolicyProjectionParams{})
		if err != nil {
			t.Fatalf("ProjectEventShadowFindings() err = %v", err)
		}
		if result.Projected != 1 {
			t.Fatalf("projected = %d, want 1", result.Projected)
		}

		count, err := q.CountRiskpolicyFindingsByShadowStatus(ctx, gen.CountRiskpolicyFindingsByShadowStatusParams{
			Shadow: true,
			Status: "open",
		})
		if err != nil {
			t.Fatalf("CountRiskpolicyFindingsByShadowStatus() err = %v", err)
		}
		if count != 1 {
			t.Fatalf("shadow open findings = %d, want 1", count)
		}
	})
}

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

func TestRiskpolicyProjectorHonorsExplicitNonShadowMode(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "riskpolicy_findings_nonshadow"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		writer := canonevents.NewWriter(pool)
		writeRiskpolicyProjectionEvent(t, ctx, writer, "evt-nonshadow", time.Date(2026, time.May, 16, 13, 0, 0, 0, time.UTC))

		q := gen.New(pool)
		processor, err := riskpolicy.NewEventQueueProcessor(q, riskpolicy.EventQueueProcessorConfig{ClaimedBy: "test"})
		if err != nil {
			t.Fatalf("NewEventQueueProcessor() err = %v", err)
		}
		if _, err := processor.ProcessQueued(ctx, 10); err != nil {
			t.Fatalf("ProcessQueued() err = %v", err)
		}

		projector := NewRiskpolicyProjector(q)
		result, err := projector.ProjectEventShadowFindings(ctx, RiskpolicyProjectionParams{Shadow: boolPtr(false)})
		if err != nil {
			t.Fatalf("ProjectEventShadowFindings() err = %v", err)
		}
		if result.Projected != 1 {
			t.Fatalf("projected = %d, want 1", result.Projected)
		}

		openNonShadow, err := q.CountRiskpolicyFindingsByShadowStatus(ctx, gen.CountRiskpolicyFindingsByShadowStatusParams{
			Shadow: false,
			Status: "open",
		})
		if err != nil {
			t.Fatalf("CountRiskpolicyFindingsByShadowStatus(false) err = %v", err)
		}
		if openNonShadow != 1 {
			t.Fatalf("non-shadow open findings = %d, want 1", openNonShadow)
		}
		openShadow, err := q.CountRiskpolicyFindingsByShadowStatus(ctx, gen.CountRiskpolicyFindingsByShadowStatusParams{
			Shadow: true,
			Status: "open",
		})
		if err != nil {
			t.Fatalf("CountRiskpolicyFindingsByShadowStatus(true) err = %v", err)
		}
		if openShadow != 0 {
			t.Fatalf("shadow open findings = %d, want 0", openShadow)
		}
	})
}

func TestRiskpolicyProjectorBoundedProjectionSkipsCurrentFindings(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "riskpolicy_findings_limit"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		q := gen.New(pool)
		processor, err := riskpolicy.NewEventQueueProcessor(q, riskpolicy.EventQueueProcessorConfig{ClaimedBy: "test"})
		if err != nil {
			t.Fatalf("NewEventQueueProcessor() err = %v", err)
		}
		writer := canonevents.NewWriter(pool)

		base := time.Date(2026, time.May, 16, 12, 0, 0, 0, time.UTC)
		writeRiskpolicyProjectionEvent(t, ctx, writer, "evt-old-1", base)
		writeRiskpolicyProjectionEvent(t, ctx, writer, "evt-old-2", base.Add(time.Minute))
		if _, err := processor.ProcessQueued(ctx, 10); err != nil {
			t.Fatalf("ProcessQueued(old) err = %v", err)
		}

		projector := NewRiskpolicyProjector(q)
		first, err := projector.ProjectEventShadowFindings(ctx, RiskpolicyProjectionParams{Limit: 2})
		if err != nil {
			t.Fatalf("ProjectEventShadowFindings(first) err = %v", err)
		}
		if first.Projected != 2 {
			t.Fatalf("first projected = %d, want 2", first.Projected)
		}

		writeRiskpolicyProjectionEvent(t, ctx, writer, "evt-new-1", base.Add(2*time.Minute))
		writeRiskpolicyProjectionEvent(t, ctx, writer, "evt-new-2", base.Add(3*time.Minute))
		if _, err := processor.ProcessQueued(ctx, 10); err != nil {
			t.Fatalf("ProcessQueued(new) err = %v", err)
		}

		second, err := projector.ProjectEventShadowFindings(ctx, RiskpolicyProjectionParams{Limit: 2})
		if err != nil {
			t.Fatalf("ProjectEventShadowFindings(second) err = %v", err)
		}
		if second.Projected != 2 {
			t.Fatalf("second projected = %d, want 2", second.Projected)
		}

		count, err := q.CountRiskpolicyFindingsByShadowStatus(ctx, gen.CountRiskpolicyFindingsByShadowStatusParams{
			Shadow: true,
			Status: "open",
		})
		if err != nil {
			t.Fatalf("CountRiskpolicyFindingsByShadowStatus() err = %v", err)
		}
		if count != 4 {
			t.Fatalf("shadow open findings = %d, want 4; bounded projection reprocessed old findings instead of advancing", count)
		}
	})
}

func TestRiskpolicyProjectorSkipsInvalidCanonicalFindingRows(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "riskpolicy_findings_skip_invalid"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		receivedAt := time.Date(2026, time.May, 16, 14, 0, 0, 0, time.UTC)
		eventID := "00000000-0000-4000-8000-000000000001"
		if _, err := pool.Exec(ctx, `
			INSERT INTO events (
			  id,
			  received_at,
			  occurred_at,
			  source_kind,
			  source_name,
			  channel,
			  provider_event_id,
			  dedupe_key,
			  dedupe_hash,
			  event_type,
			  category,
			  raw
			) VALUES (
			  $1::uuid,
			  $2::timestamptz,
			  $2::timestamptz,
			  'okta',
			  '',
			  'event_hook',
			  'evt-poison',
			  'provider:evt-poison',
			  decode('01', 'hex'),
			  'user.lifecycle.deactivate',
			  'state_refresh.user',
			  '{}'::jsonb
			)
		`, eventID, receivedAt); err != nil {
			t.Fatalf("insert poison event err = %v", err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO riskpolicy_event_shadow_signals (
			  event_received_at,
			  event_id,
			  signal_id,
			  policy_pack_id,
			  policy_pack_version,
			  severity,
			  title,
			  evidence,
			  output,
			  evaluated_at
			) VALUES (
			  $1::timestamptz,
			  $2::uuid,
			  'sig-poison',
			  'pack',
			  'v1',
			  'high',
			  'Poison signal',
			  'missing source name',
			  '{}'::jsonb,
			  $1::timestamptz
			)
		`, receivedAt, eventID); err != nil {
			t.Fatalf("insert poison signal err = %v", err)
		}

		projector := NewRiskpolicyProjector(gen.New(pool))
		result, err := projector.ProjectEventShadowFindings(ctx, RiskpolicyProjectionParams{})
		if err != nil {
			t.Fatalf("ProjectEventShadowFindings() err = %v", err)
		}
		if result.Projected != 0 || result.Skipped != 1 {
			t.Fatalf("projection result = %+v, want projected 0 skipped 1", result)
		}
	})
}

func writeRiskpolicyProjectionEvent(t *testing.T, ctx context.Context, writer *canonevents.Writer, providerEventID string, occurredAt time.Time) {
	t.Helper()
	if _, err := writer.WriteEvent(ctx, records.EventRecord{
		Source:          records.SourceRef{Kind: "okta", Name: "example.okta.com"},
		Channel:         "event_hook",
		ProviderEventID: providerEventID,
		DedupeKeyValue:  "provider:" + providerEventID,
		EventType:       "user.lifecycle.deactivate",
		Category:        "state_refresh.user",
		Action:          "user.lifecycle.deactivate",
		OccurredAt:      occurredAt,
		Targets:         []records.TargetRef{{Kind: "okta_user", ID: providerEventID, Name: providerEventID}},
		Raw:             map[string]any{"uuid": providerEventID},
	}, canonevents.WriteOptions{}); err != nil {
		t.Fatalf("WriteEvent(%s) err = %v", providerEventID, err)
	}
}

func boolPtr(value bool) *bool {
	return &value
}

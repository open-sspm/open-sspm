package handlers

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/evaluator"
)

func TestPolicyPackSummariesAreSortedForSettings(t *testing.T) {
	t.Parallel()

	registry, err := evaluator.LoadBuiltin()
	if err != nil {
		t.Fatalf("LoadBuiltin() error = %v", err)
	}

	summaries := policyPackSummaries(registry)
	if len(summaries) != registry.PackCount() {
		t.Fatalf("summaries len = %d, want %d", len(summaries), registry.PackCount())
	}

	for i := 1; i < len(summaries); i++ {
		previous := summaries[i-1]
		current := summaries[i]
		if previous.Domain > current.Domain || previous.Domain == current.Domain && previous.ID > current.ID {
			t.Fatalf("summaries not sorted at %d: %+v before %+v", i, previous, current)
		}
	}

	foundCredential := false
	for _, summary := range summaries {
		if summary.Domain == "credential" && summary.ID == "builtin.credential.risk" && summary.Version != "" {
			foundCredential = true
		}
	}
	if !foundCredential {
		t.Fatalf("summaries missing built-in credential policy: %+v", summaries)
	}
}

func TestHandleResyncStreamEmitsInnerStatusAndDone(t *testing.T) {
	c, rec := newTestContext(http.MethodGet, "http://example.com/settings/resync/stream")
	h := &Handlers{}

	if err := h.HandleResyncStream(c); err != nil {
		t.Fatalf("HandleResyncStream() error = %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	body := rec.Body.String()
	assertContains(t, body, "event: status")
	assertContains(t, body, "event: done")
	assertContains(t, body, "Sync complete")
	if strings.Contains(body, "sse-connect") || strings.Contains(body, `id="settings-sync-status"`) {
		t.Fatalf("SSE payload included reconnect wrapper: %s", body)
	}
}

func TestHandleResyncStreamEmitsFailureForFailedManualJob(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		if _, err := pool.Exec(ctx, `
			INSERT INTO sync_jobs (id, lane, trigger_kind, status, attempt_count, available_at, finished_at, last_error)
			VALUES ('00000000-0000-0000-0000-000000000001', 'full', 'manual', 'failed', 1, now(), now(), 'boom')
		`); err != nil {
			t.Fatalf("insert failed sync job: %v", err)
		}

		c, rec := newTestContext(http.MethodGet, "http://example.com/settings/resync/stream")
		if err := h.HandleResyncStream(c); err != nil {
			t.Fatalf("HandleResyncStream() error = %v", err)
		}

		body := rec.Body.String()
		assertContains(t, body, "event: status")
		assertContains(t, body, "event: done")
		assertContains(t, body, "Sync failed")
		if strings.Contains(body, "Sync complete") {
			t.Fatalf("failed stream reported success: %s", body)
		}
	})
}

func TestHandleResyncStatusTriggersFailureForFailedManualJob(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		if _, err := pool.Exec(ctx, `
			INSERT INTO sync_jobs (id, lane, trigger_kind, status, attempt_count, available_at, finished_at, last_error)
			VALUES ('00000000-0000-0000-0000-000000000002', 'full', 'manual', 'failed', 1, now(), now(), 'boom')
		`); err != nil {
			t.Fatalf("insert failed sync job: %v", err)
		}

		c, rec := newTestContext(http.MethodGet, "http://example.com/settings/resync/status")
		c.Request().Header.Set("HX-Request", "true")
		if err := h.HandleResyncStatus(c); err != nil {
			t.Fatalf("HandleResyncStatus() error = %v", err)
		}

		body := rec.Body.String()
		assertContains(t, body, "Sync failed")
		trigger := rec.Header().Get("HX-Trigger")
		assertContains(t, trigger, "osspm:data-sync-changed")
		assertContains(t, trigger, `"status":"error"`)
	})
}

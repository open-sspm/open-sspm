package handlers

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/open-sspm/open-sspm/internal/sync"
)

type connectorHealthRunSeed struct {
	status     string
	errorKind  string
	message    string
	finishedAt time.Time
}

func TestHandleConnectorHealthErrorDetails_DBBackedLastFiveNonSuccess(t *testing.T) {
	harness := newProgrammaticAccessRouteHarness(t)

	now := time.Now().UTC().Truncate(time.Second)

	insertConnectorHealthRunSeed(t, harness, connectorHealthRunSeed{
		status:     "success",
		errorKind:  "",
		message:    "success-should-not-appear",
		finishedAt: now.Add(-10 * time.Second),
	})

	hugeMessage := "huge-message-start\n" + strings.Repeat("界", connectorHealthErrorFullRunes+200)
	nonSuccessMessages := []string{
		"error-run-1",
		hugeMessage,
		"error-run-3",
		"error-run-4",
		"error-run-5",
		"error-run-6-should-be-trimmed",
	}
	statuses := []string{"error", "canceled", "error", "error", "canceled", "error"}

	for idx := range nonSuccessMessages {
		insertConnectorHealthRunSeed(t, harness, connectorHealthRunSeed{
			status:     statuses[idx],
			errorKind:  "api",
			message:    nonSuccessMessages[idx],
			finishedAt: now.Add(-1 * time.Minute).Add(-time.Duration(idx) * time.Minute),
		})
	}

	target := "/settings/connector-health/errors?source_kind=github&source_name=" + url.QueryEscape(harness.sourceName) + "&connector_name=GitHub"
	c, rec := newTestContext(http.MethodGet, target)
	if err := harness.handlers.HandleConnectorHealthErrorDetails(c); err != nil {
		t.Fatalf("HandleConnectorHealthErrorDetails() error = %v", err)
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	for _, expected := range []string{
		"GitHub errors",
		"error-run-1",
		"error-run-3",
		"error-run-4",
		"error-run-5",
		"huge-message-start",
		"Canceled",
		"Full text truncated at 20,000 characters.",
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("response missing %q: %q", expected, body)
		}
	}

	for _, unexpected := range []string{
		"success-should-not-appear",
		"error-run-6-should-be-trimmed",
	} {
		if strings.Contains(body, unexpected) {
			t.Fatalf("response unexpectedly contains %q: %q", unexpected, body)
		}
	}

	if got := strings.Count(body, "Show details"); got != 5 {
		t.Fatalf("details rows = %d, want 5", got)
	}
}

func TestHandleConnectorHealthErrorDetails_RequiresSourceParams(t *testing.T) {
	c, rec := newTestContext(http.MethodGet, "/settings/connector-health/errors")
	h := &Handlers{}
	if err := h.HandleConnectorHealthErrorDetails(c); err != nil {
		t.Fatalf("HandleConnectorHealthErrorDetails() error = %v", err)
	}

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleConnectorHealth_DBBackedIncludesDetailsActions(t *testing.T) {
	harness := newProgrammaticAccessRouteHarness(t)
	harness.handlers.Syncer = &connectorHealthSyncRunnerStub{err: sync.ErrSyncQueued}

	c, rec := newTestContext(http.MethodGet, "/settings/connector-health")
	if err := harness.handlers.HandleConnectorHealth(c); err != nil {
		t.Fatalf("HandleConnectorHealth() error = %v", err)
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	for _, expected := range []string{
		"connector-health-error-details-host",
		"/settings/connector-health/errors?",
		"/settings/connector-health/sync",
		"Trigger sync",
		"source_kind=github",
		"source_name=" + url.QueryEscape(harness.sourceName),
		"ti ti-dots",
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("response missing %q: %q", expected, body)
		}
	}
}

func TestHandleConnectorHealthSync_DBBackedQueuesScopedRequest(t *testing.T) {
	harness := newProgrammaticAccessRouteHarness(t)
	runner := &connectorHealthSyncRunnerStub{err: sync.ErrSyncQueued}
	harness.handlers.Syncer = runner

	target := "/settings/connector-health/sync?connector_kind=github&source_name=" + url.QueryEscape(harness.sourceName)
	c, rec := newTestContext(http.MethodPost, target)
	if err := harness.handlers.HandleConnectorHealthSync(c); err != nil {
		t.Fatalf("HandleConnectorHealthSync() error = %v", err)
	}

	if rec.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusSeeOther)
	}
	if loc := rec.Header().Get("Location"); loc != "/settings/connector-health" {
		t.Fatalf("location = %q, want %q", loc, "/settings/connector-health")
	}
	if runner.calls != 1 {
		t.Fatalf("runner calls = %d, want 1", runner.calls)
	}
	if !sync.IsForcedSync(runner.lastCtx) {
		t.Fatalf("expected forced sync context")
	}
	kind, sourceName, ok := sync.ConnectorScopeFromContext(runner.lastCtx)
	if !ok {
		t.Fatalf("expected scoped sync context")
	}
	if kind != "github" || !strings.EqualFold(sourceName, harness.sourceName) {
		t.Fatalf("scope = (%q, %q), want (github, %q)", kind, sourceName, harness.sourceName)
	}
}

func TestHandleConnectorHealthSync_DBBackedSupportsHXRedirect(t *testing.T) {
	harness := newProgrammaticAccessRouteHarness(t)
	harness.handlers.Syncer = &connectorHealthSyncRunnerStub{err: sync.ErrSyncQueued}

	target := "/settings/connector-health/sync?connector_kind=github&source_name=" + url.QueryEscape(harness.sourceName)
	c, rec := newTestContext(http.MethodPost, target)
	c.Request().Header.Set("HX-Request", "true")
	if err := harness.handlers.HandleConnectorHealthSync(c); err != nil {
		t.Fatalf("HandleConnectorHealthSync() error = %v", err)
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("HX-Redirect"); got != "/settings/connector-health" {
		t.Fatalf("HX-Redirect = %q, want %q", got, "/settings/connector-health")
	}
}

func TestHandleConnectorHealthSync_RejectsInvalidConnector(t *testing.T) {
	h := &Handlers{
		Syncer: &connectorHealthSyncRunnerStub{err: sync.ErrSyncQueued},
	}
	c, rec := newTestContext(http.MethodPost, "/settings/connector-health/sync?connector_kind=unknown&source_name=acme")
	if err := h.HandleConnectorHealthSync(c); err != nil {
		t.Fatalf("HandleConnectorHealthSync() error = %v", err)
	}
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusSeeOther)
	}
}

func insertConnectorHealthRunSeed(t *testing.T, harness *programmaticAccessRouteHarness, seed connectorHealthRunSeed) int64 {
	t.Helper()

	startedAt := seed.finishedAt.Add(-30 * time.Second)
	if startedAt.After(seed.finishedAt) {
		startedAt = seed.finishedAt
	}

	var id int64
	err := harness.tx.QueryRow(harness.ctx, `
		INSERT INTO sync_runs (
			source_kind,
			source_name,
			status,
			started_at,
			finished_at,
			message,
			error_kind
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id
	`,
		"github",
		harness.sourceName,
		seed.status,
		startedAt,
		seed.finishedAt,
		seed.message,
		seed.errorKind,
	).Scan(&id)
	if err != nil {
		t.Fatalf("insert sync run seed (%s): %v", seed.status, err)
	}
	return id
}

type connectorHealthSyncRunnerStub struct {
	err     error
	lastCtx context.Context
	calls   int
}

func (r *connectorHealthSyncRunnerStub) RunOnce(ctx context.Context) error {
	r.calls++
	r.lastCtx = ctx
	return r.err
}

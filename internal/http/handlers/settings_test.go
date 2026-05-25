package handlers

import (
	"net/http"
	"strings"
	"testing"

	"github.com/open-sspm/open-sspm/internal/riskpolicy"
)

func TestRiskPolicyPackSummariesAreSortedForSettings(t *testing.T) {
	t.Parallel()

	registry, err := riskpolicy.LoadBuiltin()
	if err != nil {
		t.Fatalf("LoadBuiltin() error = %v", err)
	}

	summaries := riskPolicyPackSummaries(registry)
	if len(summaries) != registry.PackCount() {
		t.Fatalf("summaries len = %d, want %d", len(summaries), registry.PackCount())
	}
	if got := riskPolicyExpressionCount(registry); got != registry.CompiledExpressionCount() || got == 0 {
		t.Fatalf("riskPolicyExpressionCount() = %d, want %d", got, registry.CompiledExpressionCount())
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

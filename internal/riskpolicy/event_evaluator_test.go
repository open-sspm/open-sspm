package riskpolicy

import (
	"testing"
	"time"
)

func TestEvaluateEventProducesShadowSignal(t *testing.T) {
	t.Parallel()

	result, err := EvaluateEvent(EventInput{
		SourceKind:      "okta",
		SourceName:      "example.okta.com",
		ProviderEventID: "evt-1",
		EventType:       "app.oauth2.signon",
		Category:        "discovery.oauth_grant",
		Action:          "app.oauth2.signon",
		Outcome:         "success",
		OccurredAt:      time.Date(2026, time.May, 16, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("EvaluateEvent() err = %v", err)
	}
	if len(result.Signals) != 1 {
		t.Fatalf("signals = %+v, want one signal", result.Signals)
	}
	if result.Signals[0].ID != "event.oauth_grant" {
		t.Fatalf("signal id = %q, want event.oauth_grant", result.Signals[0].ID)
	}
}

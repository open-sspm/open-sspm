package handlers

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/db/gen"
)

func TestNonHumanPrincipalRiskSignalsUsesStoredPolicySignals(t *testing.T) {
	t.Parallel()

	signals := nonHumanPrincipalRiskSignals(gen.NonHumanPrincipalReadModelsV{
		OwnerPresence:         "unknown",
		HasCriticalCredential: true,
		RiskSignalsJson: []byte(`[
			{
				"id": "missing_accountable_owner",
				"domain": "identity",
				"severity": "high",
				"title": "No accountable owner",
				"evidence": "No accountable owner is assigned."
			}
		]`),
	})
	if len(signals) != 1 {
		t.Fatalf("len(signals) = %d, want 1; signals=%+v", len(signals), signals)
	}
	if signals[0].Severity != "high" || signals[0].Title != "No accountable owner" {
		t.Fatalf("signals[0] = %+v, want stored policy signal", signals[0])
	}
}

func TestNonHumanPrincipalRiskSignalsEmptyOrInvalidJSONIsSafe(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		principal gen.NonHumanPrincipalReadModelsV
	}{
		{
			name: "empty",
			principal: gen.NonHumanPrincipalReadModelsV{
				OwnerPresence: "unknown",
			},
		},
		{
			name: "invalid",
			principal: gen.NonHumanPrincipalReadModelsV{
				RiskSignalsJson: []byte(`{"not":`),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if signals := nonHumanPrincipalRiskSignals(tc.principal); len(signals) != 0 {
				t.Fatalf("len(signals) = %d, want 0; signals=%+v", len(signals), signals)
			}
		})
	}
}

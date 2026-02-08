package discovery

import (
	"testing"
	"time"
)

func TestManagedStateAndReason(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.February, 8, 18, 0, 0, 0, time.UTC)

	cases := []struct {
		name       string
		input      ManagedStateInput
		wantState  string
		wantReason string
	}{
		{
			name: "no binding",
			input: ManagedStateInput{
				HasPrimaryBinding: false,
				Now:               now,
			},
			wantState:  ManagedStateUnmanaged,
			wantReason: ManagedReasonNoBinding,
		},
		{
			name: "connector disabled",
			input: ManagedStateInput{
				HasPrimaryBinding:   true,
				ConnectorEnabled:    false,
				ConnectorConfigured: true,
				Now:                 now,
			},
			wantState:  ManagedStateUnmanaged,
			wantReason: ManagedReasonConnectorDisabled,
		},
		{
			name: "connector not configured",
			input: ManagedStateInput{
				HasPrimaryBinding:   true,
				ConnectorEnabled:    true,
				ConnectorConfigured: false,
				Now:                 now,
			},
			wantState:  ManagedStateUnmanaged,
			wantReason: ManagedReasonConnectorNotConfigured,
		},
		{
			name: "stale sync",
			input: ManagedStateInput{
				HasPrimaryBinding:     true,
				ConnectorEnabled:      true,
				ConnectorConfigured:   true,
				HasLastSuccessfulSync: true,
				LastSuccessfulSyncAt:  now.Add(-2 * time.Hour),
				FreshnessWindow:       45 * time.Minute,
				Now:                   now,
			},
			wantState:  ManagedStateUnmanaged,
			wantReason: ManagedReasonStaleSync,
		},
		{
			name: "managed fresh",
			input: ManagedStateInput{
				HasPrimaryBinding:     true,
				ConnectorEnabled:      true,
				ConnectorConfigured:   true,
				HasLastSuccessfulSync: true,
				LastSuccessfulSyncAt:  now.Add(-20 * time.Minute),
				FreshnessWindow:       45 * time.Minute,
				Now:                   now,
			},
			wantState:  ManagedStateManaged,
			wantReason: ManagedReasonActiveBindingFreshSync,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			state, reason := ManagedStateAndReason(tc.input)
			if state != tc.wantState || reason != tc.wantReason {
				t.Fatalf("ManagedStateAndReason() = (%q, %q), want (%q, %q)", state, reason, tc.wantState, tc.wantReason)
			}
		})
	}
}

func TestRiskAndSuggestions(t *testing.T) {
	t.Parallel()

	score, level := RiskScoreAndLevel(RiskInput{
		ManagedState:          ManagedStateUnmanaged,
		HasPrivilegedScopes:   true,
		HasConfidentialScopes: true,
		HasOwner:              false,
		Actors30d:             220,
		BusinessCriticality:   "critical",
		DataClassification:    "restricted",
	})
	if score != 100 || level != "critical" {
		t.Fatalf("RiskScoreAndLevel() = (%d, %q), want (100, critical)", score, level)
	}

	if got := SuggestedBusinessCriticality(220, true); got != "critical" {
		t.Fatalf("SuggestedBusinessCriticality() = %q", got)
	}
	if got := SuggestedBusinessCriticality(60, false); got != "high" {
		t.Fatalf("SuggestedBusinessCriticality() = %q", got)
	}
	if got := SuggestedBusinessCriticality(20, false); got != "medium" {
		t.Fatalf("SuggestedBusinessCriticality() = %q", got)
	}
	if got := SuggestedBusinessCriticality(2, false); got != "low" {
		t.Fatalf("SuggestedBusinessCriticality() = %q", got)
	}

	if got := SuggestedDataClassification(true, true); got != "restricted" {
		t.Fatalf("SuggestedDataClassification() privileged = %q", got)
	}
	if got := SuggestedDataClassification(false, true); got != "confidential" {
		t.Fatalf("SuggestedDataClassification() confidential = %q", got)
	}
	if got := SuggestedDataClassification(false, false); got != "internal" {
		t.Fatalf("SuggestedDataClassification() internal = %q", got)
	}
}

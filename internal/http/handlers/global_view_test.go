package handlers

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func TestBuildGlobalViewCardPreservesInactiveStatus(t *testing.T) {
	t.Parallel()

	h := &Handlers{}
	card := h.buildGlobalViewCard(registry.ConnectorState{
		Definition: testConnectorDefinition{kind: "github"},
	})

	if card.IsActive {
		t.Fatal("unconfigured connector should not be active")
	}
	if card.ShowScore {
		t.Fatal("unconfigured connector should not show a score")
	}
	if card.StatusLabel != "Not configured" {
		t.Fatalf("status label = %q, want %q", card.StatusLabel, "Not configured")
	}
	if card.StatusClass == "" {
		t.Fatal("inactive connector should preserve a status class")
	}
}

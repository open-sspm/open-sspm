package views

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/http/querystate"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func TestCredentialsTableQueryBarSuppressesPrimitiveChipsForActivePreset(t *testing.T) {
	data := viewmodels.CredentialsViewData{
		Query: querystate.CredentialsQuery{
			Source:        querystate.SourceSelection{Kind: "github"},
			RiskLevel:     "critical",
			Page:          1,
			ExpiresInDays: 0,
		},
		Sources: []viewmodels.ProgrammaticSourceOption{
			{SourceKind: "github", Label: "GitHub"},
		},
	}

	bar := CredentialsTableQueryBar(data)

	if len(bar.ActiveChips) != 2 {
		t.Fatalf("expected 2 active chips, got %d", len(bar.ActiveChips))
	}

	if bar.ActiveChips[0].FieldID != "source_kind" || bar.ActiveChips[1].FieldID != "preset" {
		t.Fatalf("unexpected active chips: %#v", bar.ActiveChips)
	}

	for _, chip := range bar.ActiveChips {
		if chip.FieldID == "risk_level" {
			t.Fatalf("did not expect a risk chip when preset is active")
		}
	}

	var riskField viewmodels.TableFilterField
	for _, field := range bar.Fields {
		if field.ID == "risk_level" {
			riskField = field
			break
		}
	}
	if !riskField.PickerVisible {
		t.Fatalf("expected suppressed primitive field to stay available in picker")
	}
}

func TestIdentitiesTableQueryBarShowsSortChipAndControls(t *testing.T) {
	data := viewmodels.IdentitiesViewData{
		Query: querystate.IdentitiesQuery{
			SortBy:  "last_seen",
			SortDir: "asc",
		},
	}

	bar := IdentitiesTableQueryBar(data)

	foundChip := false
	for _, chip := range bar.ActiveChips {
		if chip.FieldID == "sort" {
			foundChip = true
		}
	}
	if !foundChip {
		t.Fatalf("expected sort chip to be present")
	}

	hasSortBy := false
	hasSortDir := false
	for _, control := range bar.HiddenControls {
		if control.Name == "sort_by" && control.Value == "last_seen" {
			hasSortBy = true
		}
		if control.Name == "sort_dir" && control.Value == "asc" {
			hasSortDir = true
		}
	}
	if !hasSortBy || !hasSortDir {
		t.Fatalf("expected sort controls to be present in hidden control bank")
	}
}

func TestActiveInactiveStateTableQueryBarBuildsChipAndControl(t *testing.T) {
	bar := ActiveInactiveStateTableQueryBar(
		querystate.BasicListQuery{State: "inactive"},
		"Search users",
	)

	if len(bar.ActiveChips) != 1 {
		t.Fatalf("expected 1 active chip, got %d", len(bar.ActiveChips))
	}
	if bar.ActiveChips[0].FieldID != "state" {
		t.Fatalf("unexpected active chip: %#v", bar.ActiveChips[0])
	}

	if len(bar.HiddenControls) != 1 || bar.HiddenControls[0].Name != "state" || bar.HiddenControls[0].Value != "inactive" {
		t.Fatalf("unexpected hidden controls: %#v", bar.HiddenControls)
	}
}

func TestConnectedAppsTableQueryBarKeepsSliceControls(t *testing.T) {
	bar := ConnectedAppsTableQueryBar(viewmodels.ConnectedAppsViewData{
		Query: querystate.ConnectedAppsQuery{GovernanceState: "approved"},
	})

	required := map[string]string{
		"source_kind":      "google_workspace",
		"asset_kind":       "google_oauth_client",
		"governance_state": "approved",
	}

	for name, value := range required {
		found := false
		for _, control := range bar.HiddenControls {
			if control.Name == name && control.Value == value {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing control %s=%s", name, value)
		}
	}
}

func TestNonHumanIdentitiesTableQueryBarDropsSourceNameControl(t *testing.T) {
	bar := NonHumanIdentitiesTableQueryBar(viewmodels.NonHumanIdentitiesViewData{
		Query: querystate.NonHumanIdentitiesQuery{
			Source: querystate.SourceSelection{
				Kind: "entra",
				Name: "tenant-1",
			},
		},
		Sources: []viewmodels.ProgrammaticSourceOption{
			{SourceKind: "entra", Label: "Entra"},
		},
	})

	hasSourceKind := false
	for _, control := range bar.HiddenControls {
		if control.Name == "source_kind" && control.Value == "entra" {
			hasSourceKind = true
		}
		if control.Name == "source_name" {
			t.Fatalf("did not expect source_name control to be preserved")
		}
	}

	if !hasSourceKind {
		t.Fatalf("expected source_kind control to be preserved")
	}
}

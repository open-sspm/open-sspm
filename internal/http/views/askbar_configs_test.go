package views

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/http/querystate"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func TestAppAssetsAskBarOnlySuggestsConfiguredSources(t *testing.T) {
	t.Parallel()

	cfg := AppAssetsAskBar(viewmodels.AppAssetsViewData{
		Sources: []viewmodels.ProgrammaticSourceOption{
			{SourceKind: "google_workspace", Label: "Google Workspace"},
		},
	})

	if got := cfg.KeywordTokens["google"]; got.Value != "google_workspace" {
		t.Fatalf("google token = %#v, want google_workspace source", got)
	}
	if got := cfg.KeywordTokens["workspace"]; got.Value != "google_workspace" {
		t.Fatalf("workspace token = %#v, want google_workspace source", got)
	}
	if _, ok := cfg.KeywordTokens["github"]; ok {
		t.Fatal("github token should not be offered when GitHub is not configured")
	}
}

func TestCredentialsAskBarSuppressesRedundantActiveExpiryChip(t *testing.T) {
	t.Parallel()

	cfg := CredentialsAskBar(viewmodels.CredentialsViewData{
		Query: querystate.CredentialsQuery{
			ExpiryState:   "active",
			ExpiresInDays: 30,
		},
	})

	for _, chip := range cfg.Chips {
		if chip.Field == "expiry_state" {
			t.Fatalf("expiry_state chip should be suppressed when expires_in_days is set; got %+v", chip)
		}
	}
	foundHidden := false
	for _, hidden := range cfg.Hidden {
		if hidden.Name == "expiry_state" && hidden.Value == "active" {
			foundHidden = true
		}
	}
	if !foundHidden {
		t.Fatal("expiry_state hidden input should still round-trip on submit")
	}
}

func TestCredentialsAskBarShowsExpiredExpiryChip(t *testing.T) {
	t.Parallel()

	cfg := CredentialsAskBar(viewmodels.CredentialsViewData{
		Query: querystate.CredentialsQuery{ExpiryState: "expired"},
	})

	found := false
	for _, chip := range cfg.Chips {
		if chip.Field == "expiry_state" && chip.Value == "expired" {
			found = true
		}
	}
	if !found {
		t.Fatal("expired expiry_state chip should still render")
	}
}

func TestCredentialsAskBarOnlySuggestsConfiguredSources(t *testing.T) {
	t.Parallel()

	cfg := CredentialsAskBar(viewmodels.CredentialsViewData{
		Sources: []viewmodels.ProgrammaticSourceOption{
			{SourceKind: "hashicorp_vault", Label: "Vault"},
		},
	})

	if got := cfg.KeywordTokens["vault"]; got.Value != "hashicorp_vault" {
		t.Fatalf("vault token = %#v, want hashicorp_vault source", got)
	}
	if _, ok := cfg.KeywordTokens["github"]; ok {
		t.Fatal("github token should not be offered when GitHub is not configured")
	}
	if _, ok := cfg.KeywordTokens["entra"]; ok {
		t.Fatal("entra token should not be offered when Entra is not configured")
	}
}

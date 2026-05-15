package views

import (
	"testing"

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

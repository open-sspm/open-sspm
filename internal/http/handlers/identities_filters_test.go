package handlers

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func TestIdentitySourceNameOptionsDedupesNamesWhenKindNotSelected(t *testing.T) {
	sources := []viewmodels.ProgrammaticSourceOption{
		{SourceKind: "github", SourceName: "acme", Label: "GitHub"},
		{SourceKind: "okta", SourceName: "acme", Label: "Okta"},
		{SourceKind: "entra", SourceName: "tenant-1", Label: "Entra"},
	}

	opts := identitySourceNameOptions("", sources)
	if len(opts) != 2 {
		t.Fatalf("len(opts) = %d, want 2", len(opts))
	}
	if opts[0].SourceName != "acme" {
		t.Fatalf("opts[0].SourceName = %q, want %q", opts[0].SourceName, "acme")
	}
	if opts[1].SourceName != "tenant-1" {
		t.Fatalf("opts[1].SourceName = %q, want %q", opts[1].SourceName, "tenant-1")
	}
}

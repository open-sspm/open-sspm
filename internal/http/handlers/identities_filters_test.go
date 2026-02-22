package handlers

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func TestNormalizeIdentitySourceSelectionInfersKindFromName(t *testing.T) {
	sources := []viewmodels.ProgrammaticSourceOption{
		{SourceKind: "github", SourceName: "acme", Label: "GitHub"},
		{SourceKind: "entra", SourceName: "tenant-1", Label: "Microsoft Entra"},
	}

	kind, name := normalizeIdentitySourceSelection("", "tenant-1", sources)
	if kind != "entra" {
		t.Fatalf("kind = %q, want %q", kind, "entra")
	}
	if name != "tenant-1" {
		t.Fatalf("name = %q, want %q", name, "tenant-1")
	}
}

func TestNormalizeIdentitySourceSelectionDropsUnknownKind(t *testing.T) {
	sources := []viewmodels.ProgrammaticSourceOption{
		{SourceKind: "github", SourceName: "acme", Label: "GitHub"},
	}

	kind, name := normalizeIdentitySourceSelection("okta", "missing", sources)
	if kind != "" || name != "" {
		t.Fatalf("selection = (%q, %q), want empty", kind, name)
	}
}

func TestNormalizeIdentitySourceSelectionKeepsNameOnlyFilterWhenAmbiguous(t *testing.T) {
	sources := []viewmodels.ProgrammaticSourceOption{
		{SourceKind: "github", SourceName: "acme", Label: "GitHub"},
		{SourceKind: "okta", SourceName: "acme", Label: "Okta"},
	}

	kind, name := normalizeIdentitySourceSelection("", "acme", sources)
	if kind != "" {
		t.Fatalf("kind = %q, want empty", kind)
	}
	if name != "acme" {
		t.Fatalf("name = %q, want %q", name, "acme")
	}
}

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

func TestNormalizeIdentitySortDirDefaultsDescWhenSortBySet(t *testing.T) {
	if got := normalizeIdentitySortDir("", "identity"); got != "desc" {
		t.Fatalf("sort dir = %q, want %q", got, "desc")
	}
	if got := normalizeIdentitySortDir("asc", "identity"); got != "asc" {
		t.Fatalf("sort dir = %q, want %q", got, "asc")
	}
	if got := normalizeIdentitySortDir("asc", ""); got != "" {
		t.Fatalf("sort dir = %q, want empty", got)
	}
}

func TestIdentitiesFilterActive(t *testing.T) {
	if identitiesFilterActive("", "", "", false, "", "", "", "", "") {
		t.Fatalf("expected no active filters")
	}
	if !identitiesFilterActive("alice", "", "", false, "", "", "", "", "") {
		t.Fatalf("expected active filters when query is set")
	}
	if !identitiesFilterActive("", "", "", true, "", "", "", "", "") {
		t.Fatalf("expected active filters when privileged flag is set")
	}
}

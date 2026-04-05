package handlers

import (
	"reflect"
	"testing"

	"github.com/open-sspm/open-sspm/internal/http/querystate"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func TestProgrammaticQuerySources(t *testing.T) {
	got := programmaticQuerySources([]viewmodels.ProgrammaticSourceOption{
		{SourceKind: " github ", SourceName: " acme "},
		{SourceKind: "  ", SourceName: ""},
		{SourceKind: "", SourceName: " tenant-1 "},
	})

	want := []querystate.SourceSelection{
		{Kind: "github", Name: "acme"},
		{Kind: "", Name: "tenant-1"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("programmaticQuerySources() = %#v, want %#v", got, want)
	}
}

func TestDiscoveryQuerySources(t *testing.T) {
	got := discoveryQuerySources([]viewmodels.DiscoverySourceOption{
		{SourceKind: " okta ", SourceName: " acme.okta.com "},
		{SourceKind: "", SourceName: ""},
	})

	want := []querystate.SourceSelection{
		{Kind: "okta", Name: "acme.okta.com"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("discoveryQuerySources() = %#v, want %#v", got, want)
	}
}

func TestNormalizeActiveInactiveState(t *testing.T) {
	if got := normalizeActiveInactiveState(" Active "); got != "active" {
		t.Fatalf("normalizeActiveInactiveState(active) = %q", got)
	}
	if got := normalizeActiveInactiveState("unknown"); got != "" {
		t.Fatalf("normalizeActiveInactiveState(unknown) = %q", got)
	}
}

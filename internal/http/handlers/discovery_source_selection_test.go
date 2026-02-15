package handlers

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func TestDiscoverySourceSelectionRules(t *testing.T) {
	t.Parallel()

	options := []viewmodels.DiscoverySourceOption{
		{SourceKind: "okta", SourceName: "acme.okta.com", Label: "Okta"},
		{SourceKind: "entra", SourceName: "tenant-1", Label: "Microsoft Entra"},
		{SourceKind: "entra", SourceName: "tenant-2", Label: "Microsoft Entra"},
		{SourceKind: "okta", SourceName: "shared-source", Label: "Okta"},
		{SourceKind: "entra", SourceName: "shared-source", Label: "Microsoft Entra"},
	}

	tests := []struct {
		name     string
		rawKind  string
		rawName  string
		wantKind string
		wantName string
	}{
		{
			name:     "exact pair preserved",
			rawKind:  "okta",
			rawName:  "acme.okta.com",
			wantKind: "okta",
			wantName: "acme.okta.com",
		},
		{
			name:     "kind only preserved",
			rawKind:  "entra",
			wantKind: "entra",
			wantName: "",
		},
		{
			name:     "name only unique infers kind and pair",
			rawName:  "tenant-1",
			wantKind: "entra",
			wantName: "tenant-1",
		},
		{
			name:     "invalid pair falls back to all configured",
			rawKind:  "okta",
			rawName:  "tenant-1",
			wantKind: "",
			wantName: "",
		},
		{
			name:     "unknown source falls back to all configured",
			rawKind:  "github",
			wantKind: "",
			wantName: "",
		},
		{
			name:     "ambiguous name only falls back to all configured",
			rawName:  "shared-source",
			wantKind: "",
			wantName: "",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			kind, name := normalizeDiscoverySourceSelection(tc.rawKind, tc.rawName, options)
			if kind != tc.wantKind || name != tc.wantName {
				t.Fatalf("selection kind=%q name=%q, want kind=%q name=%q", kind, name, tc.wantKind, tc.wantName)
			}
		})
	}
}

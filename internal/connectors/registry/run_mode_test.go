package registry

import "testing"

func TestRunModeNormalizeDefaultsUnknownToFull(t *testing.T) {
	t.Parallel()

	if got := RunMode("unexpected").Normalize(); got != RunModeFull {
		t.Fatalf("Normalize() = %q, want %q", got, RunModeFull)
	}
	if got := RunModeDiscovery.Normalize(); got != RunModeDiscovery {
		t.Fatalf("Normalize(discovery) = %q, want %q", got, RunModeDiscovery)
	}
}

func TestSyncRunSourceKindMapsDiscoveryLanes(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		kind string
		mode RunMode
		want string
	}{
		"okta discovery":              {kind: " okta ", mode: RunModeDiscovery, want: "okta_discovery"},
		"google workspace discovery":  {kind: "google_workspace", mode: RunModeDiscovery, want: "google_workspace_discovery"},
		"full stays canonical":        {kind: "okta", mode: RunModeFull, want: "okta"},
		"unknown discovery stays put": {kind: "github", mode: RunModeDiscovery, want: "github"},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := SyncRunSourceKind(tt.kind, tt.mode); got != tt.want {
				t.Fatalf("SyncRunSourceKind(%q, %q) = %q, want %q", tt.kind, tt.mode, got, tt.want)
			}
		})
	}
}

func TestSyncRunScopeKindsPairsFullAndDiscoveryLanes(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		sourceKind string
		want       []string
	}{
		"full lane expands":      {sourceKind: "okta", want: []string{"okta", "okta_discovery"}},
		"discovery lane expands": {sourceKind: "okta_discovery", want: []string{"okta", "okta_discovery"}},
		"trim and lower":         {sourceKind: " Entra_Discovery ", want: []string{"entra", "entra_discovery"}},
		"unknown singleton":      {sourceKind: "github", want: []string{"github"}},
		"empty nil":              {sourceKind: "", want: nil},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := SyncRunScopeKinds(tt.sourceKind)
			if len(got) != len(tt.want) {
				t.Fatalf("SyncRunScopeKinds(%q) len = %d, want %d", tt.sourceKind, len(got), len(tt.want))
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Fatalf("SyncRunScopeKinds(%q)[%d] = %q, want %q", tt.sourceKind, i, got[i], tt.want[i])
				}
			}
		})
	}
}

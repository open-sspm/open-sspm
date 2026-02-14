package registry

import "testing"

func TestSyncRunSourceKind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		kind string
		mode RunMode
		want string
	}{
		{name: "full okta unchanged", kind: "okta", mode: RunModeFull, want: "okta"},
		{name: "full entra unchanged", kind: "entra", mode: RunModeFull, want: "entra"},
		{name: "discovery okta mapped", kind: "okta", mode: RunModeDiscovery, want: "okta_discovery"},
		{name: "discovery entra mapped", kind: "entra", mode: RunModeDiscovery, want: "entra_discovery"},
		{name: "discovery other unchanged", kind: "github", mode: RunModeDiscovery, want: "github"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := SyncRunSourceKind(tt.kind, tt.mode); got != tt.want {
				t.Fatalf("SyncRunSourceKind(%q, %q) = %q, want %q", tt.kind, tt.mode, got, tt.want)
			}
		})
	}
}

func TestParseRunMode(t *testing.T) {
	t.Parallel()

	if got := ParseRunMode("discovery"); got != RunModeDiscovery {
		t.Fatalf("ParseRunMode(discovery) = %q, want %q", got, RunModeDiscovery)
	}
	if got := ParseRunMode(""); got != RunModeFull {
		t.Fatalf("ParseRunMode(empty) = %q, want %q", got, RunModeFull)
	}
	if got := ParseRunMode("unexpected"); got != RunModeFull {
		t.Fatalf("ParseRunMode(unexpected) = %q, want %q", got, RunModeFull)
	}
}

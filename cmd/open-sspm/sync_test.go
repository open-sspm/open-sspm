package main

import (
	"reflect"
	"testing"

	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func TestSyncRunModesForLane(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		lane string
		cfg  config.Config
		want []registry.RunMode
	}{
		{
			name: "default all includes discovery when enabled",
			lane: "",
			cfg:  config.Config{SyncDiscoveryEnabled: true},
			want: []registry.RunMode{registry.RunModeFull, registry.RunModeDiscovery},
		},
		{
			name: "all skips discovery when disabled",
			lane: "all",
			cfg:  config.Config{SyncDiscoveryEnabled: false},
			want: []registry.RunMode{registry.RunModeFull},
		},
		{
			name: "full",
			lane: " FULL ",
			cfg:  config.Config{SyncDiscoveryEnabled: true},
			want: []registry.RunMode{registry.RunModeFull},
		},
		{
			name: "discovery",
			lane: "discovery",
			cfg:  config.Config{SyncDiscoveryEnabled: true},
			want: []registry.RunMode{registry.RunModeDiscovery},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := syncRunModesForLane(tt.lane, tt.cfg)
			if err != nil {
				t.Fatalf("syncRunModesForLane(%q) err = %v", tt.lane, err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("syncRunModesForLane(%q) = %#v, want %#v", tt.lane, got, tt.want)
			}
		})
	}
}

func TestSyncRunModesForLaneRejectsDisabledDiscovery(t *testing.T) {
	t.Parallel()

	if _, err := syncRunModesForLane("discovery", config.Config{}); err == nil {
		t.Fatal("syncRunModesForLane(discovery) err = nil, want error")
	}
}

func TestSyncRunModesForLaneRejectsUnknownLane(t *testing.T) {
	t.Parallel()

	if _, err := syncRunModesForLane("unknown", config.Config{}); err == nil {
		t.Fatal("syncRunModesForLane(unknown) err = nil, want error")
	}
}

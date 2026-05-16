package main

import (
	"reflect"
	"testing"

	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func TestAPIResyncModesHonorLaneFlags(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.Config
		want []registry.RunMode
	}{
		{
			name: "full and discovery enabled",
			cfg: config.Config{
				ResyncEnabled:        true,
				SyncFullEnabled:      true,
				SyncDiscoveryEnabled: true,
			},
			want: []registry.RunMode{registry.RunModeFull, registry.RunModeDiscovery},
		},
		{
			name: "full disabled",
			cfg: config.Config{
				ResyncEnabled:        true,
				SyncFullEnabled:      false,
				SyncDiscoveryEnabled: true,
			},
			want: []registry.RunMode{registry.RunModeDiscovery},
		},
		{
			name: "all lanes disabled",
			cfg: config.Config{
				ResyncEnabled:        true,
				SyncFullEnabled:      false,
				SyncDiscoveryEnabled: false,
			},
			want: nil,
		},
		{
			name: "resync disabled",
			cfg: config.Config{
				ResyncEnabled:        false,
				SyncFullEnabled:      true,
				SyncDiscoveryEnabled: true,
			},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := apiResyncModes(tt.cfg)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("apiResyncModes() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

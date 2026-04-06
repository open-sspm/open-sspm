package main

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/config"
)

func TestChooseStartupReadModelsAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		mode         string
		needsRebuild bool
		want         startupReadModelsAction
		wantErr      bool
	}{
		{
			name: "always forces rebuild",
			mode: config.StartupReadModelRebuildAlways,
			want: startupReadModelsActionRebuildAll,
		},
		{
			name:         "auto rebuilds when projections missing",
			mode:         config.StartupReadModelRebuildAuto,
			needsRebuild: true,
			want:         startupReadModelsActionRebuildAll,
		},
		{
			name:         "auto refreshes connector state when projections exist",
			mode:         config.StartupReadModelRebuildAuto,
			needsRebuild: false,
			want:         startupReadModelsActionRefreshConnectorState,
		},
		{
			name:    "invalid mode",
			mode:    "sometimes",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := chooseStartupReadModelsAction(tc.mode, tc.needsRebuild)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("chooseStartupReadModelsAction(%q, %v) error = nil, want error", tc.mode, tc.needsRebuild)
				}
				return
			}
			if err != nil {
				t.Fatalf("chooseStartupReadModelsAction(%q, %v) error = %v", tc.mode, tc.needsRebuild, err)
			}
			if got != tc.want {
				t.Fatalf("chooseStartupReadModelsAction(%q, %v) = %q, want %q", tc.mode, tc.needsRebuild, got, tc.want)
			}
		})
	}
}

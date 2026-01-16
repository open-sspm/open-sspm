package handlers

import (
	"testing"
	"time"
)

func TestStaleAfterInterval(t *testing.T) {
	cases := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{name: "zero", in: 0, want: 2 * time.Hour},
		{name: "15m", in: 15 * time.Minute, want: 2 * time.Hour},
		{name: "30m", in: 30 * time.Minute, want: 2 * time.Hour},
		{name: "1h", in: time.Hour, want: 4 * time.Hour},
		{name: "24h", in: 24 * time.Hour, want: 72 * time.Hour},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := staleAfterInterval(tc.in); got != tc.want {
				t.Fatalf("staleAfterInterval(%s)=%s want %s", tc.in, got, tc.want)
			}
		})
	}
}

func TestConnectorHealthFromRollup(t *testing.T) {
	now := time.Date(2026, 1, 16, 12, 0, 0, 0, time.UTC)
	threeHoursAgo := now.Add(-3 * time.Hour)
	oneHourAgo := now.Add(-time.Hour)

	cases := []struct {
		name string
		in   syncRunRollup
		want connectorHealthStatus
	}{
		{name: "no success", in: syncRunRollup{lastRunStatus: "error"}, want: connectorHealthNeverSynced},
		{
			name: "stale success",
			in: syncRunRollup{
				lastRunStatus:     "error",
				lastSuccessAt:     &threeHoursAgo,
				lastRunFinishedAt: &threeHoursAgo,
			},
			want: connectorHealthStale,
		},
		{
			name: "degraded but fresh",
			in: syncRunRollup{
				lastRunStatus:     "error",
				lastSuccessAt:     &oneHourAgo,
				lastRunFinishedAt: &oneHourAgo,
			},
			want: connectorHealthDegraded,
		},
		{
			name: "healthy",
			in: syncRunRollup{
				lastRunStatus:     "success",
				lastSuccessAt:     &oneHourAgo,
				lastRunFinishedAt: &oneHourAgo,
			},
			want: connectorHealthHealthy,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := connectorHealthFromRollup(now, 15*time.Minute, tc.in)
			if got != tc.want {
				t.Fatalf("connectorHealthFromRollup=%q want %q", got, tc.want)
			}
		})
	}
}

func TestConnectorHealthWrapper(t *testing.T) {
	now := time.Date(2026, 1, 16, 12, 0, 0, 0, time.UTC)
	oneHourAgo := now.Add(-time.Hour)
	threeDaysAgo := now.Add(-72 * time.Hour)

	t.Run("unsupported", func(t *testing.T) {
		got := connectorHealth(connectorHealthInput{
			syncable:   false,
			configured: true,
			enabled:    true,
			now:        now,
		})
		if got.status != connectorHealthUnsupported {
			t.Fatalf("status=%q want %q", got.status, connectorHealthUnsupported)
		}
		if got.statusLabel == "" || got.statusClass == "" {
			t.Fatalf("missing label/class")
		}
	})

	t.Run("not configured", func(t *testing.T) {
		got := connectorHealth(connectorHealthInput{
			syncable:   true,
			configured: false,
			enabled:    true,
			now:        now,
		})
		if got.status != connectorHealthNotConfigured {
			t.Fatalf("status=%q want %q", got.status, connectorHealthNotConfigured)
		}
	})

	t.Run("disabled does not count as enabled", func(t *testing.T) {
		got := connectorHealth(connectorHealthInput{
			syncable:   true,
			configured: true,
			enabled:    false,
			now:        now,
			rollup: syncRunRollup{
				lastSuccessAt:     &oneHourAgo,
				lastRunStatus:     "success",
				lastRunFinishedAt: &oneHourAgo,
			},
		})
		if got.status != connectorHealthDisabled {
			t.Fatalf("status=%q want %q", got.status, connectorHealthDisabled)
		}
		if got.countsAsEnabled {
			t.Fatalf("countsAsEnabled=true want false")
		}
	})

	t.Run("never synced needs attention", func(t *testing.T) {
		got := connectorHealth(connectorHealthInput{
			syncable:         true,
			configured:       true,
			enabled:          true,
			expectedInterval: 15 * time.Minute,
			now:              now,
			rollup:           syncRunRollup{},
		})
		if got.status != connectorHealthNeverSynced {
			t.Fatalf("status=%q want %q", got.status, connectorHealthNeverSynced)
		}
		if !got.needsAttention {
			t.Fatalf("needsAttention=false want true")
		}
		if got.lastSuccessLabel != "—" {
			t.Fatalf("lastSuccessLabel=%q want %q", got.lastSuccessLabel, "—")
		}
	})

	t.Run("stale needs attention", func(t *testing.T) {
		got := connectorHealth(connectorHealthInput{
			syncable:         true,
			configured:       true,
			enabled:          true,
			expectedInterval: 15 * time.Minute,
			now:              now,
			rollup: syncRunRollup{
				lastSuccessAt: &threeDaysAgo,
				lastRunStatus: "error",
			},
		})
		if got.status != connectorHealthStale {
			t.Fatalf("status=%q want %q", got.status, connectorHealthStale)
		}
		if !got.needsAttention {
			t.Fatalf("needsAttention=false want true")
		}
	})
}

func TestFormatSuccessRate(t *testing.T) {
	if got := formatSuccessRate(0, 0); got != "—" {
		t.Fatalf("got %q want %q", got, "—")
	}
	if got := formatSuccessRate(5, 10); got != "50%" {
		t.Fatalf("got %q want %q", got, "50%")
	}
	if got := formatSuccessRate(12, 10); got != "100%" {
		t.Fatalf("got %q want %q", got, "100%")
	}
}

func TestFormatDuration(t *testing.T) {
	cases := []struct {
		in   time.Duration
		want string
	}{
		{in: 42 * time.Second, want: "42s"},
		{in: 2 * time.Minute, want: "2m"},
		{in: 2*time.Minute + 5*time.Second, want: "2m 5s"},
		{in: time.Hour, want: "1h"},
		{in: time.Hour + 2*time.Minute, want: "1h 2m"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.want, func(t *testing.T) {
			if got := formatDuration(tc.in); got != tc.want {
				t.Fatalf("formatDuration(%s)=%q want %q", tc.in, got, tc.want)
			}
		})
	}
}

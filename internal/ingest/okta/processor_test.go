package oktaingest

import (
	"testing"
	"time"
)

func TestBackoffDelayDoublesUpToCap(t *testing.T) {
	base := 30 * time.Second
	max := 15 * time.Minute

	cases := []struct {
		attempts int32
		want     time.Duration
	}{
		{attempts: 0, want: 30 * time.Second},
		{attempts: 1, want: 30 * time.Second},
		{attempts: 2, want: 60 * time.Second},
		{attempts: 3, want: 2 * time.Minute},
		{attempts: 4, want: 4 * time.Minute},
		{attempts: 5, want: 8 * time.Minute},
		{attempts: 6, want: 15 * time.Minute},
		{attempts: 10, want: 15 * time.Minute},
		{attempts: 100, want: 15 * time.Minute},
	}
	for _, tc := range cases {
		got := backoffDelay(tc.attempts, base, max)
		if got != tc.want {
			t.Errorf("backoffDelay(%d) = %v, want %v", tc.attempts, got, tc.want)
		}
	}
}

func TestBackoffDelayHandlesDegenerateConfig(t *testing.T) {
	// Max smaller than base: max bumped up to base, never below.
	if got := backoffDelay(3, 30*time.Second, 5*time.Second); got != 30*time.Second {
		t.Errorf("backoffDelay clamps max < base, got %v want 30s", got)
	}
	// Zero base falls back to package default.
	if got := backoffDelay(1, 0, 0); got != defaultRetryDelay {
		t.Errorf("backoffDelay(1, 0, 0) = %v, want default %v", got, defaultRetryDelay)
	}
}

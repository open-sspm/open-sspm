package sync

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

type stubRunner struct {
	err error
}

func (r stubRunner) RunOnce(context.Context) error {
	return r.err
}

func TestCompositeRunner_AggregatesStatuses(t *testing.T) {
	t.Parallel()

	hard := errors.New("hard-failure")

	tests := []struct {
		name    string
		runners []Runner
		want    error
	}{
		{
			name:    "hard error wins",
			runners: []Runner{stubRunner{err: nil}, stubRunner{err: hard}, stubRunner{err: ErrSyncQueued}},
			want:    hard,
		},
		{
			name:    "success wins over queued",
			runners: []Runner{stubRunner{err: nil}, stubRunner{err: ErrSyncQueued}},
			want:    nil,
		},
		{
			name:    "queued when no success",
			runners: []Runner{stubRunner{err: ErrSyncQueued}, stubRunner{err: ErrSyncAlreadyRunning}},
			want:    ErrSyncQueued,
		},
		{
			name:    "all busy returns busy",
			runners: []Runner{stubRunner{err: ErrSyncAlreadyRunning}, stubRunner{err: ErrSyncAlreadyRunning}},
			want:    ErrSyncAlreadyRunning,
		},
		{
			name:    "all disabled returns disabled",
			runners: []Runner{stubRunner{err: ErrNoEnabledConnectors}, stubRunner{err: ErrNoConnectorsDue}},
			want:    ErrNoEnabledConnectors,
		},
		{
			name:    "joined disabled and hard error is hard",
			runners: []Runner{stubRunner{err: nil}, stubRunner{err: errors.Join(ErrNoEnabledConnectors, hard)}},
			want:    hard,
		},
		{
			name:    "wrapped disabled-only error remains disabled",
			runners: []Runner{stubRunner{err: fmt.Errorf("wrapped: %w", ErrNoEnabledConnectors)}, stubRunner{err: ErrNoConnectorsDue}},
			want:    ErrNoEnabledConnectors,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := NewCompositeRunner(tt.runners...).RunOnce(context.Background())
			if tt.want == nil {
				if err != nil {
					t.Fatalf("RunOnce() err = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("RunOnce() err = %v, want %v", err, tt.want)
			}
		})
	}
}

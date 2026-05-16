package timing

import (
	"context"
	"time"
)

// SleepContext waits for d or until ctx is canceled. It returns false when the
// context wins, and true when the sleep completes.
func SleepContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx == nil || ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// ExponentialBackoff returns base * 2^(attempts-1), capped at max. Attempts
// less than one are treated as the first attempt.
func ExponentialBackoff(attempts int, base, max time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	if max <= 0 {
		max = base
	}
	if max < base {
		max = base
	}
	if attempts < 1 {
		attempts = 1
	}

	delay := base
	for idx := 1; idx < attempts; idx++ {
		if delay >= max {
			return max
		}
		if delay > max/2 {
			return max
		}
		delay *= 2
	}
	if delay > max {
		return max
	}
	return delay
}

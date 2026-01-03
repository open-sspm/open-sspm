package sync

import (
	"time"
)

type RunPolicy struct {
	IntervalByKind       map[string]time.Duration
	FailureBackoffBase   time.Duration
	FailureBackoffMax    time.Duration
	RecentFinishedRunCap int
	Now                  func() time.Time
}

func (p RunPolicy) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

func (p RunPolicy) intervalForKind(kind string) time.Duration {
	if p.IntervalByKind == nil {
		return 0
	}
	return p.IntervalByKind[kind]
}

func (p RunPolicy) recentCap() int32 {
	if p.RecentFinishedRunCap <= 0 {
		return 10
	}
	return int32(p.RecentFinishedRunCap)
}

func failureBackoffDelay(base time.Duration, failures int, max time.Duration) time.Duration {
	if failures <= 0 {
		return 0
	}
	if base <= 0 {
		return 0
	}

	delay := base
	for i := 1; i < failures; i++ {
		if delay > max/2 && max > 0 {
			delay = max
			break
		}
		delay *= 2
	}

	if max > 0 && delay > max {
		return max
	}
	return delay
}

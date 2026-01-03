package sync

import (
	"context"
	"log/slog"
	"time"
)

type Scheduler struct {
	Runner   Runner
	Interval time.Duration
}

func (s *Scheduler) Run(ctx context.Context) {
	if s.Runner == nil || s.Interval <= 0 {
		return
	}

	// Run immediately at startup.
	if err := s.Runner.RunOnce(ctx); err != nil {
		slog.Error("initial sync failed", "err", err)
	}

	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.Runner.RunOnce(ctx); err != nil {
				slog.Error("scheduled sync failed", "err", err)
			}
		}
	}
}

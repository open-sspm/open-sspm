package sync

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"time"
)

type Scheduler struct {
	Runner   Runner
	Interval time.Duration
	Trigger  <-chan TriggerRequest
}

func (s *Scheduler) Run(ctx context.Context) {
	if s.Runner == nil || s.Interval <= 0 {
		return
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	runOnce := func(label string, runCtx context.Context) error {
		started := time.Now()
		err := s.Runner.RunOnce(runCtx)
		if err != nil {
			if errors.Is(err, ErrNoConnectorsDue) {
				slog.Info(label+" sync skipped", "reason", err, "duration", time.Since(started))
				return nil
			}
			slog.Error(label+" sync failed", "err", err, "duration", time.Since(started))
			return err
		}
		slog.Info(label+" sync succeeded", "duration", time.Since(started))
		return nil
	}

	// Run immediately at startup.
	_ = runOnce("initial", ctx)

	timer := time.NewTimer(s.nextDelay(rng))
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			_ = runOnce("scheduled", ctx)
			resetTimer(timer, s.nextDelay(rng))
		case req, ok := <-s.Trigger:
			if !ok {
				s.Trigger = nil
				continue
			}
			runCtx := WithForcedSync(ctx)
			if req.HasConnectorScope() {
				runCtx = WithConnectorScope(runCtx, req.ConnectorKind, req.SourceName)
			}
			_ = runOnce("triggered", runCtx)
			resetTimer(timer, s.nextDelay(rng))
		}
	}
}

func resetTimer(timer *time.Timer, d time.Duration) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(d)
}

func (s *Scheduler) nextDelay(rng *rand.Rand) time.Duration {
	base := s.Interval
	if base <= 0 {
		return 0
	}
	delay := base
	jitter := base / 10
	if jitter > 5*time.Minute {
		jitter = 5 * time.Minute
	}
	if jitter <= 0 || rng == nil {
		return delay
	}
	// Spread evenly in [0, +jitter].
	delta := time.Duration(rng.Int63n(int64(jitter) + 1))
	return delay + delta
}

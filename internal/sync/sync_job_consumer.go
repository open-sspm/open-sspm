package sync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/timing"
)

const defaultSyncJobPollInterval = 2 * time.Second

var errSyncJobLeaseLost = errors.New("sync job lease lost")

type SyncJobConsumerConfig struct {
	Mode              registry.RunMode
	PollInterval      time.Duration
	LeaseTTL          time.Duration
	HeartbeatInterval time.Duration
	RetryBaseDelay    time.Duration
	RetryMaxDelay     time.Duration
	ClaimedBy         string
	Wakeups           <-chan struct{}
	OnLoopTick        func(lane string)
	OnClaimAttempt    func(lane string)
}

type SyncJobConsumer struct {
	store             syncJobStore
	locks             LockManager
	runner            Runner
	lane              string
	consumerScopeName string
	pollInterval      time.Duration
	leaseSeconds      int64
	heartbeatEvery    time.Duration
	retryBase         time.Duration
	retryMax          time.Duration
	claimedBy         string
	wakeups           <-chan struct{}
	onLoopTick        func(lane string)
	onClaimAttempt    func(lane string)
}

func NewSyncJobConsumer(store syncJobStore, locks LockManager, runner Runner, cfg SyncJobConsumerConfig) *SyncJobConsumer {
	mode := cfg.Mode.Normalize()
	pollInterval := cfg.PollInterval
	if pollInterval <= 0 {
		pollInterval = defaultSyncJobPollInterval
	}
	leaseTTL := cfg.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = defaultLockTTL
	}
	heartbeatEvery := cfg.HeartbeatInterval
	if heartbeatEvery <= 0 {
		heartbeatEvery = defaultLockHeartbeatInterval
	}
	retryBase := cfg.RetryBaseDelay
	if retryBase <= 0 {
		retryBase = pollInterval
	}
	retryMax := cfg.RetryMaxDelay
	if retryMax <= 0 {
		retryMax = retryBase * 10
	}
	if retryMax < retryBase {
		retryMax = retryBase
	}

	claimedBy := strings.TrimSpace(cfg.ClaimedBy)
	if claimedBy == "" {
		host := strings.TrimSpace(os.Getenv("HOSTNAME"))
		if host == "" {
			if resolved, err := os.Hostname(); err == nil {
				host = strings.TrimSpace(resolved)
			}
		}
		if host == "" {
			host = "unknown"
		}
		claimedBy = fmt.Sprintf("%s/%s/%s", host, SyncJobLaneForMode(mode), uuid.NewString())
	}

	return &SyncJobConsumer{
		store:             store,
		locks:             locks,
		runner:            runner,
		lane:              SyncJobLaneForMode(mode),
		consumerScopeName: SyncJobConsumerScopeNameForMode(mode),
		pollInterval:      pollInterval,
		leaseSeconds:      durationSecondsCeil(leaseTTL),
		heartbeatEvery:    heartbeatEvery,
		retryBase:         retryBase,
		retryMax:          retryMax,
		claimedBy:         claimedBy,
		wakeups:           cfg.Wakeups,
		onLoopTick:        cfg.OnLoopTick,
		onClaimAttempt:    cfg.OnClaimAttempt,
	}
}

func (c *SyncJobConsumer) Run(ctx context.Context) error {
	if c == nil || c.store == nil || c.locks == nil || c.runner == nil {
		return errors.New("sync job consumer is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		if ctx.Err() != nil {
			return nil
		}

		lock, err := c.locks.Acquire(ctx, syncJobConsumerScopeKind, c.consumerScopeName)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
				return nil
			}
			slog.Error("sync job consumer failed to acquire lane lock", "lane", c.lane, "err", err)
			if !timing.SleepContext(ctx, c.pollInterval) {
				return nil
			}
			continue
		}

		runErr, lost := runWithManagedLock(ctx, lock, c.consumeLoop)
		if lost != nil {
			slog.Error("sync job consumer lost lane lock", "lane", c.lane, "err", lost)
		}
		if runErr != nil && !errors.Is(runErr, context.Canceled) && !errors.Is(runErr, context.DeadlineExceeded) {
			slog.Error("sync job consumer loop failed", "lane", c.lane, "err", runErr)
		}
		if ctx.Err() != nil {
			return nil
		}
		if !timing.SleepContext(ctx, c.pollInterval) {
			return nil
		}
	}
}

func (c *SyncJobConsumer) consumeLoop(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		if _, err := c.store.RequeueStaleSyncJobsByLane(ctx, c.lane); err != nil {
			return err
		}

		c.observeClaimAttempt()
		job, ok, err := c.store.ClaimNextSyncJobByLane(ctx, c.lane, c.claimedBy, c.leaseSeconds)
		if err != nil {
			return err
		}
		c.observeLoopTick()
		if !ok {
			if !c.waitForWork(ctx) {
				return nil
			}
			continue
		}

		if err := c.processJob(ctx, job); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			slog.Error("sync job processing failed", "lane", c.lane, "job_id", job.ID.String(), "err", err)
		}
	}
}

func (c *SyncJobConsumer) processJob(ctx context.Context, job syncJobRecord) error {
	if !job.ID.Valid {
		return errors.New("sync job id is invalid")
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		lostMu  sync.Mutex
		lostErr error
	)
	stopHeartbeat := c.startHeartbeat(runCtx, job, func(err error) {
		lostMu.Lock()
		if lostErr == nil {
			lostErr = err
		}
		lostMu.Unlock()
		cancel()
	})
	defer stopHeartbeat()

	runningJob, markedRunning, err := c.store.MarkSyncJobRunning(runCtx, job.ID, c.claimedBy)
	if err != nil {
		return err
	}
	if !markedRunning {
		return errSyncJobLeaseLost
	}

	execCtx := runCtx
	if runningJob.TriggerKind == syncJobTriggerKindManual {
		execCtx = WithForcedSync(execCtx)
	}
	if runningJob.ConnectorKind != "" && runningJob.SourceName != "" {
		execCtx = WithConnectorScope(execCtx, runningJob.ConnectorKind, runningJob.SourceName)
	}

	runErr := c.runner.RunOnce(execCtx)

	lostMu.Lock()
	lost := lostErr
	lostMu.Unlock()
	if lost != nil {
		return lost
	}
	if runCtx.Err() != nil && (errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded)) {
		// Leave the job in its claimed/running state so stale-lease recovery can
		// requeue it after consumer shutdown or consumer-lock loss.
		return runErr
	}

	switch runningJob.TriggerKind {
	case syncJobTriggerKindManual:
		if runErr == nil || isOnlyNoWorkError(runErr) {
			marked, err := c.store.CompleteManualSyncJobSuccess(context.WithoutCancel(runCtx), job.ID, c.claimedBy)
			if err != nil {
				return err
			}
			if !marked {
				return errSyncJobLeaseLost
			}
			if isOnlyNoWorkError(runErr) {
				return nil
			}
			return nil
		}

		marked, markErr := c.store.CompleteManualSyncJobFailure(context.WithoutCancel(runCtx), job.ID, c.claimedBy, runErr.Error())
		if markErr != nil {
			return errors.Join(runErr, markErr)
		}
		if !marked {
			return errors.Join(runErr, errSyncJobLeaseLost)
		}
		return runErr
	case syncJobTriggerKindScheduled:
		if runErr == nil || isOnlyNoWorkError(runErr) {
			marked, err := c.store.CompleteScheduledSyncJobSuccess(context.WithoutCancel(runCtx), job.ID, c.claimedBy)
			if err != nil {
				return errors.Join(runErr, err)
			}
			if !marked {
				return errors.Join(runErr, errSyncJobLeaseLost)
			}
			if isOnlyNoWorkError(runErr) {
				return nil
			}
			return nil
		}

		nextAvailableAt := time.Now().Add(c.nextRetryDelay(runningJob.AttemptCount))
		marked, markErr := c.store.CompleteScheduledSyncJobFailure(context.WithoutCancel(runCtx), job.ID, c.claimedBy, runErr.Error(), nextAvailableAt)
		if markErr != nil {
			return errors.Join(runErr, markErr)
		}
		if !marked {
			return errors.Join(runErr, errSyncJobLeaseLost)
		}
		return runErr
	default:
		return errors.New("sync job trigger kind is invalid")
	}
}

func (c *SyncJobConsumer) startHeartbeat(ctx context.Context, job syncJobRecord, onLost func(error)) func() {
	if onLost == nil {
		onLost = func(error) {}
	}
	if c == nil || c.store == nil || c.heartbeatEvery <= 0 {
		return func() {}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	hbCtx, cancel := context.WithCancel(ctx)
	var once sync.Once
	stop := func() {
		once.Do(cancel)
	}

	go func() {
		ticker := time.NewTicker(c.heartbeatEvery)
		defer ticker.Stop()

		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
			}

			renewCtx, renewCancel := context.WithTimeout(hbCtx, c.heartbeatEvery)
			ok, err := c.store.RenewSyncJobLease(renewCtx, job.ID, c.claimedBy, c.leaseSeconds)
			renewCancel()
			if hbCtx.Err() != nil {
				return
			}
			if err != nil {
				onLost(err)
				stop()
				return
			}
			if !ok {
				onLost(errSyncJobLeaseLost)
				stop()
				return
			}
		}
	}()

	return stop
}

func (c *SyncJobConsumer) waitForWork(ctx context.Context) bool {
	if c == nil {
		return ctx == nil || ctx.Err() == nil
	}
	if c.wakeups == nil {
		return timing.SleepContext(ctx, c.pollInterval)
	}
	if c.pollInterval <= 0 {
		select {
		case <-ctx.Done():
			return false
		case <-c.wakeups:
			return true
		}
	}

	timer := time.NewTimer(c.pollInterval)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-c.wakeups:
		return true
	case <-timer.C:
		return true
	}
}

func (c *SyncJobConsumer) nextRetryDelay(attemptCount int32) time.Duration {
	base := c.retryBase
	if base <= 0 {
		base = defaultSyncJobPollInterval
	}
	maxDelay := c.retryMax
	if maxDelay <= 0 {
		maxDelay = base * 10
	}
	if maxDelay < base {
		maxDelay = base
	}

	return timing.ExponentialBackoff(int(attemptCount), base, maxDelay)
}

func (c *SyncJobConsumer) observeLoopTick() {
	if c != nil && c.onLoopTick != nil {
		c.onLoopTick(c.lane)
	}
}

func (c *SyncJobConsumer) observeClaimAttempt() {
	if c != nil && c.onClaimAttempt != nil {
		c.onClaimAttempt(c.lane)
	}
}

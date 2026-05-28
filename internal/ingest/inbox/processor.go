package inbox

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

type Handler interface {
	ProcessInboxDelivery(context.Context, Delivery) (ProcessResult, error)
}

type ProcessStatus string

const (
	ProcessStatusProcessed ProcessStatus = "processed"
	ProcessStatusIgnored   ProcessStatus = "ignored"
	ProcessStatusDead      ProcessStatus = "dead"
)

type ProcessResult struct {
	Status         ProcessStatus
	DecodedSummary map[string]any
	IgnoreReason   string
	LastError      string
}

type ProcessorConfig struct {
	BatchSize     int32
	LeaseOwner    string
	LeaseTTL      time.Duration
	RetryDelay    time.Duration
	MaxRetryDelay time.Duration
	MaxAttempts   int32
}

type Processor struct {
	store   *Store
	handler Handler
	cfg     ProcessorConfig
}

type RunOnceResult struct {
	Claimed   int
	Processed int
	Ignored   int
	Dead      int
	Retried   int
}

func NewProcessor(store *Store, handler Handler, cfg ProcessorConfig) *Processor {
	return &Processor{store: store, handler: handler, cfg: cfg.normalized()}
}

func (p *Processor) RunOnce(ctx context.Context) (RunOnceResult, error) {
	if p == nil || p.store == nil || p.handler == nil {
		return RunOnceResult{}, errors.New("event inbox processor is not configured")
	}
	deliveries, err := p.store.ClaimQueued(ctx, p.cfg.BatchSize, p.cfg.LeaseOwner, p.cfg.LeaseTTL)
	if err != nil {
		return RunOnceResult{}, err
	}
	out := RunOnceResult{Claimed: len(deliveries)}
	claim := newProcessingClaim(p.store, deliveries, p.cfg.LeaseOwner, p.cfg.LeaseTTL)
	processCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stopHeartbeat := startProcessingLeaseHeartbeat(processCtx, claim, heartbeatInterval(p.cfg.LeaseTTL), func(err error) {
		cancel(err)
	})
	defer stopHeartbeat()

	var errs []error
processLoop:
	for _, delivery := range deliveries {
		if err := processingContextError(processCtx); err != nil {
			errs = append(errs, err)
			break processLoop
		}
		result, err := p.handler.ProcessInboxDelivery(processCtx, delivery)
		if lost := processingContextError(processCtx); lost != nil {
			errs = append(errs, lost)
			break processLoop
		}
		if err != nil {
			deadLettered, markErr := p.retryOrDead(processCtx, claim, delivery, err)
			if markErr != nil {
				errs = append(errs, errors.Join(err, markErr))
				if errors.Is(markErr, ErrLeaseLost) {
					cancel(markErr)
					break processLoop
				}
			} else if deadLettered {
				out.Dead++
			} else {
				out.Retried++
			}
			continue
		}
		switch result.normalizedStatus() {
		case ProcessStatusIgnored:
			if err := claim.MarkIgnored(processCtx, delivery.ID, result.IgnoreReason, result.DecodedSummary); err != nil {
				errs = append(errs, err)
				if errors.Is(err, ErrLeaseLost) {
					cancel(err)
					break processLoop
				}
				continue
			}
			out.Ignored++
		case ProcessStatusDead:
			msg := strings.TrimSpace(result.LastError)
			if msg == "" {
				msg = "delivery rejected by processor"
			}
			if err := claim.MarkDead(processCtx, delivery.ID, msg); err != nil {
				errs = append(errs, err)
				if errors.Is(err, ErrLeaseLost) {
					cancel(err)
					break processLoop
				}
				continue
			}
			out.Dead++
		default:
			if err := claim.MarkProcessed(processCtx, delivery.ID, result.DecodedSummary); err != nil {
				errs = append(errs, err)
				if errors.Is(err, ErrLeaseLost) {
					cancel(err)
					break processLoop
				}
				continue
			}
			out.Processed++
		}
	}
	return out, errors.Join(errs...)
}

func (p *Processor) retryOrDead(ctx context.Context, claim *processingClaim, delivery Delivery, cause error) (bool, error) {
	msg := "processing failed"
	if cause != nil && strings.TrimSpace(cause.Error()) != "" {
		msg = cause.Error()
	}
	maxAttempts := p.maxAttempts(delivery)
	if maxAttempts > 0 && delivery.AttemptCount >= maxAttempts {
		return true, claim.MarkDead(ctx, delivery.ID, msg)
	}
	return false, claim.MarkRetry(ctx, delivery.ID, time.Now().Add(p.retryDelay(delivery.AttemptCount)), msg)
}

func (p *Processor) maxAttempts(delivery Delivery) int32 {
	if p.cfg.MaxAttempts > 0 {
		return p.cfg.MaxAttempts
	}
	return delivery.MaxAttempts
}

func (p *Processor) retryDelay(attempt int32) time.Duration {
	delay := p.cfg.RetryDelay
	for i := int32(1); i < attempt; i++ {
		delay *= 2
		if delay >= p.cfg.MaxRetryDelay {
			return p.cfg.MaxRetryDelay
		}
	}
	return delay
}

func (r ProcessResult) normalizedStatus() ProcessStatus {
	switch r.Status {
	case ProcessStatusIgnored, ProcessStatusDead:
		return r.Status
	default:
		return ProcessStatusProcessed
	}
}

func (c ProcessorConfig) normalized() ProcessorConfig {
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	c.LeaseOwner = strings.TrimSpace(c.LeaseOwner)
	if c.LeaseOwner == "" {
		c.LeaseOwner = "event-inbox-processor"
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = 5 * time.Minute
	}
	if c.RetryDelay <= 0 {
		c.RetryDelay = 30 * time.Second
	}
	if c.MaxRetryDelay <= 0 {
		c.MaxRetryDelay = 15 * time.Minute
	}
	if c.MaxRetryDelay < c.RetryDelay {
		c.MaxRetryDelay = c.RetryDelay
	}
	return c
}

func (s ProcessStatus) String() string {
	return string(s)
}

func (r RunOnceResult) String() string {
	return fmt.Sprintf("claimed=%d processed=%d ignored=%d dead=%d retried=%d", r.Claimed, r.Processed, r.Ignored, r.Dead, r.Retried)
}

type processingClaim struct {
	store      *Store
	leaseOwner string
	leaseTTL   time.Duration
	mu         sync.Mutex
	active     map[int64]struct{}
}

func newProcessingClaim(store *Store, deliveries []Delivery, leaseOwner string, leaseTTL time.Duration) *processingClaim {
	active := make(map[int64]struct{}, len(deliveries))
	for _, delivery := range deliveries {
		if delivery.ID > 0 {
			active[delivery.ID] = struct{}{}
		}
	}
	return &processingClaim{
		store:      store,
		leaseOwner: strings.TrimSpace(leaseOwner),
		leaseTTL:   leaseTTL,
		active:     active,
	}
}

func (c *processingClaim) RenewActive(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := c.activeIDsLocked()
	if len(ids) == 0 {
		return nil
	}
	return c.store.RenewLease(ctx, c.leaseOwner, ids, c.leaseTTL)
}

func (c *processingClaim) MarkProcessed(ctx context.Context, id int64, summary map[string]any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.store.MarkProcessed(ctx, c.leaseOwner, []int64{id}, summary)
	c.releaseLocked(id)
	return err
}

func (c *processingClaim) MarkIgnored(ctx context.Context, id int64, reason string, summary map[string]any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.store.MarkIgnored(ctx, c.leaseOwner, []int64{id}, reason, summary)
	c.releaseLocked(id)
	return err
}

func (c *processingClaim) MarkDead(ctx context.Context, id int64, lastError string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.store.MarkDead(ctx, c.leaseOwner, []int64{id}, lastError)
	c.releaseLocked(id)
	return err
}

func (c *processingClaim) MarkRetry(ctx context.Context, id int64, availableAt time.Time, lastError string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.store.MarkRetry(ctx, c.leaseOwner, []int64{id}, availableAt, lastError)
	c.releaseLocked(id)
	return err
}

func (c *processingClaim) releaseLocked(id int64) {
	if c == nil || id <= 0 {
		return
	}
	delete(c.active, id)
}

func (c *processingClaim) activeIDsLocked() []int64 {
	ids := make([]int64, 0, len(c.active))
	for id := range c.active {
		ids = append(ids, id)
	}
	return ids
}

func startProcessingLeaseHeartbeat(ctx context.Context, claim *processingClaim, interval time.Duration, onLost func(error)) func() {
	if claim == nil || interval <= 0 {
		return func() {}
	}
	hbCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
			}
			timeout := interval
			if timeout < time.Second {
				timeout = time.Second
			}
			renewCtx, renewCancel := context.WithTimeout(hbCtx, timeout)
			err := claim.RenewActive(renewCtx)
			renewCancel()
			if hbCtx.Err() != nil {
				return
			}
			if err != nil {
				if onLost != nil {
					onLost(err)
				}
				return
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func heartbeatInterval(leaseTTL time.Duration) time.Duration {
	if leaseTTL <= 0 {
		leaseTTL = 5 * time.Minute
	}
	interval := leaseTTL / 2
	if interval <= 0 {
		interval = time.Second
	}
	if interval > time.Minute {
		return time.Minute
	}
	return interval
}

func processingContextError(ctx context.Context) error {
	if err := ctx.Err(); err == nil {
		return nil
	}
	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
		return cause
	}
	return ctx.Err()
}

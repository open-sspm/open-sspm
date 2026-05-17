package inbox

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
	var errs []error
	for _, delivery := range deliveries {
		result, err := p.handler.ProcessInboxDelivery(ctx, delivery)
		if err != nil {
			if markErr := p.retryOrDead(ctx, delivery, err); markErr != nil {
				errs = append(errs, errors.Join(err, markErr))
			} else {
				out.Retried++
			}
			continue
		}
		switch result.normalizedStatus() {
		case ProcessStatusIgnored:
			if err := p.store.MarkIgnored(ctx, p.cfg.LeaseOwner, []int64{delivery.ID}, result.IgnoreReason, result.DecodedSummary); err != nil {
				errs = append(errs, err)
				continue
			}
			out.Ignored++
		case ProcessStatusDead:
			msg := strings.TrimSpace(result.LastError)
			if msg == "" {
				msg = "delivery rejected by processor"
			}
			if err := p.store.MarkDead(ctx, p.cfg.LeaseOwner, []int64{delivery.ID}, msg); err != nil {
				errs = append(errs, err)
				continue
			}
			out.Dead++
		default:
			if err := p.store.MarkProcessed(ctx, p.cfg.LeaseOwner, []int64{delivery.ID}, result.DecodedSummary); err != nil {
				errs = append(errs, err)
				continue
			}
			out.Processed++
		}
	}
	return out, errors.Join(errs...)
}

func (p *Processor) retryOrDead(ctx context.Context, delivery Delivery, cause error) error {
	msg := "processing failed"
	if cause != nil && strings.TrimSpace(cause.Error()) != "" {
		msg = cause.Error()
	}
	if delivery.MaxAttempts > 0 && delivery.AttemptCount >= delivery.MaxAttempts {
		return p.store.MarkDead(ctx, p.cfg.LeaseOwner, []int64{delivery.ID}, msg)
	}
	return p.store.MarkRetry(ctx, p.cfg.LeaseOwner, []int64{delivery.ID}, time.Now().Add(p.retryDelay(delivery.AttemptCount)), msg)
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

package sync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/identity"
	"github.com/open-sspm/open-sspm/internal/metrics"
	"github.com/open-sspm/open-sspm/internal/rules/datasets"
	"github.com/open-sspm/open-sspm/internal/rules/engine"
	"golang.org/x/sync/errgroup"
)

type integrationKey struct {
	kind string
	name string
}

type Orchestrator struct {
	pool           *pgxpool.Pool
	q              *gen.Queries
	registry       *registry.ConnectorRegistry
	reporter       registry.Reporter
	globalEvalMode string
	locks          LockManager
	mode           registry.RunMode
	identityFn     func(context.Context, *gen.Queries) (identity.Stats, error)
	globalEvalFn   func(context.Context, *gen.Queries, string, bool, func(registry.Event)) error

	mu           sync.Mutex
	integrations []registry.Integration
	keys         map[integrationKey]struct{}
	idp          registry.Integration

	timeoutRetryAttempts int
	timeoutRetryDelay    time.Duration
}

const (
	globalEvalModeBestEffort = "best_effort"
	globalEvalModeStrict     = "strict"

	defaultTimeoutRetryAttempts = 2
	defaultTimeoutRetryDelay    = 2 * time.Second
)

var errSyncLockLost = errors.New("sync lock lost")

func NewOrchestrator(pool *pgxpool.Pool, reg *registry.ConnectorRegistry) *Orchestrator {
	return &Orchestrator{
		pool:                 pool,
		q:                    gen.New(pool),
		registry:             reg,
		keys:                 make(map[integrationKey]struct{}),
		globalEvalMode:       globalEvalModeBestEffort,
		mode:                 registry.RunModeFull,
		identityFn:           identity.Resolve,
		globalEvalFn:         runGlobalComplianceEvaluations,
		timeoutRetryAttempts: defaultTimeoutRetryAttempts,
		timeoutRetryDelay:    defaultTimeoutRetryDelay,
	}
}

func (o *Orchestrator) AddIntegration(i registry.Integration) error {
	if i == nil {
		return errors.New("integration is nil")
	}
	if v := reflect.ValueOf(i); v.Kind() == reflect.Pointer && v.IsNil() {
		return errors.New("integration is nil")
	}

	kind := strings.TrimSpace(i.Kind())
	name := strings.TrimSpace(i.Name())
	if kind == "" {
		return errors.New("integration kind is required")
	}
	if name == "" {
		return errors.New("integration name is required")
	}

	role := i.Role()
	switch role {
	case registry.RoleIdP, registry.RoleApp:
	default:
		return fmt.Errorf("integration %s/%s has invalid role %q", kind, name, role)
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	key := integrationKey{kind: kind, name: name}
	if _, ok := o.keys[key]; ok {
		return fmt.Errorf("integration %s/%s already registered", kind, name)
	}

	if role == registry.RoleIdP {
		if o.idp != nil {
			return fmt.Errorf("integration %s/%s: only one idp integration is supported", kind, name)
		}
		o.idp = i
	}

	o.keys[key] = struct{}{}
	o.integrations = append(o.integrations, i)
	return nil
}

func (o *Orchestrator) SetReporter(r registry.Reporter) {
	o.reporter = r
}

func (o *Orchestrator) SetLockManager(m LockManager) {
	o.locks = m
}

func (o *Orchestrator) SetGlobalEvalMode(mode string) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		mode = globalEvalModeBestEffort
	}
	o.globalEvalMode = mode
}

func (o *Orchestrator) SetRunMode(mode registry.RunMode) {
	o.mode = mode.Normalize()
}

func (o *Orchestrator) report(e registry.Event) {
	if o.reporter == nil {
		return
	}
	o.reporter.Report(e)
}

func (o *Orchestrator) RunOnce(ctx context.Context) error {
	o.mu.Lock()
	integrations := append([]registry.Integration(nil), o.integrations...)
	o.mu.Unlock()

	for _, i := range integrations {
		for _, e := range i.InitEvents() {
			o.report(e)
		}
	}

	var (
		g    errgroup.Group
		errs []error
		mu   sync.Mutex

		runErrByKey = make(map[integrationKey]error, len(integrations))
	)

	for _, i := range integrations {
		g.Go(func() error {
			kind := strings.TrimSpace(i.Kind())
			name := strings.TrimSpace(i.Name())
			runKind := registry.SyncRunSourceKind(kind, o.mode)
			start := time.Now()

			lockErr := o.runIntegrationWithRetry(ctx, i)
			duration := time.Since(start).Seconds()
			metrics.SyncDuration.WithLabelValues(runKind, name).Observe(duration)

			status := "success"
			if lockErr != nil {
				status = "failure"
			}
			metrics.SyncRunsTotal.WithLabelValues(runKind, name, status).Inc()

			mu.Lock()
			runErrByKey[integrationKey{kind: kind, name: name}] = lockErr
			mu.Unlock()

			if lockErr != nil {
				err := lockErr
				wrapped := fmt.Errorf("%s sync: %w", strings.TrimSpace(i.Kind()), err)
				slog.Error("integration sync failed", "kind", kind, "name", name, "err", lockErr)
				mu.Lock()
				errs = append(errs, wrapped)
				mu.Unlock()
			} else {
				metrics.SyncLastSuccessTimestamp.WithLabelValues(runKind, name).Set(float64(time.Now().Unix()))

				// Collect resource metrics if registry is available
				if o.registry == nil {
					metrics.SyncMetricsCollectionFailuresTotal.WithLabelValues(runKind, name, "registry_missing").Inc()
					return nil
				}
				def, ok := o.registry.Get(kind)
				if !ok {
					metrics.SyncMetricsCollectionFailuresTotal.WithLabelValues(runKind, name, "definition_missing").Inc()
					return nil
				}
				provider := def.MetricsProvider()
				if provider == nil {
					metrics.SyncMetricsCollectionFailuresTotal.WithLabelValues(runKind, name, "provider_missing").Inc()
					return nil
				}
				m, err := provider.FetchMetrics(ctx, o.q, name)
				if err != nil {
					metrics.SyncMetricsCollectionFailuresTotal.WithLabelValues(runKind, name, "fetch_error").Inc()
					slog.Warn("failed to fetch metrics after sync", "kind", kind, "name", name, "err", err)
					return nil
				}
				metrics.ResourcesTotal.WithLabelValues(kind, name, "total").Set(float64(m.Total))
				metrics.ResourcesTotal.WithLabelValues(kind, name, "matched").Set(float64(m.Matched))
				metrics.ResourcesTotal.WithLabelValues(kind, name, "unmatched").Set(float64(m.Unmatched))
			}
			return nil
		})
	}

	_ = g.Wait()

	if o.mode.Normalize() == registry.RunModeDiscovery {
		err := errors.Join(errs...)
		o.report(registry.Event{Source: "sync", Stage: "done", Done: true, Err: err})
		return err
	}

	identityFn := o.identityFn
	if identityFn == nil {
		identityFn = identity.Resolve
	}

	o.report(registry.Event{Source: "identity", Stage: "resolve", Current: 0, Total: 1, Message: "resolving identity graph"})
	resolveStats, resolveErr := identityFn(ctx, o.q)
	if resolveErr != nil {
		resolveErr = fmt.Errorf("identity resolve: %w", resolveErr)
		errs = append(errs, resolveErr)
		slog.Error("identity resolution failed", "err", resolveErr)
		o.report(registry.Event{Source: "identity", Stage: "resolve", Current: 1, Total: 1, Message: resolveErr.Error(), Err: resolveErr})
	} else {
		if resolveStats.AutoLinked > 0 {
			metrics.AutoLinksTotal.WithLabelValues("identity", "resolver").Add(float64(resolveStats.AutoLinked))
		}
		o.report(registry.Event{
			Source:  "identity",
			Stage:   "resolve",
			Current: 1,
			Total:   1,
			Message: fmt.Sprintf("identity resolution: linked=%d created=%d updated=%d", resolveStats.AutoLinked, resolveStats.NewIdentities, resolveStats.UpdatedIdentites),
		})
	}

	// Run all compliance ruleset evaluations *after* all connectors have synced
	// and identities have been resolved.
	for _, i := range integrations {
		eval, ok := i.(registry.ComplianceEvaluator)
		if !ok {
			continue
		}

		kind := strings.TrimSpace(i.Kind())
		name := strings.TrimSpace(i.Name())
		if runErrByKey[integrationKey{kind: kind, name: name}] != nil {
			continue
		}

		err := o.withConnectorLock(ctx, kind, name, func(lockCtx context.Context) error {
			return eval.EvaluateCompliance(lockCtx, o.q, o.report)
		})
		if err != nil {
			wrapped := fmt.Errorf("%s compliance: %w", kind, err)
			slog.Error("integration compliance evaluation failed", "kind", kind, "name", name, "err", err)
			errs = append(errs, wrapped)
		}
	}

	// Evaluate global-scope rulesets (normalized datasets) after connector syncs and
	// per-integration compliance evaluations have completed.
	anySyncErrors := false
	for _, err := range runErrByKey {
		if err != nil {
			anySyncErrors = true
			break
		}
	}

	globalEvalFn := o.globalEvalFn
	if globalEvalFn == nil {
		globalEvalFn = runGlobalComplianceEvaluations
	}
	if err := globalEvalFn(ctx, o.q, o.globalEvalMode, anySyncErrors, o.report); err != nil {
		wrapped := fmt.Errorf("global compliance: %w", err)
		slog.Error("global compliance evaluation failed", "err", err)
		errs = append(errs, wrapped)
	}

	err := errors.Join(errs...)
	o.report(registry.Event{Source: "sync", Stage: "done", Done: true, Err: err})
	return err
}

func runGlobalComplianceEvaluations(ctx context.Context, q *gen.Queries, mode string, anySyncErrors bool, report func(registry.Event)) error {
	if mode == globalEvalModeStrict && anySyncErrors {
		slog.Info("skipping global compliance evaluation due to integration errors")
		if report != nil {
			report(registry.Event{Source: "rules", Stage: "global", Message: "skipping global compliance evaluation due to integration errors"})
		}
		return nil
	}

	router := datasets.RouterProvider{
		Normalized: &datasets.NormalizedProvider{Q: q},
	}
	globalEngine := engine.Engine{
		Q:        q,
		Datasets: router,
		Now:      time.Now,
	}
	return globalEngine.Run(ctx, engine.Context{ScopeKind: "global", EvaluatedAt: time.Now()})
}

func (o *Orchestrator) runIntegrationWithRetry(ctx context.Context, integration registry.Integration) error {
	kind := strings.TrimSpace(integration.Kind())
	name := strings.TrimSpace(integration.Name())
	mode := o.mode.Normalize()

	attempts := o.timeoutRetryAttempts
	if attempts <= 0 {
		attempts = 1
	}

	delay := max(o.timeoutRetryDelay, 0)

	var runErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt > 1 {
			slog.Warn(
				"retrying integration sync after timeout",
				"kind", kind,
				"name", name,
				"attempt", attempt,
				"max_attempts", attempts,
				"retry_delay", delay,
				"err", runErr,
			)
			if err := sleepWithContext(ctx, delay); err != nil {
				return errors.Join(runErr, err)
			}
		}

		runErr = o.withConnectorLock(ctx, kind, name, func(lockCtx context.Context) error {
			return integration.Run(lockCtx, o.q, o.pool, o.report, mode)
		})
		if runErr == nil {
			return nil
		}
		if !isRetryableTimeoutError(runErr) {
			return runErr
		}
	}
	return runErr
}

func isRetryableTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	// Lock-loss errors may wrap context.DeadlineExceeded; do not retry them.
	if errors.Is(err, errSyncLockLost) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if os.IsTimeout(err) {
		return true
	}

	var timeoutErr interface {
		Timeout() bool
	}
	return errors.As(err, &timeoutErr) && timeoutErr.Timeout()
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (o *Orchestrator) withConnectorLock(ctx context.Context, kind, name string, fn func(context.Context) error) error {
	if o.pool == nil {
		return errors.New("sync pool is nil")
	}
	if o.locks == nil {
		return errors.New("sync lock manager is nil")
	}

	lock, err := o.locks.Acquire(ctx, kind, name)
	if err != nil {
		return err
	}

	fnErr, lost := runWithManagedLock(ctx, lock, fn)
	if lost != nil {
		return errors.Join(fnErr, fmt.Errorf("%w: %w", errSyncLockLost, lost))
	}
	return fnErr
}

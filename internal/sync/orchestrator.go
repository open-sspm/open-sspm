package sync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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

	mu           sync.Mutex
	integrations []registry.Integration
	keys         map[integrationKey]struct{}
	idp          registry.Integration
}

const (
	globalEvalModeBestEffort = "best_effort"
	globalEvalModeStrict     = "strict"
)

func NewOrchestrator(pool *pgxpool.Pool, reg *registry.ConnectorRegistry) *Orchestrator {
	return &Orchestrator{
		pool:           pool,
		q:              gen.New(pool),
		registry:       reg,
		keys:           make(map[integrationKey]struct{}),
		globalEvalMode: globalEvalModeBestEffort,
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
		i := i
		g.Go(func() error {
			kind := strings.TrimSpace(i.Kind())
			name := strings.TrimSpace(i.Name())
			start := time.Now()

			lockErr := o.withConnectorLock(ctx, kind, name, func(lockCtx context.Context) error {
				return i.Run(lockCtx, registry.IntegrationDeps{Q: o.q, Pool: o.pool, Report: o.report})
			})
			duration := time.Since(start).Seconds()
			metrics.SyncDuration.WithLabelValues(kind, name).Observe(duration)

			status := "success"
			if lockErr != nil {
				status = "failure"
			}
			metrics.SyncRunsTotal.WithLabelValues(kind, name, status).Inc()

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
				metrics.SyncLastSuccessTimestamp.WithLabelValues(kind, name).Set(float64(time.Now().Unix()))

				// Collect resource metrics if registry is available
				if o.registry == nil {
					metrics.SyncMetricsCollectionFailuresTotal.WithLabelValues(kind, name, "registry_missing").Inc()
					return nil
				}
				def, ok := o.registry.Get(kind)
				if !ok {
					metrics.SyncMetricsCollectionFailuresTotal.WithLabelValues(kind, name, "definition_missing").Inc()
					return nil
				}
				provider := def.MetricsProvider()
				if provider == nil {
					metrics.SyncMetricsCollectionFailuresTotal.WithLabelValues(kind, name, "provider_missing").Inc()
					return nil
				}
				m, err := provider.FetchMetrics(ctx, o.q, name)
				if err != nil {
					metrics.SyncMetricsCollectionFailuresTotal.WithLabelValues(kind, name, "fetch_error").Inc()
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

	o.report(registry.Event{Source: "identity", Stage: "resolve", Current: 0, Total: 1, Message: "resolving identity graph"})
	resolveStats, resolveErr := identity.Resolve(ctx, o.q)
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
			return eval.EvaluateCompliance(lockCtx, registry.IntegrationDeps{Q: o.q, Pool: o.pool, Report: o.report})
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

	router := datasets.RouterProvider{
		Normalized: &datasets.NormalizedProvider{Q: o.q},
	}
	globalEngine := engine.Engine{
		Q:        o.q,
		Datasets: router,
		Now:      time.Now,
	}
	if o.globalEvalMode == globalEvalModeStrict && anySyncErrors {
		slog.Info("skipping global compliance evaluation due to integration errors")
		o.report(registry.Event{Source: "rules", Stage: "global", Message: "skipping global compliance evaluation due to integration errors"})
	} else {
		if err := globalEngine.Run(ctx, engine.Context{ScopeKind: "global", EvaluatedAt: time.Now()}); err != nil {
			wrapped := fmt.Errorf("global compliance: %w", err)
			slog.Error("global compliance evaluation failed", "err", err)
			errs = append(errs, wrapped)
		}
	}

	err := errors.Join(errs...)
	o.report(registry.Event{Source: "sync", Stage: "done", Done: true, Err: err})
	return err
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

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	var (
		lockLostMu sync.Mutex
		lockLost   error
	)
	stopHeartbeat := lock.StartHeartbeat(runCtx, func(err error) {
		lockLostMu.Lock()
		if lockLost == nil {
			lockLost = err
		}
		lockLostMu.Unlock()

		slog.Error("sync lock heartbeat failed", "scope_kind", lock.ScopeKind(), "scope_name", lock.ScopeName(), "err", err)
		cancelRun()
	})

	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := lock.Release(unlockCtx); err != nil {
			slog.Warn("failed to release sync lock", "scope_kind", lock.ScopeKind(), "scope_name", lock.ScopeName(), "err", err)
		}
	}()
	defer stopHeartbeat()

	fnErr := fn(runCtx)

	lockLostMu.Lock()
	lost := lockLost
	lockLostMu.Unlock()
	if lost != nil {
		return errors.Join(fnErr, fmt.Errorf("sync lock lost: %w", lost))
	}
	return fnErr
}

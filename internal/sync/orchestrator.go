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
	"github.com/open-sspm/open-sspm/internal/matching"
	"github.com/open-sspm/open-sspm/internal/opensspm"
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
	reporter       registry.Reporter
	globalEvalMode string

	mu           sync.Mutex
	integrations []registry.Integration
	keys         map[integrationKey]struct{}
	idp          registry.Integration
}

const (
	globalEvalModeBestEffort = "best_effort"
	globalEvalModeStrict     = "strict"
)

func NewOrchestrator(pool *pgxpool.Pool) *Orchestrator {
	return &Orchestrator{
		pool:           pool,
		q:              gen.New(pool),
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

	var apps []registry.Integration
	for _, i := range integrations {
		if i.Role() == registry.RoleApp {
			apps = append(apps, i)
		}
	}

	autoLinkTotal := int64(len(apps))
	if autoLinkTotal > 0 {
		slog.Info("matching auto-linking by email", "apps", autoLinkTotal)
		o.report(registry.Event{Source: "matching", Stage: "auto-link", Current: 0, Total: autoLinkTotal, Message: "auto-linking by email"})
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

			lockErr := o.withConnectorLock(ctx, kind, name, func() error {
				return i.Run(ctx, registry.IntegrationDeps{Q: o.q, Pool: o.pool, Report: o.report})
			})
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
			}
			return nil
		})
	}

	_ = g.Wait()

	if autoLinkTotal > 0 {
		var autoLinkDone int64
		for _, app := range apps {
			n, err := matching.AutoLinkByEmail(ctx, o.q, app.Kind(), app.Name())
			autoLinkDone++
			if err != nil {
				err = fmt.Errorf("auto-link %s: %w", strings.TrimSpace(app.Kind()), err)
				o.report(registry.Event{Source: "matching", Stage: "auto-link", Current: autoLinkDone, Total: autoLinkTotal, Message: err.Error(), Err: err})
				slog.Error("matching auto-link failed", "kind", strings.TrimSpace(app.Kind()), "name", strings.TrimSpace(app.Name()), "err", err)
				errs = append(errs, err)
			} else {
				slog.Info("matching auto-link complete", "kind", strings.TrimSpace(app.Kind()), "name", strings.TrimSpace(app.Name()), "new_links", n)
				o.report(registry.Event{Source: "matching", Stage: "auto-link", Current: autoLinkDone, Total: autoLinkTotal, Message: fmt.Sprintf("%s auto-link: %d new links", strings.TrimSpace(app.Kind()), n)})
			}
		}
	}

	// Run all compliance ruleset evaluations *after* all connectors have synced
	// and accounts have been auto-linked.
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

		err := o.withConnectorLock(ctx, kind, name, func() error {
			return eval.EvaluateCompliance(ctx, registry.IntegrationDeps{Q: o.q, Pool: o.pool, Report: o.report})
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
		Q: o.q,
		Datasets: opensspm.RuntimeDatasetProviderAdapter{
			Provider:      router,
			CapabilitiesV: datasets.RuntimeCapabilities(router),
		},
		Now: time.Now,
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

func (o *Orchestrator) withConnectorLock(ctx context.Context, kind, name string, fn func() error) error {
	if o.pool == nil {
		return errors.New("sync pool is nil")
	}

	kind = strings.TrimSpace(kind)
	name = strings.TrimSpace(name)
	lockConn, err := o.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	lockQ := gen.New(lockConn)
	lockKey := registry.ConnectorLockKey(kind, name)

	err = lockQ.AcquireAdvisoryLock(ctx, lockKey)
	if err != nil {
		lockConn.Release()
		return err
	}

	defer lockConn.Release()
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = lockQ.ReleaseAdvisoryLock(unlockCtx, lockKey)
	}()

	return fn()
}

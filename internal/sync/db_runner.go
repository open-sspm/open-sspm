package sync

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

type DBRunner struct {
	pool     *pgxpool.Pool
	q        *gen.Queries
	registry *registry.ConnectorRegistry
	reporter registry.Reporter
}

func NewDBRunner(pool *pgxpool.Pool, reg *registry.ConnectorRegistry) *DBRunner {
	q := gen.New(pool)
	return &DBRunner{
		pool:     pool,
		q:        q,
		registry: reg,
	}
}

func (r *DBRunner) SetReporter(reporter registry.Reporter) {
	r.reporter = reporter
}

func (r *DBRunner) RunOnce(ctx context.Context) error {
	if r == nil || r.q == nil || r.pool == nil {
		return ErrNoEnabledConnectors
	}

	configs, err := r.q.ListConnectorConfigs(ctx)
	if err != nil {
		return err
	}

	orchestrator := NewOrchestrator(r.pool)
	if r.reporter != nil {
		orchestrator.SetReporter(r.reporter)
	}

	var (
		errList          []error
		integrationCount int
		enabledKinds     []string
		disabledKinds    []string
		skippedKinds     []string
		planned          []string
	)

	for _, cfgRow := range configs {
		kind := strings.TrimSpace(cfgRow.Kind)
		if kind == "" {
			continue
		}

		if !cfgRow.Enabled {
			disabledKinds = append(disabledKinds, kind)
			continue
		}
		enabledKinds = append(enabledKinds, kind)

		def, ok := r.registry.Get(kind)
		if !ok {
			log.Printf("unknown connector kind %q skipped", kind)
			skippedKinds = append(skippedKinds, kind)
			continue
		}

		cfg, err := def.DecodeConfig(cfgRow.Config)
		if err != nil {
			errList = append(errList, fmt.Errorf("%s config: %w", kind, err))
			skippedKinds = append(skippedKinds, kind)
			continue
		}

		if err := def.ValidateConfig(cfg); err != nil {
			errList = append(errList, fmt.Errorf("%s config: %w", kind, err))
			skippedKinds = append(skippedKinds, kind)
			continue
		}

		integration, err := def.NewIntegration(cfg)
		if err != nil {
			errList = append(errList, fmt.Errorf("%s integration: %w", kind, err))
			skippedKinds = append(skippedKinds, kind)
			continue
		}

		if integration == nil {
			// Not syncable (e.g. Vault stub)
			skippedKinds = append(skippedKinds, kind)
			continue
		}

		if err := orchestrator.AddIntegration(integration); err != nil {
			errList = append(errList, err)
			skippedKinds = append(skippedKinds, kind)
			continue
		}
		integrationCount++
		planned = append(planned, strings.TrimSpace(integration.Kind())+"/"+strings.TrimSpace(integration.Name()))
	}

	if r.reporter != nil {
		if len(enabledKinds) > 0 {
			r.reporter.Report(registry.Event{Source: "sync", Stage: "plan", Message: "enabled connectors: " + strings.Join(enabledKinds, ", ")})
		}
		if len(disabledKinds) > 0 {
			r.reporter.Report(registry.Event{Source: "sync", Stage: "plan", Message: "disabled connectors: " + strings.Join(disabledKinds, ", ")})
		}
		if len(skippedKinds) > 0 {
			r.reporter.Report(registry.Event{Source: "sync", Stage: "plan", Message: "skipped connectors: " + strings.Join(skippedKinds, ", ")})
		}
		if len(planned) > 0 {
			r.reporter.Report(registry.Event{Source: "sync", Stage: "plan", Message: "planned integrations: " + strings.Join(planned, ", ")})
		}
	}

	if integrationCount == 0 {
		if len(errList) > 0 {
			return errors.Join(ErrNoEnabledConnectors, errors.Join(errList...))
		}
		return ErrNoEnabledConnectors
	}

	runErr := orchestrator.RunOnce(ctx)
	if len(errList) > 0 {
		return errors.Join(runErr, errors.Join(errList...))
	}
	return runErr
}

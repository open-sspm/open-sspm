package registry

import (
	"context"
	"fmt"
	"strings"

	"github.com/open-sspm/open-sspm/internal/db/gen"
)

// ConnectorRegistry is the central registry for all connectors.
type ConnectorRegistry struct {
	definitions map[string]ConnectorDefinition
	order       []string // Display order
}

// NewRegistry creates a new connector registry.
func NewRegistry() *ConnectorRegistry {
	return &ConnectorRegistry{
		definitions: make(map[string]ConnectorDefinition),
		order:       make([]string, 0),
	}
}

// Register adds a connector definition to the registry.
func (r *ConnectorRegistry) Register(def ConnectorDefinition) error {
	kind := strings.ToLower(strings.TrimSpace(def.Kind()))
	if kind == "" {
		return fmt.Errorf("connector kind cannot be empty")
	}
	if _, exists := r.definitions[kind]; exists {
		return fmt.Errorf("connector kind %q already registered", kind)
	}
	r.definitions[kind] = def
	r.order = append(r.order, kind)
	return nil
}

// Get retrieves a connector definition by kind.
func (r *ConnectorRegistry) Get(kind string) (ConnectorDefinition, bool) {
	def, ok := r.definitions[strings.ToLower(strings.TrimSpace(kind))]
	return def, ok
}

// All returns all registered connector definitions in order.
func (r *ConnectorRegistry) All() []ConnectorDefinition {
	defs := make([]ConnectorDefinition, 0, len(r.order))
	for _, kind := range r.order {
		defs = append(defs, r.definitions[kind])
	}
	return defs
}

// LoadStates loads the state of all connectors from the database.
func (r *ConnectorRegistry) LoadStates(ctx context.Context, q *gen.Queries) ([]ConnectorState, error) {
	return r.loadStatesInternal(ctx, q, false)
}

// LoadStatesWithMetrics loads the state of all connectors including metrics.
func (r *ConnectorRegistry) LoadStatesWithMetrics(ctx context.Context, q *gen.Queries) ([]ConnectorState, error) {
	return r.loadStatesInternal(ctx, q, true)
}

func (r *ConnectorRegistry) loadStatesInternal(ctx context.Context, q *gen.Queries, withMetrics bool) ([]ConnectorState, error) {
	configs, err := q.ListConnectorConfigs(ctx)
	if err != nil {
		return nil, err
	}

	configMap := make(map[string]gen.ConnectorConfig)
	for _, cfg := range configs {
		configMap[strings.ToLower(strings.TrimSpace(cfg.Kind))] = cfg
	}

	states := make([]ConnectorState, 0, len(r.order))
	for _, kind := range r.order {
		def := r.definitions[kind]
		state := ConnectorState{
			Definition: def,
		}

		if cfgRow, ok := configMap[kind]; ok {
			state.Enabled = cfgRow.Enabled
			cfg, err := def.DecodeConfig(cfgRow.Config)
			if err != nil {
				// Log error but continue? For now, return error to be safe
				return nil, fmt.Errorf("decode config for %s: %w", kind, err)
			}
			state.Config = cfg
			state.Configured = def.IsConfigured(cfg)
			state.SourceName = def.SourceName(cfg)
		}

		if withMetrics && state.Configured && state.Enabled {
			if provider := def.MetricsProvider(); provider != nil {
				m, err := provider.FetchMetrics(ctx, q, state.SourceName)
				if err != nil {
					// Don't fail the whole request if metrics fail; just log (or ignore) and continue.
					// Ideally we'd log this, but we don't have a logger passed in here easily.
					// We'll leave Metrics as nil.
				} else {
					state.Metrics = &m
				}
			}
		}

		states = append(states, state)
	}

	return states, nil
}

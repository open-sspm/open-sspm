package registry

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

// ConnectorRegistry is the central registry for all connectors.
type ConnectorRegistry struct {
	definitions        map[string]ConnectorDefinition
	order              []string // Display order
	connectorSecretKey []byte
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

func (r *ConnectorRegistry) SetConnectorSecretKey(key []byte) {
	if len(key) == 0 {
		r.connectorSecretKey = nil
		return
	}
	r.connectorSecretKey = append([]byte(nil), key...)
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
	store := configstore.NewStore(nil, q, r.connectorSecretKey)
	rows, secretRowsByKind, err := store.ListConnectorConfigsWithSecretRows(ctx)
	if err != nil {
		return nil, err
	}

	configRows := make(map[string]gen.ConnectorConfig, len(rows))
	configMap := make(map[string]configstore.ResolvedConnectorConfig, len(rows))
	configErrors := make(map[string]error)
	for _, row := range rows {
		kind := strings.ToLower(strings.TrimSpace(row.Kind))
		configRows[kind] = row

		resolved, err := store.ResolveConnectorConfigRowWithSecretRows(row, secretRowsByKind[kind])
		if err != nil {
			configErrors[kind] = err
			continue
		}
		configMap[kind] = resolved
	}

	states := make([]ConnectorState, 0, len(r.order))
	for _, kind := range r.order {
		def := r.definitions[kind]
		state := ConnectorState{
			Definition: def,
		}

		if row, ok := configRows[kind]; ok {
			state.Enabled = row.Enabled
		}

		if err, ok := configErrors[kind]; ok {
			state.ConfigError = fmt.Sprintf("resolve config for %s: %v", kind, err)
			slog.Warn("connector config resolve failed", "kind", kind, "err", err)
			states = append(states, state)
			continue
		}

		if cfgRow, ok := configMap[kind]; ok {
			cfg, err := def.DecodeConfig(cfgRow.ResolvedConfig)
			if err != nil {
				state.ConfigError = fmt.Sprintf("decode config for %s: %v", kind, err)
				slog.Warn("connector config decode failed", "kind", kind, "err", err)
				states = append(states, state)
				continue
			}
			state.Config = cfg
			state.Configured = def.IsConfigured(cfg)
			state.SourceName = def.SourceName(cfg)
		}

		if withMetrics && state.Configured && state.Enabled {
			if provider := def.MetricsProvider(); provider != nil {
				m, err := provider.FetchMetrics(ctx, q, state.SourceName)
				if err != nil {
					slog.Warn("connector metrics fetch failed", "kind", kind, "name", state.SourceName, "err", err)
				} else {
					state.Metrics = &m
				}
			}
		}

		states = append(states, state)
	}

	return states, nil
}

func (r *ConnectorRegistry) DecodeConfigRow(ctx context.Context, q *gen.Queries, row gen.ConnectorConfig) (any, error) {
	kind := strings.ToLower(strings.TrimSpace(row.Kind))
	def, ok := r.Get(kind)
	if !ok {
		return nil, fmt.Errorf("connector kind %q is not registered", row.Kind)
	}
	store := configstore.NewStore(nil, q, r.connectorSecretKey)
	resolved, err := store.ResolveConnectorConfigRow(ctx, row)
	if err != nil {
		return nil, err
	}
	return def.DecodeConfig(resolved.ResolvedConfig)
}

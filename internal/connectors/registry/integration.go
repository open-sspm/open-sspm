package registry

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

type IntegrationRole string

const (
	RoleIdP IntegrationRole = "idp"
	RoleApp IntegrationRole = "app"
)

type IntegrationDeps struct {
	Q      *gen.Queries
	Pool   *pgxpool.Pool
	Report func(Event)
	Mode   RunMode
}

type Integration interface {
	Kind() string
	Name() string
	Role() IntegrationRole
	InitEvents() []Event
	Run(context.Context, IntegrationDeps) error
}

// ComplianceEvaluator is an optional interface that integrations can implement
// to run compliance ruleset evaluations after all connectors have synced.
type ComplianceEvaluator interface {
	EvaluateCompliance(context.Context, IntegrationDeps) error
}

// ModeAwareIntegration is an optional interface that integrations can implement
// to declare whether they support a given runner mode.
type ModeAwareIntegration interface {
	SupportsRunMode(RunMode) bool
}

const UnknownTotal int64 = -1

type Reporter interface {
	Report(Event)
}

type Event struct {
	Source  string
	Stage   string
	Current int64
	Total   int64
	Message string
	Done    bool
	Err     error
	At      time.Time
}

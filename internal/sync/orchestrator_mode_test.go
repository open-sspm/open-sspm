package sync

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/identity"
)

type orchestratorTestLock struct {
	scopeKind string
	scopeName string
}

func (l orchestratorTestLock) ScopeKind() string { return l.scopeKind }
func (l orchestratorTestLock) ScopeName() string { return l.scopeName }
func (l orchestratorTestLock) StartHeartbeat(context.Context, func(error)) func() {
	return func() {}
}
func (l orchestratorTestLock) Release(context.Context) error { return nil }

type orchestratorTestLockManager struct{}

func (orchestratorTestLockManager) TryAcquire(_ context.Context, scopeKind, scopeName string) (Lock, bool, error) {
	return orchestratorTestLock{scopeKind: scopeKind, scopeName: scopeName}, true, nil
}

func (orchestratorTestLockManager) Acquire(_ context.Context, scopeKind, scopeName string) (Lock, error) {
	return orchestratorTestLock{scopeKind: scopeKind, scopeName: scopeName}, nil
}

type orchestratorCountingIntegration struct {
	runCount        int
	complianceCount int
}

func (i *orchestratorCountingIntegration) Kind() string { return "okta" }
func (i *orchestratorCountingIntegration) Name() string { return "example.okta.com" }
func (i *orchestratorCountingIntegration) Role() registry.IntegrationRole {
	return registry.RoleIdP
}
func (i *orchestratorCountingIntegration) InitEvents() []registry.Event { return nil }
func (i *orchestratorCountingIntegration) Run(context.Context, *gen.Queries, *pgxpool.Pool, func(registry.Event), registry.RunMode) error {
	i.runCount++
	return nil
}
func (i *orchestratorCountingIntegration) EvaluateCompliance(context.Context, *gen.Queries, func(registry.Event)) error {
	i.complianceCount++
	return nil
}

func TestOrchestrator_DiscoveryModeSkipsPostProcessing(t *testing.T) {
	t.Parallel()

	orch := NewOrchestrator(&pgxpool.Pool{}, nil)
	orch.SetLockManager(orchestratorTestLockManager{})
	orch.SetRunMode(registry.RunModeDiscovery)

	var identityCalled bool
	orch.identityFn = func(context.Context, *gen.Queries) (identity.Stats, error) {
		identityCalled = true
		return identity.Stats{}, nil
	}

	var globalCalled bool
	orch.globalEvalFn = func(context.Context, *gen.Queries, string, bool, func(registry.Event)) error {
		globalCalled = true
		return nil
	}

	integration := &orchestratorCountingIntegration{}
	if err := orch.AddIntegration(integration); err != nil {
		t.Fatalf("AddIntegration() error = %v", err)
	}

	if err := orch.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if integration.runCount != 1 {
		t.Fatalf("runCount = %d, want 1", integration.runCount)
	}
	if integration.complianceCount != 0 {
		t.Fatalf("complianceCount = %d, want 0", integration.complianceCount)
	}
	if identityCalled {
		t.Fatalf("identity resolver should be skipped in discovery mode")
	}
	if globalCalled {
		t.Fatalf("global evaluator should be skipped in discovery mode")
	}
}

func TestOrchestrator_FullModeRunsPostProcessing(t *testing.T) {
	t.Parallel()

	orch := NewOrchestrator(&pgxpool.Pool{}, nil)
	orch.SetLockManager(orchestratorTestLockManager{})
	orch.SetRunMode(registry.RunModeFull)

	var identityCalled bool
	orch.identityFn = func(context.Context, *gen.Queries) (identity.Stats, error) {
		identityCalled = true
		return identity.Stats{}, nil
	}

	var globalCalled bool
	orch.globalEvalFn = func(context.Context, *gen.Queries, string, bool, func(registry.Event)) error {
		globalCalled = true
		return nil
	}

	integration := &orchestratorCountingIntegration{}
	if err := orch.AddIntegration(integration); err != nil {
		t.Fatalf("AddIntegration() error = %v", err)
	}

	if err := orch.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if integration.runCount != 1 {
		t.Fatalf("runCount = %d, want 1", integration.runCount)
	}
	if integration.complianceCount != 1 {
		t.Fatalf("complianceCount = %d, want 1", integration.complianceCount)
	}
	if !identityCalled {
		t.Fatalf("identity resolver should run in full mode")
	}
	if !globalCalled {
		t.Fatalf("global evaluator should run in full mode")
	}
}

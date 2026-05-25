package sync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/identity"
	"github.com/open-sspm/open-sspm/internal/readmodels"
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
	runErr          error
	runFn           func(context.Context, *gen.Queries)
	kind            string
	name            string
	role            registry.IntegrationRole
}

func (i *orchestratorCountingIntegration) Kind() string {
	if i.kind == "" {
		return "okta"
	}
	return i.kind
}
func (i *orchestratorCountingIntegration) Name() string {
	if i.name == "" {
		return "example.okta.com"
	}
	return i.name
}
func (i *orchestratorCountingIntegration) Role() registry.IntegrationRole {
	if i.role == "" {
		return registry.RoleIdP
	}
	return i.role
}
func (i *orchestratorCountingIntegration) InitEvents() []registry.Event { return nil }
func (i *orchestratorCountingIntegration) Run(ctx context.Context, q *gen.Queries, _ *pgxpool.Pool, _ func(registry.Event), _ registry.RunMode) error {
	i.runCount++
	if i.runFn != nil {
		i.runFn(ctx, q)
	}
	return i.runErr
}
func (i *orchestratorCountingIntegration) EvaluateCompliance(context.Context, *gen.Queries, *pgxpool.Pool, func(registry.Event)) error {
	i.complianceCount++
	return nil
}

func TestOrchestrator_DiscoveryModeSkipsPostProcessing(t *testing.T) {
	t.Parallel()

	orch := NewOrchestrator(&pgxpool.Pool{}, nil)
	orch.SetLockManager(orchestratorTestLockManager{})
	orch.SetRunMode(registry.RunModeDiscovery)

	var identityCalled bool
	orch.identityFn = func(context.Context, *gen.Queries, []string, []string) (identity.Stats, error) {
		identityCalled = true
		return identity.Stats{}, nil
	}

	var globalCalled bool
	orch.globalEvalFn = func(context.Context, *gen.Queries, *pgxpool.Pool, string, bool, func(registry.Event)) error {
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

func TestOrchestrator_TailModeSkipsPostProcessing(t *testing.T) {
	t.Parallel()

	orch := NewOrchestrator(&pgxpool.Pool{}, nil)
	orch.SetLockManager(orchestratorTestLockManager{})
	orch.SetRunMode(registry.RunModeTail)

	var identityCalled bool
	orch.identityFn = func(context.Context, *gen.Queries, []string, []string) (identity.Stats, error) {
		identityCalled = true
		return identity.Stats{}, nil
	}

	var globalCalled bool
	orch.globalEvalFn = func(context.Context, *gen.Queries, *pgxpool.Pool, string, bool, func(registry.Event)) error {
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
		t.Fatalf("identity resolver should be skipped in tail mode")
	}
	if globalCalled {
		t.Fatalf("global evaluator should be skipped in tail mode")
	}
}

func TestOrchestrator_FullModeRunsPostProcessing(t *testing.T) {
	t.Parallel()

	orch := NewOrchestrator(&pgxpool.Pool{}, nil)
	orch.SetLockManager(orchestratorTestLockManager{})
	orch.SetRunMode(registry.RunModeFull)

	var identityCalled bool
	orch.identityFn = func(context.Context, *gen.Queries, []string, []string) (identity.Stats, error) {
		identityCalled = true
		return identity.Stats{}, nil
	}

	var globalCalled bool
	orch.globalEvalFn = func(context.Context, *gen.Queries, *pgxpool.Pool, string, bool, func(registry.Event)) error {
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

func TestOrchestrator_PassesReadModelConfigThroughIntegrationContext(t *testing.T) {
	t.Parallel()

	orch := NewOrchestrator(&pgxpool.Pool{}, nil)
	orch.SetLockManager(orchestratorTestLockManager{})
	orch.SetRunMode(registry.RunModeDiscovery)
	orch.SetReadModelConfig(readmodels.RefreshConfig{SyncInterval: 20 * time.Minute})

	integration := &orchestratorCountingIntegration{
		kind: "okta",
		name: "example.okta.com",
		runFn: func(ctx context.Context, q *gen.Queries) {
			projector := readmodels.ProjectorFromContext(ctx, q)
			if projector == nil {
				t.Fatalf("ProjectorFromContext() = nil, want projector")
			}
		},
	}
	if err := orch.AddIntegration(integration); err != nil {
		t.Fatalf("AddIntegration() error = %v", err)
	}

	if err := orch.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
}

func TestOrchestrator_StrictModeSkipsGlobalComplianceWhenIdentityResolutionFails(t *testing.T) {
	t.Parallel()

	orch := NewOrchestrator(&pgxpool.Pool{}, nil)
	orch.SetLockManager(orchestratorTestLockManager{})
	orch.SetRunMode(registry.RunModeFull)
	orch.SetGlobalEvalMode(globalEvalModeStrict)

	identityErr := errors.New("identity exploded")
	orch.identityFn = func(context.Context, *gen.Queries, []string, []string) (identity.Stats, error) {
		return identity.Stats{}, identityErr
	}

	var globalRan bool
	var globalSkipped bool
	orch.globalEvalFn = func(_ context.Context, _ *gen.Queries, _ *pgxpool.Pool, mode string, hasPrerequisiteErrors bool, _ func(registry.Event)) error {
		if mode == globalEvalModeStrict && hasPrerequisiteErrors {
			globalSkipped = true
			return nil
		}
		globalRan = true
		return nil
	}

	integration := &orchestratorCountingIntegration{}
	if err := orch.AddIntegration(integration); err != nil {
		t.Fatalf("AddIntegration() error = %v", err)
	}

	err := orch.RunOnce(context.Background())
	if err == nil {
		t.Fatalf("RunOnce() error = nil, want identity error")
	}
	if integration.complianceCount != 1 {
		t.Fatalf("complianceCount = %d, want 1", integration.complianceCount)
	}
	if globalRan {
		t.Fatalf("global evaluator ran, want skipped")
	}
	if !globalSkipped {
		t.Fatalf("global evaluator skip flag = false, want true")
	}
}

func TestOrchestrator_BestEffortModeRunsGlobalComplianceWhenIdentityResolutionFails(t *testing.T) {
	t.Parallel()

	orch := NewOrchestrator(&pgxpool.Pool{}, nil)
	orch.SetLockManager(orchestratorTestLockManager{})
	orch.SetRunMode(registry.RunModeFull)
	orch.SetGlobalEvalMode(globalEvalModeBestEffort)

	identityErr := errors.New("identity exploded")
	orch.identityFn = func(context.Context, *gen.Queries, []string, []string) (identity.Stats, error) {
		return identity.Stats{}, identityErr
	}

	var globalRan bool
	var receivedPrereqErrors bool
	orch.globalEvalFn = func(_ context.Context, _ *gen.Queries, _ *pgxpool.Pool, mode string, hasPrerequisiteErrors bool, _ func(registry.Event)) error {
		if mode != globalEvalModeBestEffort {
			t.Fatalf("mode = %q, want %q", mode, globalEvalModeBestEffort)
		}
		receivedPrereqErrors = hasPrerequisiteErrors
		globalRan = true
		return nil
	}

	integration := &orchestratorCountingIntegration{}
	if err := orch.AddIntegration(integration); err != nil {
		t.Fatalf("AddIntegration() error = %v", err)
	}

	err := orch.RunOnce(context.Background())
	if err == nil {
		t.Fatalf("RunOnce() error = nil, want identity error")
	}
	if integration.complianceCount != 1 {
		t.Fatalf("complianceCount = %d, want 1", integration.complianceCount)
	}
	if !globalRan {
		t.Fatalf("global evaluator did not run in best-effort mode")
	}
	if !receivedPrereqErrors {
		t.Fatalf("hasPrerequisiteErrors = false, want true")
	}
}

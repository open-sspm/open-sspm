package sync

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

type orchestratorRetryIntegration struct {
	kind  string
	name  string
	role  registry.IntegrationRole
	errs  []error
	calls int
}

func (i *orchestratorRetryIntegration) Kind() string { return i.kind }
func (i *orchestratorRetryIntegration) Name() string { return i.name }
func (i *orchestratorRetryIntegration) Role() registry.IntegrationRole {
	return i.role
}
func (i *orchestratorRetryIntegration) InitEvents() []registry.Event { return nil }
func (i *orchestratorRetryIntegration) Run(context.Context, *gen.Queries, *pgxpool.Pool, func(registry.Event), registry.RunMode) error {
	idx := i.calls
	i.calls++
	if idx < len(i.errs) {
		return i.errs[idx]
	}
	return nil
}

type orchestratorTimeoutErr struct{}

func (orchestratorTimeoutErr) Error() string { return "timeout" }
func (orchestratorTimeoutErr) Timeout() bool { return true }

func TestOrchestrator_RetriesTimeoutFailures(t *testing.T) {
	t.Parallel()

	orch := NewOrchestrator(&pgxpool.Pool{}, nil)
	orch.SetLockManager(orchestratorTestLockManager{})
	orch.SetRunMode(registry.RunModeDiscovery)
	orch.timeoutRetryAttempts = 2
	orch.timeoutRetryDelay = 0

	integration := &orchestratorRetryIntegration{
		kind: "okta",
		name: "example.okta.com",
		role: registry.RoleIdP,
		errs: []error{context.DeadlineExceeded, nil},
	}
	if err := orch.AddIntegration(integration); err != nil {
		t.Fatalf("AddIntegration() error = %v", err)
	}

	if err := orch.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if integration.calls != 2 {
		t.Fatalf("calls = %d, want 2", integration.calls)
	}
}

func TestOrchestrator_DoesNotRetryNonTimeoutFailures(t *testing.T) {
	t.Parallel()

	orch := NewOrchestrator(&pgxpool.Pool{}, nil)
	orch.SetLockManager(orchestratorTestLockManager{})
	orch.SetRunMode(registry.RunModeDiscovery)
	orch.timeoutRetryAttempts = 2
	orch.timeoutRetryDelay = 0

	integration := &orchestratorRetryIntegration{
		kind: "okta",
		name: "example.okta.com",
		role: registry.RoleIdP,
		errs: []error{errors.New("boom")},
	}
	if err := orch.AddIntegration(integration); err != nil {
		t.Fatalf("AddIntegration() error = %v", err)
	}

	if err := orch.RunOnce(context.Background()); err == nil {
		t.Fatalf("RunOnce() error = nil, want non-nil")
	}
	if integration.calls != 1 {
		t.Fatalf("calls = %d, want 1", integration.calls)
	}
}

func TestOrchestrator_TimeoutRetryHonorsMaxAttempts(t *testing.T) {
	t.Parallel()

	orch := NewOrchestrator(&pgxpool.Pool{}, nil)
	orch.SetLockManager(orchestratorTestLockManager{})
	orch.SetRunMode(registry.RunModeDiscovery)
	orch.timeoutRetryAttempts = 2
	orch.timeoutRetryDelay = 0

	integration := &orchestratorRetryIntegration{
		kind: "okta",
		name: "example.okta.com",
		role: registry.RoleIdP,
		errs: []error{context.DeadlineExceeded, context.DeadlineExceeded},
	}
	if err := orch.AddIntegration(integration); err != nil {
		t.Fatalf("AddIntegration() error = %v", err)
	}

	err := orch.RunOnce(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RunOnce() error = %v, want deadline exceeded", err)
	}
	if integration.calls != 2 {
		t.Fatalf("calls = %d, want 2", integration.calls)
	}
}

func TestIsRetryableTimeoutError(t *testing.T) {
	t.Parallel()

	if !isRetryableTimeoutError(context.DeadlineExceeded) {
		t.Fatalf("deadline exceeded should be retryable")
	}
	if !isRetryableTimeoutError(orchestratorTimeoutErr{}) {
		t.Fatalf("timeout-compatible error should be retryable")
	}
	if isRetryableTimeoutError(context.Canceled) {
		t.Fatalf("context canceled should not be retryable")
	}
	if isRetryableTimeoutError(errors.New("boom")) {
		t.Fatalf("generic error should not be retryable")
	}
}

func TestOrchestrator_DoesNotRetryLockLostErrors(t *testing.T) {
	t.Parallel()

	lockLost := errors.Join(
		errors.New("integration failed"),
		fmt.Errorf("%w: %w", errSyncLockLost, context.DeadlineExceeded),
	)
	if isRetryableTimeoutError(lockLost) {
		t.Fatalf("lock-lost error should not be retryable")
	}
}

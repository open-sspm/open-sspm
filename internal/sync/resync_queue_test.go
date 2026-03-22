package sync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

type stubSyncJobStore struct {
	enqueueManualErr      error
	enqueueScheduledErr   error
	enqueueManualCalls    int
	enqueueScheduledCalls int
	enqueueLane           string
	enqueueKind           string
	enqueueSource         string
	scheduledRequests     []TriggerRequest
}

type stubManualPlanner struct {
	err    error
	calls  int
	forced bool
	scope  TriggerRequest
}

func (p *stubManualPlanner) Prepare(ctx context.Context) error {
	p.calls++
	p.forced = IsForcedSync(ctx)
	if kind, name, ok := ConnectorScopeFromContext(ctx); ok {
		p.scope = TriggerRequest{ConnectorKind: kind, SourceName: name}
	}
	return p.err
}

func (s *stubSyncJobStore) EnqueueManualSyncJob(_ context.Context, lane, connectorKind, sourceName string) error {
	s.enqueueManualCalls++
	s.enqueueLane = lane
	s.enqueueKind = connectorKind
	s.enqueueSource = sourceName
	return s.enqueueManualErr
}

func (s *stubSyncJobStore) EnqueueScheduledSyncJob(_ context.Context, lane, connectorKind, sourceName string) error {
	s.enqueueScheduledCalls++
	s.enqueueLane = lane
	s.enqueueKind = connectorKind
	s.enqueueSource = sourceName
	s.scheduledRequests = append(s.scheduledRequests, TriggerRequest{
		ConnectorKind: connectorKind,
		SourceName:    sourceName,
	}.Normalized())
	return s.enqueueScheduledErr
}

func (s *stubSyncJobStore) RequeueStaleSyncJobsByLane(context.Context, string) (int64, error) {
	panic("RequeueStaleSyncJobsByLane should not be called")
}

func (s *stubSyncJobStore) ClaimNextSyncJobByLane(context.Context, string, string, int64) (syncJobRecord, bool, error) {
	panic("ClaimNextSyncJobByLane should not be called")
}

func (s *stubSyncJobStore) RenewSyncJobLease(context.Context, pgtype.UUID, string, int64) (bool, error) {
	panic("RenewSyncJobLease should not be called")
}

func (s *stubSyncJobStore) MarkSyncJobRunning(context.Context, pgtype.UUID, string) (syncJobRecord, bool, error) {
	panic("MarkSyncJobRunning should not be called")
}

func (s *stubSyncJobStore) CompleteManualSyncJobSuccess(context.Context, pgtype.UUID, string) (bool, error) {
	panic("CompleteManualSyncJobSuccess should not be called")
}

func (s *stubSyncJobStore) CompleteManualSyncJobFailure(context.Context, pgtype.UUID, string, string) (bool, error) {
	panic("CompleteManualSyncJobFailure should not be called")
}

func (s *stubSyncJobStore) CompleteScheduledSyncJobSuccess(context.Context, pgtype.UUID, string) (bool, error) {
	panic("CompleteScheduledSyncJobSuccess should not be called")
}

func (s *stubSyncJobStore) CompleteScheduledSyncJobFailure(context.Context, pgtype.UUID, string, string, time.Time) (bool, error) {
	panic("CompleteScheduledSyncJobFailure should not be called")
}

func TestResyncQueueRunner_QueuesForcedScopedWork(t *testing.T) {
	t.Parallel()

	store := &stubSyncJobStore{}
	runner := NewResyncQueueRunner(store, registry.RunModeFull)

	err := runner.RunOnce(WithConnectorScope(context.Background(), " GitHub ", " Acme "))
	if !errors.Is(err, ErrSyncQueued) {
		t.Fatalf("RunOnce() err = %v, want ErrSyncQueued", err)
	}
	if store.enqueueManualCalls != 1 {
		t.Fatalf("manual enqueue calls = %d, want 1", store.enqueueManualCalls)
	}
	if store.enqueueLane != syncJobLaneFull || store.enqueueKind != "github" || store.enqueueSource != "Acme" {
		t.Fatalf("stored job = lane=%q kind=%q source=%q", store.enqueueLane, store.enqueueKind, store.enqueueSource)
	}
}

func TestResyncQueueRunner_DuplicateActiveJobReturnsBusy(t *testing.T) {
	t.Parallel()

	store := &stubSyncJobStore{enqueueManualErr: errActiveSyncJobExists}
	runner := NewResyncQueueRunner(store, registry.RunModeFull)

	err := runner.RunOnce(context.Background())
	if !errors.Is(err, ErrSyncAlreadyRunning) {
		t.Fatalf("RunOnce() err = %v, want ErrSyncAlreadyRunning", err)
	}
}

func TestResyncQueueRunner_DuplicatePendingJobReturnsQueued(t *testing.T) {
	t.Parallel()

	store := &stubSyncJobStore{enqueueManualErr: errPendingSyncJobExists}
	runner := NewResyncQueueRunner(store, registry.RunModeFull)

	err := runner.RunOnce(context.Background())
	if !errors.Is(err, ErrSyncQueued) {
		t.Fatalf("RunOnce() err = %v, want ErrSyncQueued", err)
	}
}

func TestResyncQueueRunner_DiscoveryUnsupportedScopeReturnsNoWork(t *testing.T) {
	t.Parallel()

	store := &stubSyncJobStore{}
	runner := NewResyncQueueRunner(store, registry.RunModeDiscovery)

	err := runner.RunOnce(WithConnectorScope(context.Background(), "github", "acme"))
	if !errors.Is(err, ErrNoConnectorsDue) {
		t.Fatalf("RunOnce() err = %v, want ErrNoConnectorsDue", err)
	}
	if store.enqueueManualCalls != 0 {
		t.Fatalf("manual enqueue calls = %d, want 0", store.enqueueManualCalls)
	}
}

func TestResyncQueueRunner_PropagatesEnqueueErrors(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("enqueue failed")
	store := &stubSyncJobStore{enqueueManualErr: sentinel}
	runner := NewResyncQueueRunner(store, registry.RunModeFull)

	err := runner.RunOnce(context.Background())
	if !errors.Is(err, sentinel) {
		t.Fatalf("RunOnce() err = %v, want sentinel", err)
	}
	if store.enqueueManualCalls != 1 {
		t.Fatalf("manual enqueue calls = %d, want 1", store.enqueueManualCalls)
	}
}

func TestResyncQueueRunner_PlannerBlocksNoWorkWithoutEnqueue(t *testing.T) {
	t.Parallel()

	store := &stubSyncJobStore{}
	planner := &stubManualPlanner{err: ErrNoConnectorsDue}
	runner := NewResyncQueueRunnerWithPlanner(store, planner, registry.RunModeDiscovery)

	err := runner.RunOnce(WithConnectorScope(context.Background(), "okta", "Acme"))
	if !errors.Is(err, ErrNoConnectorsDue) {
		t.Fatalf("RunOnce() err = %v, want ErrNoConnectorsDue", err)
	}
	if planner.calls != 1 {
		t.Fatalf("planner calls = %d, want 1", planner.calls)
	}
	if !planner.forced {
		t.Fatalf("expected planner context to be forced")
	}
	if planner.scope.ConnectorKind != "okta" || planner.scope.SourceName != "Acme" {
		t.Fatalf("planner scope = %+v", planner.scope)
	}
	if store.enqueueManualCalls != 0 {
		t.Fatalf("manual enqueue calls = %d, want 0", store.enqueueManualCalls)
	}
}

func TestResyncQueueRunner_UnscopedRequestsSkipPlannerPrecheck(t *testing.T) {
	t.Parallel()

	store := &stubSyncJobStore{}
	planner := &stubManualPlanner{err: errors.New("plan failed")}
	runner := NewResyncQueueRunnerWithPlanner(store, planner, registry.RunModeFull)

	err := runner.RunOnce(context.Background())
	if !errors.Is(err, ErrSyncQueued) {
		t.Fatalf("RunOnce() err = %v, want ErrSyncQueued", err)
	}
	if planner.calls != 0 {
		t.Fatalf("planner calls = %d, want 0", planner.calls)
	}
	if store.enqueueManualCalls != 1 {
		t.Fatalf("manual enqueue calls = %d, want 1", store.enqueueManualCalls)
	}
}

func TestResyncQueueRunner_DiscoverySupportsGoogleWorkspaceScope(t *testing.T) {
	t.Parallel()

	store := &stubSyncJobStore{}
	runner := NewResyncQueueRunner(store, registry.RunModeDiscovery)

	err := runner.RunOnce(WithConnectorScope(context.Background(), "google_workspace", "C0123"))
	if !errors.Is(err, ErrSyncQueued) {
		t.Fatalf("RunOnce() err = %v, want ErrSyncQueued", err)
	}
	if store.enqueueManualCalls != 1 {
		t.Fatalf("manual enqueue calls = %d, want 1", store.enqueueManualCalls)
	}
	if store.enqueueLane != syncJobLaneDiscovery || store.enqueueKind != "google_workspace" || store.enqueueSource != "C0123" {
		t.Fatalf("stored job = lane=%q kind=%q source=%q", store.enqueueLane, store.enqueueKind, store.enqueueSource)
	}
}

func TestScheduledQueueRunner_EnqueuesLaneJob(t *testing.T) {
	t.Parallel()

	planner := &stubScheduledPlanner{
		requests: []TriggerRequest{
			{ConnectorKind: "entra", SourceName: "Tenant A"},
			{ConnectorKind: "okta", SourceName: "Org B"},
		},
	}
	store := &stubSyncJobStore{}
	runner := NewScheduledQueueRunner(store, planner, registry.RunModeDiscovery)

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() err = %v, want nil", err)
	}
	if store.enqueueScheduledCalls != 2 {
		t.Fatalf("scheduled enqueue calls = %d, want 2", store.enqueueScheduledCalls)
	}
	if got, want := store.enqueueLane, syncJobLaneDiscovery; got != want {
		t.Fatalf("last scheduled enqueue lane = %q, want %q", got, want)
	}
	if got, want := planner.calls, 1; got != want {
		t.Fatalf("planner calls = %d, want %d", got, want)
	}
	if got := store.scheduledRequests; len(got) != 2 || got[0].ConnectorKind != "entra" || got[0].SourceName != "Tenant A" || got[1].ConnectorKind != "okta" || got[1].SourceName != "Org B" {
		t.Fatalf("scheduled requests = %#v", got)
	}
}

type stubScheduledPlanner struct {
	requests []TriggerRequest
	err      error
	calls    int
}

func (p *stubScheduledPlanner) PlannedConnectorScopes(context.Context) ([]TriggerRequest, error) {
	p.calls++
	if p.err != nil {
		return nil, p.err
	}
	out := make([]TriggerRequest, len(p.requests))
	copy(out, p.requests)
	return out, nil
}

package sync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

type capturingRunner struct {
	err      error
	calls    int
	forced   bool
	scope    TriggerRequest
	resource string
}

func (r *capturingRunner) RunOnce(ctx context.Context) error {
	r.calls++
	r.forced = IsForcedSync(ctx)
	if kind, name, ok := ConnectorScopeFromContext(ctx); ok {
		r.scope = TriggerRequest{ConnectorKind: kind, SourceName: name}
	}
	if resource, ok := ResourceScopeFromContext(ctx); ok {
		r.resource = resource
	}
	return r.err
}

type cancelAwareRunner struct {
	started chan struct{}
}

func (r *cancelAwareRunner) RunOnce(ctx context.Context) error {
	if r.started != nil {
		close(r.started)
	}
	<-ctx.Done()
	return ctx.Err()
}

type consumerClaimResult struct {
	job syncJobRecord
	ok  bool
	err error
}

type noopLockManager struct{}

func (noopLockManager) TryAcquire(context.Context, string, string) (Lock, bool, error) {
	panic("TryAcquire should not be called")
}

func (noopLockManager) Acquire(context.Context, string, string) (Lock, error) {
	panic("Acquire should not be called")
}

type consumerStoreStub struct {
	requeueCalls []string
	claimCalls   []string

	claimResults []consumerClaimResult
	runningJob   syncJobRecord
	runningOK    bool
	runningErr   error

	manualSuccessCalls []pgtype.UUID
	manualFailureCalls []string
	schedSuccessCalls  []pgtype.UUID
	schedFailureCalls  []scheduledFailureCall

	renewStarted chan struct{}
	renewCtxErr  chan error

	manualSuccessOK bool
	manualFailureOK bool
	schedSuccessOK  bool
	schedFailureOK  bool
}

type scheduledFailureCall struct {
	lastError       string
	nextAvailableAt time.Time
}

func (s *consumerStoreStub) EnqueueManualSyncJob(context.Context, string, string, string) error {
	panic("EnqueueManualSyncJob should not be called")
}

func (s *consumerStoreStub) EnqueueScheduledSyncJob(context.Context, string, string, string) error {
	panic("EnqueueScheduledSyncJob should not be called")
}

func (s *consumerStoreStub) RequeueStaleSyncJobsByLane(_ context.Context, lane string) (int64, error) {
	s.requeueCalls = append(s.requeueCalls, lane)
	return 1, nil
}

func (s *consumerStoreStub) ClaimNextSyncJobByLane(_ context.Context, lane, _ string, _ int64) (syncJobRecord, bool, error) {
	s.claimCalls = append(s.claimCalls, lane)
	if len(s.claimResults) == 0 {
		return syncJobRecord{}, false, nil
	}
	next := s.claimResults[0]
	s.claimResults = s.claimResults[1:]
	return next.job, next.ok, next.err
}

func (s *consumerStoreStub) RenewSyncJobLease(ctx context.Context, _ pgtype.UUID, _ string, _ int64) (bool, error) {
	if s.renewStarted != nil {
		select {
		case s.renewStarted <- struct{}{}:
		default:
		}
	}
	if s.renewCtxErr != nil {
		<-ctx.Done()
		s.renewCtxErr <- ctx.Err()
		return false, ctx.Err()
	}
	return true, nil
}

func (s *consumerStoreStub) MarkSyncJobRunning(_ context.Context, _ pgtype.UUID, _ string) (syncJobRecord, bool, error) {
	return s.runningJob, s.runningOK, s.runningErr
}

func (s *consumerStoreStub) CompleteManualSyncJobSuccess(_ context.Context, jobID pgtype.UUID, _ string) (bool, error) {
	s.manualSuccessCalls = append(s.manualSuccessCalls, jobID)
	return s.manualSuccessOK, nil
}

func (s *consumerStoreStub) CompleteManualSyncJobFailure(_ context.Context, _ pgtype.UUID, _ string, lastError string) (bool, error) {
	s.manualFailureCalls = append(s.manualFailureCalls, lastError)
	return s.manualFailureOK, nil
}

func (s *consumerStoreStub) CompleteScheduledSyncJobSuccess(_ context.Context, jobID pgtype.UUID, _ string) (bool, error) {
	s.schedSuccessCalls = append(s.schedSuccessCalls, jobID)
	return s.schedSuccessOK, nil
}

func (s *consumerStoreStub) CompleteScheduledSyncJobFailure(_ context.Context, _ pgtype.UUID, _ string, lastError string, nextAvailableAt time.Time) (bool, error) {
	s.schedFailureCalls = append(s.schedFailureCalls, scheduledFailureCall{
		lastError:       lastError,
		nextAvailableAt: nextAvailableAt,
	})
	return s.schedFailureOK, nil
}

func TestSyncJobConsumer_ProcessManualJobMarksSuccessWithForcedScope(t *testing.T) {
	t.Parallel()

	jobID := pgUUID(uuid.New())
	store := &consumerStoreStub{
		runningJob: syncJobRecord{
			ID:            jobID,
			Lane:          syncJobLaneFull,
			TriggerKind:   syncJobTriggerKindManual,
			ConnectorKind: "github",
			SourceName:    "Acme",
		},
		runningOK:       true,
		manualSuccessOK: true,
	}
	runner := &capturingRunner{}
	consumer := NewSyncJobConsumer(store, noopLockManager{}, runner, SyncJobConsumerConfig{
		Mode:              registry.RunModeFull,
		HeartbeatInterval: time.Hour,
		LeaseTTL:          time.Minute,
		ClaimedBy:         "claimant",
	})
	job := syncJobRecord{ID: jobID, Lane: syncJobLaneFull}

	err := consumer.processJob(context.Background(), job)
	if err != nil {
		t.Fatalf("processJob() err = %v, want nil", err)
	}
	if runner.calls != 1 {
		t.Fatalf("runner calls = %d, want 1", runner.calls)
	}
	if !runner.forced {
		t.Fatalf("expected forced execution context")
	}
	if runner.scope.ConnectorKind != "github" || runner.scope.SourceName != "Acme" {
		t.Fatalf("runner scope = %+v", runner.scope)
	}
	if len(store.manualSuccessCalls) != 1 || store.manualSuccessCalls[0] != jobID {
		t.Fatalf("manual success calls = %#v", store.manualSuccessCalls)
	}
}

func TestSyncJobConsumer_ProcessManualJobMarksFailure(t *testing.T) {
	t.Parallel()

	runErr := errors.New("sync failed")
	store := &consumerStoreStub{
		runningJob: syncJobRecord{
			ID:          pgUUID(uuid.New()),
			Lane:        syncJobLaneDiscovery,
			TriggerKind: syncJobTriggerKindManual,
		},
		runningOK:       true,
		manualFailureOK: true,
	}
	runner := &capturingRunner{err: runErr}
	consumer := NewSyncJobConsumer(store, noopLockManager{}, runner, SyncJobConsumerConfig{
		Mode:              registry.RunModeDiscovery,
		HeartbeatInterval: time.Hour,
		LeaseTTL:          time.Minute,
		ClaimedBy:         "claimant",
	})
	job := syncJobRecord{ID: pgUUID(uuid.New()), Lane: syncJobLaneDiscovery}

	err := consumer.processJob(context.Background(), job)
	if !errors.Is(err, runErr) {
		t.Fatalf("processJob() err = %v, want runErr", err)
	}
	if len(store.manualFailureCalls) != 1 || store.manualFailureCalls[0] != runErr.Error() {
		t.Fatalf("manual failure calls = %#v", store.manualFailureCalls)
	}
}

func TestSyncJobConsumer_ProcessManualJobTreatsNoWorkAsSuccess(t *testing.T) {
	t.Parallel()

	jobID := pgUUID(uuid.New())
	store := &consumerStoreStub{
		runningJob: syncJobRecord{
			ID:          jobID,
			Lane:        syncJobLaneDiscovery,
			TriggerKind: syncJobTriggerKindManual,
		},
		runningOK:       true,
		manualSuccessOK: true,
		manualFailureOK: true,
	}
	runner := &capturingRunner{err: ErrNoConnectorsDue}
	consumer := NewSyncJobConsumer(store, noopLockManager{}, runner, SyncJobConsumerConfig{
		Mode:              registry.RunModeDiscovery,
		HeartbeatInterval: time.Hour,
		LeaseTTL:          time.Minute,
		ClaimedBy:         "claimant",
	})

	err := consumer.processJob(context.Background(), syncJobRecord{ID: jobID, Lane: syncJobLaneDiscovery})
	if err != nil {
		t.Fatalf("processJob() err = %v, want nil", err)
	}
	if len(store.manualSuccessCalls) != 1 || store.manualSuccessCalls[0] != jobID {
		t.Fatalf("manual success calls = %#v", store.manualSuccessCalls)
	}
	if len(store.manualFailureCalls) != 0 {
		t.Fatalf("manual failure calls = %#v, want none", store.manualFailureCalls)
	}
}

func TestSyncJobConsumer_ProcessScheduledJobSkipsWithoutForcedMode(t *testing.T) {
	t.Parallel()

	jobID := pgUUID(uuid.New())
	store := &consumerStoreStub{
		runningJob: syncJobRecord{
			ID:          jobID,
			Lane:        syncJobLaneFull,
			TriggerKind: syncJobTriggerKindScheduled,
		},
		runningOK:      true,
		schedSuccessOK: true,
	}
	runner := &capturingRunner{err: ErrNoConnectorsDue}
	consumer := NewSyncJobConsumer(store, noopLockManager{}, runner, SyncJobConsumerConfig{
		Mode:              registry.RunModeFull,
		HeartbeatInterval: time.Hour,
		LeaseTTL:          time.Minute,
		ClaimedBy:         "claimant",
	})
	job := syncJobRecord{ID: jobID, Lane: syncJobLaneFull}

	err := consumer.processJob(context.Background(), job)
	if err != nil {
		t.Fatalf("processJob() err = %v, want nil", err)
	}
	if runner.forced {
		t.Fatalf("scheduled execution should not force sync")
	}
	if len(store.schedSuccessCalls) != 1 || store.schedSuccessCalls[0] != jobID {
		t.Fatalf("scheduled success calls = %#v", store.schedSuccessCalls)
	}
}

func TestSyncJobConsumer_ProcessTailJobPassesForcedResourceScope(t *testing.T) {
	t.Parallel()

	jobID := pgUUID(uuid.New())
	store := &consumerStoreStub{
		runningJob: syncJobRecord{
			ID:            jobID,
			Lane:          syncJobLaneTail,
			TriggerKind:   syncJobTriggerKindScheduled,
			ConnectorKind: "okta",
			SourceName:    "dev-123.okta.com",
			Resource:      "system_log",
		},
		runningOK:      true,
		schedSuccessOK: true,
	}
	runner := &capturingRunner{}
	consumer := NewSyncJobConsumer(store, noopLockManager{}, runner, SyncJobConsumerConfig{
		Mode:              registry.RunModeTail,
		HeartbeatInterval: time.Hour,
		LeaseTTL:          time.Minute,
		ClaimedBy:         "claimant",
	})

	err := consumer.processJob(context.Background(), syncJobRecord{ID: jobID, Lane: syncJobLaneTail})
	if err != nil {
		t.Fatalf("processJob() err = %v, want nil", err)
	}
	if !runner.forced {
		t.Fatalf("expected tail execution to force sync")
	}
	if runner.scope.ConnectorKind != "okta" || runner.scope.SourceName != "dev-123.okta.com" {
		t.Fatalf("runner scope = %+v", runner.scope)
	}
	if runner.resource != "system_log" {
		t.Fatalf("runner resource = %q, want system_log", runner.resource)
	}
	if len(store.schedSuccessCalls) != 1 || store.schedSuccessCalls[0] != jobID {
		t.Fatalf("scheduled success calls = %#v", store.schedSuccessCalls)
	}
}

func TestSyncJobConsumer_ProcessScheduledJobTreatsNoEnabledConnectorsAsSuccess(t *testing.T) {
	t.Parallel()

	jobID := pgUUID(uuid.New())
	store := &consumerStoreStub{
		runningJob: syncJobRecord{
			ID:          jobID,
			Lane:        syncJobLaneFull,
			TriggerKind: syncJobTriggerKindScheduled,
		},
		runningOK:      true,
		schedSuccessOK: true,
	}
	runner := &capturingRunner{err: ErrNoEnabledConnectors}
	consumer := NewSyncJobConsumer(store, noopLockManager{}, runner, SyncJobConsumerConfig{
		Mode:              registry.RunModeFull,
		HeartbeatInterval: time.Hour,
		LeaseTTL:          time.Minute,
		ClaimedBy:         "claimant",
	})

	err := consumer.processJob(context.Background(), syncJobRecord{ID: jobID, Lane: syncJobLaneFull})
	if err != nil {
		t.Fatalf("processJob() err = %v, want nil", err)
	}
	if len(store.schedSuccessCalls) != 1 || store.schedSuccessCalls[0] != jobID {
		t.Fatalf("scheduled success calls = %#v", store.schedSuccessCalls)
	}
}

func TestSyncJobConsumer_ProcessScheduledJobRequeuesFailureWithBackoff(t *testing.T) {
	t.Parallel()

	runErr := errors.New("scheduled failed")
	store := &consumerStoreStub{
		runningJob: syncJobRecord{
			ID:           pgUUID(uuid.New()),
			Lane:         syncJobLaneFull,
			TriggerKind:  syncJobTriggerKindScheduled,
			AttemptCount: 3,
		},
		runningOK:      true,
		schedFailureOK: true,
	}
	runner := &capturingRunner{err: runErr}
	consumer := NewSyncJobConsumer(store, noopLockManager{}, runner, SyncJobConsumerConfig{
		Mode:              registry.RunModeFull,
		HeartbeatInterval: time.Hour,
		LeaseTTL:          time.Minute,
		ClaimedBy:         "claimant",
		RetryBaseDelay:    time.Second,
		RetryMaxDelay:     10 * time.Second,
	})
	job := syncJobRecord{ID: pgUUID(uuid.New()), Lane: syncJobLaneFull}

	before := time.Now()
	err := consumer.processJob(context.Background(), job)
	after := time.Now()
	if !errors.Is(err, runErr) {
		t.Fatalf("processJob() err = %v, want runErr", err)
	}
	if len(store.schedFailureCalls) != 1 {
		t.Fatalf("scheduled failure calls = %#v", store.schedFailureCalls)
	}
	got := store.schedFailureCalls[0]
	if got.lastError != runErr.Error() {
		t.Fatalf("scheduled failure lastError = %q, want %q", got.lastError, runErr.Error())
	}
	minAvailable := before.Add(4 * time.Second)
	maxAvailable := after.Add(4 * time.Second)
	if got.nextAvailableAt.Before(minAvailable) || got.nextAvailableAt.After(maxAvailable) {
		t.Fatalf("nextAvailableAt = %v, want about 4s after now", got.nextAvailableAt)
	}
}

func TestSyncJobConsumer_ProcessJobLeavesCanceledJobRecoverable(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := &consumerStoreStub{
		runningJob: syncJobRecord{
			ID:          pgUUID(uuid.New()),
			Lane:        syncJobLaneFull,
			TriggerKind: syncJobTriggerKindManual,
		},
		runningOK:       true,
		manualSuccessOK: true,
		manualFailureOK: true,
	}
	runner := &cancelAwareRunner{started: make(chan struct{})}
	consumer := NewSyncJobConsumer(store, noopLockManager{}, runner, SyncJobConsumerConfig{
		Mode:              registry.RunModeFull,
		HeartbeatInterval: time.Hour,
		LeaseTTL:          time.Minute,
		ClaimedBy:         "claimant",
	})
	job := syncJobRecord{ID: pgUUID(uuid.New()), Lane: syncJobLaneFull}

	done := make(chan error, 1)
	go func() {
		done <- consumer.processJob(ctx, job)
	}()

	<-runner.started
	cancel()

	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("processJob() err = %v, want context.Canceled", err)
	}
	if len(store.manualSuccessCalls) != 0 {
		t.Fatalf("manual success calls = %d, want 0", len(store.manualSuccessCalls))
	}
	if len(store.manualFailureCalls) != 0 {
		t.Fatalf("manual failure calls = %#v, want none", store.manualFailureCalls)
	}
}

func TestSyncJobConsumer_HeartbeatRenewalCancelsWithParent(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := &consumerStoreStub{
		renewStarted: make(chan struct{}, 1),
		renewCtxErr:  make(chan error, 1),
	}
	consumer := NewSyncJobConsumer(store, noopLockManager{}, &capturingRunner{}, SyncJobConsumerConfig{
		Mode:              registry.RunModeFull,
		HeartbeatInterval: 10 * time.Millisecond,
		LeaseTTL:          time.Minute,
		ClaimedBy:         "claimant",
	})
	lostCh := make(chan error, 1)
	stopHeartbeat := consumer.startHeartbeat(ctx, syncJobRecord{ID: pgUUID(uuid.New())}, func(err error) {
		lostCh <- err
	})
	defer stopHeartbeat()

	select {
	case <-store.renewStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for heartbeat renewal")
	}

	cancel()

	select {
	case err := <-store.renewCtxErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("renewal context error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for renewal context cancellation")
	}

	select {
	case err := <-lostCh:
		t.Fatalf("heartbeat reported canceled renewal as lease loss: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestSyncJobConsumer_ConsumeLoopUsesLaneSpecificClaims(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	store := &consumerStoreStub{
		claimResults: []consumerClaimResult{{ok: false}},
	}
	consumer := NewSyncJobConsumer(store, noopLockManager{}, &capturingRunner{}, SyncJobConsumerConfig{
		Mode:         registry.RunModeFull,
		PollInterval: time.Millisecond,
		LeaseTTL:     time.Minute,
		ClaimedBy:    "claimant",
	})

	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	if err := consumer.consumeLoop(ctx); err != nil {
		t.Fatalf("consumeLoop() err = %v", err)
	}
	if len(store.requeueCalls) == 0 || store.requeueCalls[0] != syncJobLaneFull {
		t.Fatalf("requeue calls = %#v", store.requeueCalls)
	}
	if len(store.claimCalls) == 0 || store.claimCalls[0] != syncJobLaneFull {
		t.Fatalf("claim calls = %#v", store.claimCalls)
	}
}

func TestSyncJobConsumer_NextRetryDelayCapsAtMax(t *testing.T) {
	t.Parallel()

	consumer := NewSyncJobConsumer(&consumerStoreStub{}, noopLockManager{}, &capturingRunner{}, SyncJobConsumerConfig{
		Mode:           registry.RunModeFull,
		RetryBaseDelay: time.Second,
		RetryMaxDelay:  5 * time.Second,
	})
	if got := consumer.nextRetryDelay(10); got != 5*time.Second {
		t.Fatalf("nextRetryDelay(10) = %v, want 5s", got)
	}
}

func TestSyncJobConsumer_WaitForWorkWakesOnSignal(t *testing.T) {
	t.Parallel()

	wakeups := make(chan struct{}, 1)
	consumer := NewSyncJobConsumer(&consumerStoreStub{}, noopLockManager{}, &capturingRunner{}, SyncJobConsumerConfig{
		Mode:         registry.RunModeFull,
		PollInterval: time.Hour,
		Wakeups:      wakeups,
	})

	done := make(chan bool, 1)
	go func() {
		done <- consumer.waitForWork(context.Background())
	}()

	time.Sleep(10 * time.Millisecond)
	wakeups <- struct{}{}

	select {
	case woke := <-done:
		if !woke {
			t.Fatalf("waitForWork() = false, want true")
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for wakeup")
	}
}

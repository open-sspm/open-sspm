package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWorkerHostRunCriticalGoroutineFailureIsObservable(t *testing.T) {
	t.Parallel()

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	run := newWorkerHostRun(runCtx, cancel, "test-lane")
	wantErr := errors.New("boom")

	run.Go("consumer", true, func(context.Context) error {
		return wantErr
	})

	err := run.Wait()
	if !errors.Is(err, wantErr) {
		t.Fatalf("Wait() err = %v, want %v", err, wantErr)
	}
}

func TestWorkerHostRunExternalShutdownSuppressesLateComponentError(t *testing.T) {
	t.Parallel()

	runCtx, cancel := context.WithCancel(context.Background())
	run := newWorkerHostRun(runCtx, cancel, "test-lane")
	ready := make(chan struct{})

	run.Go("consumer", true, func(ctx context.Context) error {
		close(ready)
		<-ctx.Done()
		return errors.New("late failure")
	})
	<-ready
	cancel()

	if err := run.Wait(); err != nil {
		t.Fatalf("Wait() err = %v, want nil after external shutdown", err)
	}
}

func TestWorkerHostRunCleanupRunsInReverseOrder(t *testing.T) {
	t.Parallel()

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	run := newWorkerHostRun(runCtx, cancel, "test-lane")
	var order []string

	run.OnShutdown("first", func(context.Context) error {
		order = append(order, "first")
		return nil
	})
	run.OnShutdown("second", func(context.Context) error {
		order = append(order, "second")
		return nil
	})

	if err := run.Cleanup(); err != nil {
		t.Fatalf("Cleanup() err = %v", err)
	}
	if got, want := order, []string{"second", "first"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("cleanup order = %v, want %v", got, want)
	}
}

func TestWorkerHostRunCleanupUsesFreshTimeoutPerHandler(t *testing.T) {
	t.Parallel()

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	run := newWorkerHostRun(runCtx, cancel, "test-lane")
	var firstDeadline, secondDeadline time.Time

	run.OnShutdown("first", func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("first cleanup context has no deadline")
		}
		firstDeadline = deadline
		return nil
	})
	run.OnShutdown("second", func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("second cleanup context has no deadline")
		}
		secondDeadline = deadline
		time.Sleep(20 * time.Millisecond)
		return nil
	})

	if err := run.Cleanup(); err != nil {
		t.Fatalf("Cleanup() err = %v", err)
	}
	if !firstDeadline.After(secondDeadline) {
		t.Fatalf("first cleanup deadline = %v, want after second cleanup deadline %v", firstDeadline, secondDeadline)
	}
}

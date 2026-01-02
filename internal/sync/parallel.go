package sync

import (
	"context"
	"errors"
	gosync "sync"
	"sync/atomic"
)

// ParallelResult holds the result of a parallel operation.
type ParallelResult[R any] struct {
	Value R
	Err   error
}

// ParallelCollect processes items in parallel with the specified number of workers,
// collecting results. It cancels remaining work on the first error and returns
// the first non-context error.
//
// The onProgress callback is called after each successful item is processed.
func ParallelCollect[T any, R any](
	ctx context.Context,
	items []T,
	workers int,
	process func(ctx context.Context, item T) (R, error),
	onProgress func(done int64, total int64),
) ([]ParallelResult[R], error) {
	if len(items) == 0 {
		return nil, nil
	}

	workers = normalizeWorkers(workers, len(items))
	total := int64(len(items))

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan T, len(items))
	results := make(chan ParallelResult[R], len(items))
	var done int64

	var wg gosync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range jobs {
				if workerCtx.Err() != nil {
					return
				}
				value, err := process(workerCtx, item)
				if err != nil {
					results <- ParallelResult[R]{Err: err}
					cancel()
					continue
				}
				n := atomic.AddInt64(&done, 1)
				if onProgress != nil {
					onProgress(n, total)
				}
				results <- ParallelResult[R]{Value: value}
			}
		}()
	}

	for _, item := range items {
		jobs <- item
	}
	close(jobs)
	wg.Wait()
	close(results)

	out := make([]ParallelResult[R], 0, len(items))
	var firstErr error
	var firstNonCancelErr error
	for res := range results {
		out = append(out, res)
		if res.Err != nil {
			if firstErr == nil {
				firstErr = res.Err
			}
			if firstNonCancelErr == nil && !errors.Is(res.Err, context.Canceled) {
				firstNonCancelErr = res.Err
			}
		}
	}

	// Prefer non-cancel errors for reporting
	if firstNonCancelErr != nil {
		return out, firstNonCancelErr
	}
	return out, firstErr
}

// normalizeWorkers ensures worker count is between 1 and item count.
func normalizeWorkers(workers, itemCount int) int {
	if workers < 1 {
		workers = 1
	}
	if workers > itemCount {
		workers = itemCount
	}
	return workers
}

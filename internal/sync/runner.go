package sync

import (
	"context"
	"errors"
)

// Runner executes a single sync pass.
type Runner interface {
	RunOnce(context.Context) error
}

var ErrNoEnabledConnectors = errors.New("no enabled connectors are configured")

// ErrSyncAlreadyRunning is returned by a try-lock runner when another sync pass
// is already in progress.
var ErrSyncAlreadyRunning = errors.New("sync is already running")

// ErrSyncQueued is returned when a sync request is accepted but will be processed
// asynchronously by the worker.
var ErrSyncQueued = errors.New("sync queued")

// ErrNoConnectorsDue is returned when all enabled connectors are deferred by a
// run policy (interval/backoff) and there is no work to do.
var ErrNoConnectorsDue = errors.New("no connectors are due to sync")

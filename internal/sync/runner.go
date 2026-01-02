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

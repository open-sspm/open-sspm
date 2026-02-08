package sync

import (
	"context"
	"errors"
)

type compositeRunner struct {
	runners []Runner
}

func NewCompositeRunner(runners ...Runner) Runner {
	filtered := make([]Runner, 0, len(runners))
	for _, runner := range runners {
		if runner != nil {
			filtered = append(filtered, runner)
		}
	}
	return &compositeRunner{runners: filtered}
}

func (r *compositeRunner) RunOnce(ctx context.Context) error {
	if r == nil || len(r.runners) == 0 {
		return ErrNoEnabledConnectors
	}

	var (
		hardErrs       []error
		anySuccess     bool
		anyQueued      bool
		busyCount      int
		disabledOrIdle int
	)

	for _, runner := range r.runners {
		err := runner.RunOnce(ctx)
		switch {
		case err == nil:
			anySuccess = true
		case errors.Is(err, ErrSyncQueued):
			anyQueued = true
		case errors.Is(err, ErrSyncAlreadyRunning):
			busyCount++
		case isOnlyNoWorkError(err):
			disabledOrIdle++
		default:
			hardErrs = append(hardErrs, err)
		}
	}

	if len(hardErrs) > 0 {
		return errors.Join(hardErrs...)
	}
	if anySuccess {
		return nil
	}
	if anyQueued {
		return ErrSyncQueued
	}
	if busyCount > 0 && busyCount+disabledOrIdle == len(r.runners) {
		return ErrSyncAlreadyRunning
	}
	if disabledOrIdle == len(r.runners) {
		return ErrNoEnabledConnectors
	}
	if busyCount > 0 {
		return ErrSyncAlreadyRunning
	}
	return ErrNoEnabledConnectors
}

func isOnlyNoWorkError(err error) bool {
	if err == nil {
		return false
	}

	matched := false
	var walk func(error) bool
	walk = func(current error) bool {
		if current == nil {
			return true
		}

		children := unwrapErrors(current)
		if len(children) == 0 {
			switch current {
			case ErrNoEnabledConnectors, ErrNoConnectorsDue:
				matched = true
				return true
			default:
				return false
			}
		}

		for _, child := range children {
			if !walk(child) {
				return false
			}
		}
		return true
	}

	return walk(err) && matched
}

func unwrapErrors(err error) []error {
	if err == nil {
		return nil
	}

	type multiUnwrapper interface {
		Unwrap() []error
	}
	if unwrapped, ok := err.(multiUnwrapper); ok {
		return unwrapped.Unwrap()
	}

	single := errors.Unwrap(err)
	if single == nil {
		return nil
	}
	return []error{single}
}

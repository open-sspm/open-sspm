package recorddispatch

import (
	"context"
	"errors"
	"fmt"
	"strings"

	canonevents "github.com/open-sspm/open-sspm/internal/events"
	"github.com/open-sspm/open-sspm/internal/records"
)

type EventWriter interface {
	WriteEvent(context.Context, records.EventRecord, canonevents.WriteOptions) (canonevents.WriteResult, error)
}

type StateProjector interface {
	UpsertState(context.Context, records.StateUpsert) error
	DeleteState(context.Context, records.StateDelete) error
	BeginSnapshot(context.Context, records.SnapshotBegin) error
	CompleteSnapshot(context.Context, records.SnapshotComplete) error
}

type Dispatcher struct {
	eventWriter EventWriter
	projector   StateProjector
}

func NewDispatcher(eventWriter EventWriter, projector StateProjector) *Dispatcher {
	return &Dispatcher{
		eventWriter: eventWriter,
		projector:   projector,
	}
}

func NewEventDispatcher(db canonevents.Beginner) *Dispatcher {
	return NewDispatcher(canonevents.NewWriter(db), nil)
}

func (d *Dispatcher) EmitEvent(ctx context.Context, record records.EventRecord) error {
	_, err := d.DispatchEvent(ctx, record)
	return err
}

func (d *Dispatcher) EmitInternalEvent(ctx context.Context, record records.EventRecord) error {
	return d.EmitEvent(ctx, record)
}

func (d *Dispatcher) DispatchEvent(ctx context.Context, record records.EventRecord) (canonevents.WriteResult, error) {
	if d == nil || d.eventWriter == nil {
		return canonevents.WriteResult{}, errors.New("record dispatcher event writer is not configured")
	}
	if err := validateInbound(record); err != nil {
		return canonevents.WriteResult{}, err
	}
	result, err := d.eventWriter.WriteEvent(ctx, record, canonevents.WriteOptions{})
	if err != nil {
		return canonevents.WriteResult{}, fmt.Errorf("dispatch event record: %w", err)
	}
	return result, nil
}

func (d *Dispatcher) UpsertState(ctx context.Context, record records.StateUpsert) error {
	if d == nil || d.projector == nil {
		return errors.New("record dispatcher state projector is not configured")
	}
	if err := validateInbound(record); err != nil {
		return err
	}
	if err := record.Validate(); err != nil {
		return err
	}
	return d.projector.UpsertState(ctx, record)
}

func (d *Dispatcher) DeleteState(ctx context.Context, record records.StateDelete) error {
	if d == nil || d.projector == nil {
		return errors.New("record dispatcher state projector is not configured")
	}
	if err := validateInbound(record); err != nil {
		return err
	}
	if strings.TrimSpace(string(record.Resource)) == "" {
		return errors.New("state delete resource is required")
	}
	if strings.TrimSpace(record.Key) == "" && strings.TrimSpace(record.ProviderID) == "" {
		return errors.New("state delete key or provider id is required")
	}
	if strings.TrimSpace(string(record.Reason)) == "" {
		return errors.New("state delete reason is required")
	}
	return d.projector.DeleteState(ctx, record)
}

func (d *Dispatcher) BeginSnapshot(ctx context.Context, record records.SnapshotBegin) error {
	if d == nil || d.projector == nil {
		return errors.New("record dispatcher state projector is not configured")
	}
	if err := validateInbound(record); err != nil {
		return err
	}
	if err := validateSnapshot(record.Resource, record.Scope); err != nil {
		return err
	}
	return d.projector.BeginSnapshot(ctx, record)
}

func (d *Dispatcher) CompleteSnapshot(ctx context.Context, record records.SnapshotComplete) error {
	if d == nil || d.projector == nil {
		return errors.New("record dispatcher state projector is not configured")
	}
	if err := validateInbound(record); err != nil {
		return err
	}
	if err := validateSnapshot(record.Resource, record.Scope); err != nil {
		return err
	}
	return d.projector.CompleteSnapshot(ctx, record)
}

func validateInbound(record records.InboundRecord) error {
	if record == nil {
		return errors.New("record is required")
	}
	if strings.TrimSpace(record.SourceKind()) == "" {
		return errors.New("record source kind is required")
	}
	if strings.TrimSpace(record.SourceName()) == "" {
		return errors.New("record source name is required")
	}
	if strings.TrimSpace(record.DedupeKey()) == "" {
		return errors.New("record dedupe key is required")
	}
	return nil
}

func validateSnapshot(resource records.ResourceName, scope records.FullScope) error {
	if strings.TrimSpace(string(resource)) == "" {
		return errors.New("snapshot resource is required")
	}
	if strings.TrimSpace(string(scope.Resource)) == "" {
		return errors.New("snapshot scope resource is required")
	}
	if scope.Resource != resource {
		return errors.New("snapshot scope resource does not match record resource")
	}
	return nil
}

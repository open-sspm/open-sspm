package recorddispatch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	canonevents "github.com/open-sspm/open-sspm/internal/events"
	"github.com/open-sspm/open-sspm/internal/records"
)

type fakeEventWriter struct {
	records []records.EventRecord
	err     error
}

func (w *fakeEventWriter) WriteEvent(_ context.Context, record records.EventRecord, _ canonevents.WriteOptions) (canonevents.WriteResult, error) {
	if w.err != nil {
		return canonevents.WriteResult{}, w.err
	}
	w.records = append(w.records, record)
	return canonevents.WriteResult{EventID: uuid.Must(uuid.NewV7()), ReceivedAt: time.Now(), Inserted: true}, nil
}

type fakeProjector struct {
	upserts        int
	deletes        int
	snapshotBegins int
	snapshotEnds   int
}

func (p *fakeProjector) UpsertState(context.Context, records.StateUpsert) error {
	p.upserts++
	return nil
}

func (p *fakeProjector) DeleteState(context.Context, records.StateDelete) error {
	p.deletes++
	return nil
}

func (p *fakeProjector) BeginSnapshot(context.Context, records.SnapshotBegin) error {
	p.snapshotBegins++
	return nil
}

func (p *fakeProjector) CompleteSnapshot(context.Context, records.SnapshotComplete) error {
	p.snapshotEnds++
	return nil
}

func TestDispatcherDispatchEventValidatesAndWrites(t *testing.T) {
	writer := &fakeEventWriter{}
	dispatcher := NewDispatcher(writer, nil)

	_, err := dispatcher.DispatchEvent(context.Background(), records.EventRecord{
		Source:         records.SourceRef{Kind: "Okta", Name: "example.okta.com"},
		Channel:        "system_log",
		DedupeKeyValue: "provider:evt-1",
		EventType:      "user.authentication.sso",
		Category:       "okta.system_log",
		Raw:            map[string]any{"uuid": "evt-1"},
	})
	if err != nil {
		t.Fatalf("DispatchEvent() err = %v", err)
	}
	if len(writer.records) != 1 {
		t.Fatalf("writer records = %d, want 1", len(writer.records))
	}
}

func TestDispatcherRejectsInvalidReplayableRecord(t *testing.T) {
	dispatcher := NewDispatcher(&fakeEventWriter{}, nil)

	_, err := dispatcher.DispatchEvent(context.Background(), records.EventRecord{
		Source:    records.SourceRef{Kind: "okta", Name: "example.okta.com"},
		Channel:   "system_log",
		EventType: "user.authentication.sso",
		Category:  "okta.system_log",
		Raw:       map[string]any{"uuid": "evt-1"},
	})
	if err == nil {
		t.Fatal("DispatchEvent() err = nil, want validation error")
	}
}

func TestDispatcherReturnsWriterErrorBeforeCallerCanAdvanceCursor(t *testing.T) {
	wantErr := errors.New("write failed")
	dispatcher := NewDispatcher(&fakeEventWriter{err: wantErr}, nil)

	_, err := dispatcher.DispatchEvent(context.Background(), records.EventRecord{
		Source:         records.SourceRef{Kind: "okta", Name: "example.okta.com"},
		Channel:        "system_log",
		DedupeKeyValue: "provider:evt-1",
		EventType:      "user.authentication.sso",
		Category:       "okta.system_log",
		Raw:            map[string]any{"uuid": "evt-1"},
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("DispatchEvent() err = %v, want %v", err, wantErr)
	}
}

func TestDispatcherRoutesStateAndSnapshotToProjector(t *testing.T) {
	projector := &fakeProjector{}
	dispatcher := NewDispatcher(nil, projector)
	ctx := context.Background()
	source := records.SourceRef{Kind: "okta", Name: "example.okta.com"}

	if err := dispatcher.UpsertState(ctx, records.StateUpsert{
		Source:         source,
		Resource:       records.ResourceIdentity,
		Key:            "00u1",
		ProviderID:     "00u1",
		DedupeKeyValue: "state:identity:00u1",
		Payload: records.IdentityPayload{
			ExternalID: "00u1",
		},
	}); err != nil {
		t.Fatalf("UpsertState() err = %v", err)
	}
	if err := dispatcher.DeleteState(ctx, records.StateDelete{
		Source:         source,
		Resource:       records.ResourceIdentity,
		Key:            "00u2",
		DedupeKeyValue: "state_delete:identity:00u2",
		Reason:         records.DeleteReasonExplicit,
	}); err != nil {
		t.Fatalf("DeleteState() err = %v", err)
	}
	scope := records.FullScope{Resource: records.ResourceIdentity}
	if err := dispatcher.BeginSnapshot(ctx, records.SnapshotBegin{
		Source:         source,
		Resource:       records.ResourceIdentity,
		Scope:          scope,
		DedupeKeyValue: "snapshot_begin:identity",
	}); err != nil {
		t.Fatalf("BeginSnapshot() err = %v", err)
	}
	if err := dispatcher.CompleteSnapshot(ctx, records.SnapshotComplete{
		Source:         source,
		Resource:       records.ResourceIdentity,
		Scope:          scope,
		Complete:       true,
		ExpireAbsent:   true,
		DedupeKeyValue: "snapshot_complete:identity",
	}); err != nil {
		t.Fatalf("CompleteSnapshot() err = %v", err)
	}

	if projector.upserts != 1 || projector.deletes != 1 || projector.snapshotBegins != 1 || projector.snapshotEnds != 1 {
		t.Fatalf("projector calls = %+v", projector)
	}
}

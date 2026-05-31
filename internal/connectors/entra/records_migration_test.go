package entra

import (
	"context"
	"testing"
	"time"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/discovery"
	"github.com/open-sspm/open-sspm/internal/records"
)

type recordingEmitter struct {
	upserts []records.StateUpsert
}

func (e *recordingEmitter) EmitEvent(context.Context, records.EventRecord) error {
	return nil
}

func (e *recordingEmitter) UpsertState(_ context.Context, record records.StateUpsert) error {
	e.upserts = append(e.upserts, record)
	return nil
}

func (e *recordingEmitter) DeleteState(context.Context, records.StateDelete) error {
	return nil
}

func (e *recordingEmitter) BeginSnapshot(context.Context, records.SnapshotBegin) error {
	return nil
}

func (e *recordingEmitter) CompleteSnapshot(context.Context, records.SnapshotComplete) error {
	return nil
}

func (e *recordingEmitter) EmitInternalEvent(context.Context, records.EventRecord) error {
	return nil
}

func TestEntraWriteUsersEmitsIdentityStateRecords(t *testing.T) {
	t.Parallel()

	integration := NewEntraIntegration(stubEntraClient{}, "tenant-1", false)
	user := mustParseUser(t, `{
		"id": "user-1",
		"displayName": "Alice",
		"userPrincipalName": "Alice@Example.com",
		"accountEnabled": true
	}`)
	emitter := &recordingEmitter{}

	written, err := integration.writeUsers(context.Background(), emitter, func(registry.Event) {}, 42, []User{user})
	if err != nil {
		t.Fatalf("writeUsers() err = %v", err)
	}
	if written != 1 {
		t.Fatalf("written = %d, want 1", written)
	}
	if len(emitter.upserts) != 1 {
		t.Fatalf("upserts = %d, want 1", len(emitter.upserts))
	}

	record := emitter.upserts[0]
	if record.Source.Kind != "entra" || record.Source.Name != "tenant-1" {
		t.Fatalf("source = %+v, want entra/tenant-1", record.Source)
	}
	if record.Resource != records.ResourceIdentity {
		t.Fatalf("resource = %q, want identity", record.Resource)
	}
	payload, ok := record.Payload.(records.IdentityPayload)
	if !ok {
		t.Fatalf("payload type = %T, want IdentityPayload", record.Payload)
	}
	if payload.ExternalID != "user-1" || payload.Email != "alice@example.com" || payload.DisplayName != "Alice" {
		t.Fatalf("payload identity fields = %+v", payload)
	}
	if payload.ProviderAttrs["entity_category"] != registry.EntityCategoryUser {
		t.Fatalf("entity_category = %v, want user", payload.ProviderAttrs["entity_category"])
	}
	if payload.Raw["status"] != "Active" {
		t.Fatalf("raw status = %v, want Active", payload.Raw["status"])
	}
}

func TestEntraWriteDiscoveryRowsEmitsDiscoveryEvidenceRecords(t *testing.T) {
	t.Parallel()

	integration := NewEntraIntegration(stubEntraClient{}, "tenant-1", true)
	emitter := &recordingEmitter{}
	observedAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	err := integration.writeDiscoveryRows(context.Background(), emitter, func(registry.Event) {}, 42, []discovery.SourceRow{{
		CanonicalKey:  "app:example",
		SourceAppID:   "app-1",
		SourceAppName: "Example App",
		SeenAt:        observedAt,
	}}, []discovery.EventRow{{
		CanonicalKey:    "app:example",
		SignalKind:      discovery.SignalKindIDPSSO,
		EventExternalID: "event-1",
		SourceAppID:     "app-1",
		SourceAppName:   "Example App",
		ObservedAt:      observedAt,
		RawJSON:         []byte(`{"id":"event-1"}`),
	}})
	if err != nil {
		t.Fatalf("writeDiscoveryRows() err = %v", err)
	}
	if len(emitter.upserts) != 2 {
		t.Fatalf("upserts = %d, want 2", len(emitter.upserts))
	}
	for _, record := range emitter.upserts {
		if record.Resource != records.ResourceDiscoveryEvidence {
			t.Fatalf("resource = %q, want discovery evidence", record.Resource)
		}
		payload, ok := record.Payload.(records.DiscoveryEvidencePayload)
		if !ok {
			t.Fatalf("payload type = %T, want DiscoveryEvidencePayload", record.Payload)
		}
		if payload.SourceAppID != "app-1" {
			t.Fatalf("source app id = %q, want app-1", payload.SourceAppID)
		}
	}
}

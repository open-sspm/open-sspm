package googleworkspace

import (
	"context"
	"testing"
	"time"

	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
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

func TestGoogleWorkspaceUpsertAccountsEmitsStateRecords(t *testing.T) {
	t.Parallel()

	integration := NewGoogleWorkspaceIntegration(&Client{}, "C0123", "example.com", false)
	emitter := &recordingEmitter{}
	rows := buildGoogleWorkspaceAccountRows(
		[]WorkspaceUser{{ID: "user-1", PrimaryEmail: "Alice@Example.com", Name: struct {
			FullName string `json:"fullName"`
		}{FullName: "Alice"}}},
		[]WorkspaceGroup{{ID: "group-1", Email: "Team@Example.com", Name: "Team"}},
	)

	if err := integration.upsertAccounts(context.Background(), emitter, func(registry.Event) {}, 42, rows); err != nil {
		t.Fatalf("upsertAccounts() err = %v", err)
	}
	if len(emitter.upserts) != 2 {
		t.Fatalf("upserts = %d, want 2", len(emitter.upserts))
	}

	if emitter.upserts[0].Source.Kind != configstore.KindGoogleWorkspace || emitter.upserts[0].Source.Name != "C0123" {
		t.Fatalf("source = %+v, want google_workspace/C0123", emitter.upserts[0].Source)
	}
	if emitter.upserts[0].Resource != records.ResourceIdentity {
		t.Fatalf("first resource = %q, want identity", emitter.upserts[0].Resource)
	}
	if emitter.upserts[1].Resource != records.ResourceGroup {
		t.Fatalf("second resource = %q, want group", emitter.upserts[1].Resource)
	}
	payload, ok := emitter.upserts[0].Payload.(records.IdentityPayload)
	if !ok {
		t.Fatalf("first payload = %T, want IdentityPayload", emitter.upserts[0].Payload)
	}
	if payload.Email != "alice@example.com" || payload.ProviderAttrs["entity_category"] != registry.EntityCategoryUser {
		t.Fatalf("identity payload = %+v", payload)
	}
}

func TestGoogleWorkspaceWriteDiscoveryRowsEmitsDiscoveryEvidenceRecords(t *testing.T) {
	t.Parallel()

	integration := NewGoogleWorkspaceIntegration(&Client{}, "C0123", "example.com", true)
	emitter := &recordingEmitter{}
	observedAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	err := integration.writeDiscoveryRows(context.Background(), emitter, func(registry.Event) {}, 42, []discovery.SourceRow{{
		CanonicalKey:  "app:example",
		SourceAppID:   "app-1",
		SourceAppName: "Example App",
		SeenAt:        observedAt,
	}}, []discovery.EventRow{{
		CanonicalKey:    "app:example",
		SignalKind:      discovery.SignalKindOAuth,
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
		if _, ok := record.Payload.(records.DiscoveryEvidencePayload); !ok {
			t.Fatalf("payload = %T, want DiscoveryEvidencePayload", record.Payload)
		}
	}
}

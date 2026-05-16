package records

import "testing"

func TestStateUpsertValidateRejectsMissingPayload(t *testing.T) {
	record := StateUpsert{
		Resource: ResourceIdentity,
		Key:      "okta:user:00u1",
	}

	if err := record.Validate(); err == nil {
		t.Fatal("expected missing payload to be rejected")
	}
}

func TestStateUpsertValidateRejectsPayloadResourceMismatch(t *testing.T) {
	record := StateUpsert{
		Resource: ResourceGroup,
		Key:      "okta:user:00u1",
		Payload:  IdentityPayload{ExternalID: "00u1"},
	}

	if err := record.Validate(); err == nil {
		t.Fatal("expected payload resource mismatch to be rejected")
	}
}

func TestIdentityPayloadEnvelopeKeepsValidatedFields(t *testing.T) {
	payload := IdentityPayload{
		ExternalID:    " 00u1 ",
		Email:         " user@example.com ",
		DisplayName:   " User Example ",
		Status:        " ACTIVE ",
		IsPrivileged:  true,
		ProviderAttrs: map[string]any{"provider_status": "ACTIVE"},
	}
	record := StateUpsert{
		Resource: ResourceIdentity,
		Key:      "okta:user:00u1",
		Payload:  payload,
	}

	if err := record.Validate(); err != nil {
		t.Fatalf("expected payload to validate: %v", err)
	}
	envelope := payload.ToEnvelope()
	if envelope.Resource != ResourceIdentity {
		t.Fatalf("resource = %q, want %q", envelope.Resource, ResourceIdentity)
	}
	if envelope.SchemaVersion != 1 {
		t.Fatalf("schema version = %d, want 1", envelope.SchemaVersion)
	}
	if envelope.ExternalID != "00u1" {
		t.Fatalf("external id = %q, want trimmed external id", envelope.ExternalID)
	}
	if envelope.Attributes["email"] != "user@example.com" {
		t.Fatalf("email attribute = %v, want trimmed email", envelope.Attributes["email"])
	}
}

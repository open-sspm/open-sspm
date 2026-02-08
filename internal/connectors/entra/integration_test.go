package entra

import "testing"

func TestBuildCredentialAuditEventRowsMapsCredentialAuditFields(t *testing.T) {
	t.Parallel()

	rows := buildCredentialAuditEventRows([]DirectoryAuditEvent{
		{
			ID:                  "event-1",
			Category:            "ApplicationManagement",
			Result:              "success",
			ActivityDisplayName: "Add application password credential",
			ActivityDateTimeRaw: "2026-02-07T23:00:00Z",
			InitiatedBy: DirectoryAuditInitiatedBy{
				User: &DirectoryAuditActorUser{
					ID:                "user-1",
					DisplayName:       "Alice Admin",
					UserPrincipalName: "alice@example.com",
				},
			},
			TargetResources: []DirectoryAuditTargetResource{
				{
					ID:          "app-1",
					DisplayName: "Payroll App",
					Type:        "Application",
					ModifiedProperties: []DirectoryAuditModifiedProperty{
						{
							DisplayName: "PasswordCredentials",
							NewValue:    `{"keyId":"cred-1"}`,
						},
					},
				},
			},
		},
	})

	if len(rows) != 1 {
		t.Fatalf("len(rows)=%d want 1", len(rows))
	}

	row := rows[0]
	if row.EventExternalID != "event-1" {
		t.Fatalf("EventExternalID=%q want event-1", row.EventExternalID)
	}
	if row.ActorKind != "entra_user" || row.ActorExternalID != "user-1" {
		t.Fatalf("unexpected actor mapping kind=%q external_id=%q", row.ActorKind, row.ActorExternalID)
	}
	if row.TargetKind != "entra_application" || row.TargetExternalID != "app-1" {
		t.Fatalf("unexpected target mapping kind=%q external_id=%q", row.TargetKind, row.TargetExternalID)
	}
	if row.CredentialKind != "entra_client_secret" {
		t.Fatalf("CredentialKind=%q want entra_client_secret", row.CredentialKind)
	}
	if row.CredentialExternalID != "cred-1" {
		t.Fatalf("CredentialExternalID=%q want cred-1", row.CredentialExternalID)
	}
	if !row.EventTime.Valid {
		t.Fatalf("expected valid event timestamp")
	}
}

func TestBuildCredentialAuditEventRowsSkipsInvalidOrIrrelevantEvents(t *testing.T) {
	t.Parallel()

	rows := buildCredentialAuditEventRows([]DirectoryAuditEvent{
		{
			ID:                  "sign-in-1",
			Category:            "SignInLogs",
			ActivityDisplayName: "User signed in",
			ActivityDateTimeRaw: "2026-02-07T23:00:00Z",
		},
		{
			ID:                  "event-2",
			Category:            "ApplicationManagement",
			ActivityDisplayName: "Add application password credential",
			ActivityDateTimeRaw: "not-a-time",
		},
	})

	if len(rows) != 0 {
		t.Fatalf("len(rows)=%d want 0", len(rows))
	}
}

func TestExtractCredentialExternalIDNestedJSON(t *testing.T) {
	t.Parallel()

	got := extractCredentialExternalID(`"{\"keyId\":\"123e4567-e89b-12d3-a456-426614174000\"}"`)
	want := "123e4567-e89b-12d3-a456-426614174000"
	if got != want {
		t.Fatalf("extractCredentialExternalID=%q want %q", got, want)
	}
}

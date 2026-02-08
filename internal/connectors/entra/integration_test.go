package entra

import (
	"testing"
	"time"
)

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

func TestGraphObservedAtOrNowUsesValidity(t *testing.T) {
	t.Parallel()

	fallback := time.Date(2026, 2, 8, 12, 34, 56, 0, time.UTC)

	invalid := graphObservedAtOrNow("not-a-time", fallback)
	if !invalid.Equal(fallback) {
		t.Fatalf("invalid timestamp fallback = %s want %s", invalid.Format(time.RFC3339Nano), fallback.Format(time.RFC3339Nano))
	}

	zero := graphObservedAtOrNow("0001-01-01T00:00:00Z", fallback)
	if !zero.IsZero() {
		t.Fatalf("valid zero timestamp parsed as %s, expected zero value", zero.Format(time.RFC3339Nano))
	}

	valid := graphObservedAtOrNow("2026-02-08T11:22:33Z", fallback)
	want := time.Date(2026, 2, 8, 11, 22, 33, 0, time.UTC)
	if !valid.Equal(want) {
		t.Fatalf("valid timestamp parsed as %s want %s", valid.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
	}
}

package entra

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func TestBuildCredentialAuditEventRowsMapsCredentialAuditFields(t *testing.T) {
	t.Parallel()

	rows, err := buildCredentialAuditEventRows([]DirectoryAuditEvent{
		mustParseDirectoryAudit(t, `{
			"id":"event-1",
			"category":"ApplicationManagement",
			"result":"success",
			"activityDisplayName":"Add application password credential",
			"activityDateTime":"2026-02-07T23:00:00Z",
			"initiatedBy":{"user":{"id":"user-1","displayName":"Alice Admin","userPrincipalName":"alice@example.com"}},
			"targetResources":[{"id":"app-1","displayName":"Payroll App","type":"Application","modifiedProperties":[{"displayName":"PasswordCredentials","newValue":"{\"keyId\":\"cred-1\"}"}]}]
		}`),
	})
	if err != nil {
		t.Fatalf("buildCredentialAuditEventRows() error = %v", err)
	}

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

	rows, err := buildCredentialAuditEventRows([]DirectoryAuditEvent{
		mustParseDirectoryAudit(t, `{"id":"sign-in-1","category":"SignInLogs","activityDisplayName":"User signed in","activityDateTime":"2026-02-07T23:00:00Z"}`),
		mustParseDirectoryAudit(t, `{"id":"event-2","category":"ApplicationManagement","activityDisplayName":"Add application password credential"}`),
	})
	if err != nil {
		t.Fatalf("buildCredentialAuditEventRows() error = %v", err)
	}

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

func TestBuildOwnerRowsPrefixesServicePrincipalOwnerExternalID(t *testing.T) {
	t.Parallel()

	rows, err := buildOwnerRows("entra_application", "app-1", []DirectoryOwner{
		mustParseDirectoryOwner(t, `{"id":"owner-sp-1","@odata.type":"#microsoft.graph.servicePrincipal","displayName":"Owner Service Principal","appId":"owner-client-app"}`),
	})
	if err != nil {
		t.Fatalf("buildOwnerRows() error = %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("len(rows)=%d want 1", len(rows))
	}
	if rows[0].OwnerExternalID != entraServicePrincipalExternalID("owner-sp-1") {
		t.Fatalf("OwnerExternalID=%q want %q", rows[0].OwnerExternalID, entraServicePrincipalExternalID("owner-sp-1"))
	}
}

type stubEntraClient struct {
	lookupUsersByIDs func(context.Context, []string) ([]User, error)
}

func (s stubEntraClient) ListApplications(context.Context) ([]Application, error) {
	panic("unexpected call")
}

func (s stubEntraClient) ListServicePrincipals(context.Context) ([]ServicePrincipal, error) {
	panic("unexpected call")
}

func (s stubEntraClient) ListServicePrincipalAssignedTo(context.Context, string) ([]ServicePrincipalAppRoleAssignment, error) {
	panic("unexpected call")
}

func (s stubEntraClient) ListDirectoryAudits(context.Context, *time.Time) ([]DirectoryAuditEvent, error) {
	panic("unexpected call")
}

func (s stubEntraClient) ListDirectoryRoles(context.Context) ([]DirectoryRole, error) {
	panic("unexpected call")
}

func (s stubEntraClient) ListDirectoryRoleAssignments(context.Context) ([]DirectoryRoleAssignment, error) {
	panic("unexpected call")
}

func (s stubEntraClient) ListUsers(context.Context) ([]User, error) {
	panic("unexpected call")
}

func (s stubEntraClient) ListGroups(context.Context) ([]Group, error) {
	panic("unexpected call")
}

func (s stubEntraClient) ListGroupUserMembers(context.Context, string) ([]User, error) {
	panic("unexpected call")
}

func (s stubEntraClient) ListGroupTransitiveUserMembers(context.Context, string) ([]User, error) {
	panic("unexpected call")
}

func (s stubEntraClient) ListApplicationOwners(context.Context, string) ([]DirectoryOwner, error) {
	panic("unexpected call")
}

func (s stubEntraClient) ListServicePrincipalOwners(context.Context, string) ([]DirectoryOwner, error) {
	panic("unexpected call")
}

func (s stubEntraClient) ListSignIns(context.Context, *time.Time) ([]SignInEvent, error) {
	panic("unexpected call")
}

func (s stubEntraClient) ListOAuth2PermissionGrants(context.Context) ([]OAuth2PermissionGrant, error) {
	panic("unexpected call")
}

func (s stubEntraClient) LookupUsersByIDs(ctx context.Context, ids []string) ([]User, error) {
	if s.lookupUsersByIDs == nil {
		panic("unexpected call")
	}
	return s.lookupUsersByIDs(ctx, ids)
}

func TestResolveGrantActorsReturnsDistinctResolvedUsers(t *testing.T) {
	t.Parallel()

	var gotIDs []string
	integration := &EntraIntegration{
		client: stubEntraClient{
			lookupUsersByIDs: func(_ context.Context, ids []string) ([]User, error) {
				gotIDs = append(gotIDs, ids...)
				return []User{mustParseUser(t, `{"id":"user-1","displayName":"Alice"}`)}, nil
			},
		},
	}

	var events []registry.Event
	users := integration.resolveGrantActors(context.Background(), func(event registry.Event) {
		events = append(events, event)
	}, []OAuth2PermissionGrant{
		mustParseGrant(t, `{"id":"grant-1","principalId":" user-1 "}`),
		mustParseGrant(t, `{"id":"grant-2","principalId":""}`),
		mustParseGrant(t, `{"id":"grant-3","principalId":"user-2"}`),
		mustParseGrant(t, `{"id":"grant-4","principalId":"user-1"}`),
	})

	if len(gotIDs) != 2 || gotIDs[0] != "user-1" || gotIDs[1] != "user-2" {
		t.Fatalf("gotIDs=%v want [user-1 user-2]", gotIDs)
	}
	if len(users) != 1 || entityID(users[0]) != "user-1" {
		t.Fatalf("users=%v want [{ID:user-1}]", users)
	}
	if len(events) != 2 {
		t.Fatalf("len(events)=%d want 2", len(events))
	}
	if events[0].Stage != "resolve-grant-actors" || !strings.Contains(events[0].Message, "resolving 2 Entra grant actors") {
		t.Fatalf("unexpected start event: %+v", events[0])
	}
	if events[1].Stage != "resolve-grant-actors" || !strings.Contains(events[1].Message, "resolved 1 Entra grant actors") || events[1].Err != nil {
		t.Fatalf("unexpected completion event: %+v", events[1])
	}
}

func TestResolveGrantActorsLookupFailureIsNonFatal(t *testing.T) {
	t.Parallel()

	lookupErr := errors.New("graph api failed")
	integration := &EntraIntegration{
		client: stubEntraClient{
			lookupUsersByIDs: func(_ context.Context, ids []string) ([]User, error) {
				if len(ids) != 1 || ids[0] != "user-1" {
					t.Fatalf("ids=%v want [user-1]", ids)
				}
				return nil, lookupErr
			},
		},
	}

	var events []registry.Event
	users := integration.resolveGrantActors(context.Background(), func(event registry.Event) {
		events = append(events, event)
	}, []OAuth2PermissionGrant{mustParseGrant(t, `{"id":"grant-1","principalId":"user-1"}`)})

	if users != nil {
		t.Fatalf("users=%v want nil", users)
	}
	if len(events) != 2 {
		t.Fatalf("len(events)=%d want 2", len(events))
	}
	if events[1].Stage != "resolve-grant-actors" {
		t.Fatalf("unexpected stage: %+v", events[1])
	}
	if events[1].Err == nil || !errors.Is(events[1].Err, lookupErr) {
		t.Fatalf("expected lookup error to be reported, got %+v", events[1])
	}
	if !strings.Contains(events[1].Message, "skipping grant actor enrichment") {
		t.Fatalf("unexpected failure event message: %+v", events[1])
	}
}

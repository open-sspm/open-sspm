package entra

import (
	"testing"

	absser "github.com/microsoft/kiota-abstractions-go/serialization"
	msgraphmodels "github.com/microsoftgraph/msgraph-sdk-go/models"
)

func mustParseModel[T any](t *testing.T, raw string, factory absser.ParsableFactory) T {
	t.Helper()

	ensureGraphSerializationRegistered()
	model, err := absser.Deserialize("application/json", []byte(raw), factory)
	if err != nil {
		t.Fatalf("Deserialize(%T): %v", *new(T), err)
	}
	typed, ok := model.(T)
	if !ok {
		t.Fatalf("Deserialize(%T): unexpected type %T", *new(T), model)
	}
	return typed
}

func mustParseUser(t *testing.T, raw string) User {
	t.Helper()
	return mustParseModel[User](t, raw, msgraphmodels.CreateUserFromDiscriminatorValue)
}

func mustParseApplication(t *testing.T, raw string) Application {
	t.Helper()
	return mustParseModel[Application](t, raw, msgraphmodels.CreateApplicationFromDiscriminatorValue)
}

func mustParseServicePrincipal(t *testing.T, raw string) ServicePrincipal {
	t.Helper()
	return mustParseModel[ServicePrincipal](t, raw, msgraphmodels.CreateServicePrincipalFromDiscriminatorValue)
}

func mustParseGroup(t *testing.T, raw string) Group {
	t.Helper()
	return mustParseModel[Group](t, raw, msgraphmodels.CreateGroupFromDiscriminatorValue)
}

func mustParseDirectoryAudit(t *testing.T, raw string) DirectoryAuditEvent {
	t.Helper()
	return mustParseModel[DirectoryAuditEvent](t, raw, msgraphmodels.CreateDirectoryAuditFromDiscriminatorValue)
}

func mustParseSignIn(t *testing.T, raw string) SignInEvent {
	t.Helper()
	return mustParseModel[SignInEvent](t, raw, msgraphmodels.CreateSignInFromDiscriminatorValue)
}

func mustParseGrant(t *testing.T, raw string) OAuth2PermissionGrant {
	t.Helper()
	return mustParseModel[OAuth2PermissionGrant](t, raw, msgraphmodels.CreateOAuth2PermissionGrantFromDiscriminatorValue)
}

func mustParseAppRoleAssignment(t *testing.T, raw string) ServicePrincipalAppRoleAssignment {
	t.Helper()
	return mustParseModel[ServicePrincipalAppRoleAssignment](t, raw, msgraphmodels.CreateAppRoleAssignmentFromDiscriminatorValue)
}

func mustParseDirectoryRole(t *testing.T, raw string) DirectoryRole {
	t.Helper()
	return mustParseModel[DirectoryRole](t, raw, msgraphmodels.CreateUnifiedRoleDefinitionFromDiscriminatorValue)
}

func mustParseDirectoryRoleAssignment(t *testing.T, raw string) DirectoryRoleAssignment {
	t.Helper()
	return mustParseModel[DirectoryRoleAssignment](t, raw, msgraphmodels.CreateUnifiedRoleAssignmentFromDiscriminatorValue)
}

func mustParseDirectoryOwner(t *testing.T, raw string) DirectoryOwner {
	t.Helper()
	return mustParseModel[DirectoryOwner](t, raw, msgraphmodels.CreateDirectoryObjectFromDiscriminatorValue)
}

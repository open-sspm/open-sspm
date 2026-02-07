package datasets

import (
	"context"
	"errors"
	"testing"

	runtimev2 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/runtime/v2"
	"github.com/open-sspm/open-sspm/internal/connectors/oktaapi"
)

type stubOktaClient struct {
	listPoliciesErr error
}

func (s stubOktaClient) ListPolicies(ctx context.Context, policyType string) ([]oktaapi.Policy, error) {
	return nil, s.listPoliciesErr
}
func (s stubOktaClient) ListOktaSignOnPolicyRules(ctx context.Context, policyID string) ([]oktaapi.SignOnPolicyRule, error) {
	return nil, errors.New("unexpected call")
}
func (s stubOktaClient) ListAccessPolicyRules(ctx context.Context, policyID string) ([]oktaapi.AccessPolicyRule, error) {
	return nil, errors.New("unexpected call")
}
func (s stubOktaClient) SearchApplications(ctx context.Context, query string) ([]oktaapi.Application, error) {
	return nil, errors.New("unexpected call")
}
func (s stubOktaClient) ListPasswordPolicies(ctx context.Context) ([]oktaapi.PasswordPolicy, error) {
	return nil, errors.New("unexpected call")
}
func (s stubOktaClient) ListAuthenticators(ctx context.Context) ([]oktaapi.Authenticator, error) {
	return nil, errors.New("unexpected call")
}
func (s stubOktaClient) GetAdminConsoleSettings(ctx context.Context) (oktaapi.AdminConsoleSettings, error) {
	return oktaapi.AdminConsoleSettings{}, errors.New("unexpected call")
}

func TestRouterProviderMissingProvider(t *testing.T) {
	p := RouterProvider{}
	res := p.GetDataset(context.Background(), runtimev2.EvalContext{}, runtimev2.DatasetRef{Dataset: "okta:apps", Version: 1})
	if res.Error == nil {
		t.Fatalf("expected error, got nil")
	}
	if res.Error.Kind != runtimev2.DatasetErrorKind_MISSING_DATASET {
		t.Fatalf("expected missing_dataset, got %q", res.Error.Kind)
	}
}

func TestOktaProviderPermissionDeniedIsCategorized(t *testing.T) {
	p := &OktaProvider{
		Client: stubOktaClient{listPoliciesErr: &oktaapi.APIError{StatusCode: 403, Status: "403 Forbidden", Summary: "nope"}},
	}
	res := p.GetDataset(context.Background(), runtimev2.EvalContext{}, runtimev2.DatasetRef{Dataset: "okta:policies/sign-on", Version: 1})
	if res.Error == nil {
		t.Fatalf("expected error, got nil")
	}
	if res.Error.Kind != runtimev2.DatasetErrorKind_PERMISSION_DENIED {
		t.Fatalf("expected permission_denied, got %q", res.Error.Kind)
	}
}

func TestNormalizedProviderRejectsUnsupportedVersion(t *testing.T) {
	p := &NormalizedProvider{}
	res := p.GetDataset(context.Background(), runtimev2.EvalContext{}, runtimev2.DatasetRef{Dataset: "normalized:identities", Version: 3})
	if res.Error == nil {
		t.Fatalf("expected error, got nil")
	}
	if res.Error.Kind != runtimev2.DatasetErrorKind_MISSING_DATASET {
		t.Fatalf("expected missing_dataset, got %q", res.Error.Kind)
	}
}

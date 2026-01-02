package datasets

import (
	"context"
	"errors"
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/oktaapi"
	"github.com/open-sspm/open-sspm/internal/rules/engine"
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
	_, err := p.GetDataset(context.Background(), "okta:apps", nil)
	if err == nil {
		t.Fatalf("expected error")
	}
	var de engine.DatasetError
	if !errors.As(err, &de) {
		t.Fatalf("expected dataset error, got %T", err)
	}
	if de.Kind != engine.DatasetErrorMissingDataset {
		t.Fatalf("expected missing_dataset, got %q", de.Kind)
	}
}

func TestOktaProviderPermissionDeniedIsCategorized(t *testing.T) {
	p := &OktaProvider{
		Client: stubOktaClient{listPoliciesErr: &oktaapi.APIError{StatusCode: 403, Status: "403 Forbidden", Summary: "nope"}},
	}
	_, err := p.GetDataset(context.Background(), "okta:policies/sign-on", nil)
	if err == nil {
		t.Fatalf("expected error")
	}
	var de engine.DatasetError
	if !errors.As(err, &de) {
		t.Fatalf("expected dataset error, got %T", err)
	}
	if de.Kind != engine.DatasetErrorPermissionDenied {
		t.Fatalf("expected permission_denied, got %q", de.Kind)
	}
}

func TestNormalizedProviderRejectsUnsupportedVersion(t *testing.T) {
	v2 := 2
	p := &NormalizedProvider{}
	_, err := p.GetDataset(context.Background(), "normalized:identities", &v2)
	if err == nil {
		t.Fatalf("expected error")
	}
	var de engine.DatasetError
	if !errors.As(err, &de) {
		t.Fatalf("expected dataset error, got %T", err)
	}
	if de.Kind != engine.DatasetErrorMissingDataset {
		t.Fatalf("expected missing_dataset, got %q", de.Kind)
	}
}

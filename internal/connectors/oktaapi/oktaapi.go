package oktaapi

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

type Client interface {
	ListPolicies(ctx context.Context, policyType string) ([]Policy, error)
	ListOktaSignOnPolicyRules(ctx context.Context, policyID string) ([]SignOnPolicyRule, error)
	ListAccessPolicyRules(ctx context.Context, policyID string) ([]AccessPolicyRule, error)
	SearchApplications(ctx context.Context, query string) ([]Application, error)
	ListPasswordPolicies(ctx context.Context) ([]PasswordPolicy, error)
	ListAuthenticators(ctx context.Context) ([]Authenticator, error)
	GetAdminConsoleSettings(ctx context.Context) (AdminConsoleSettings, error)
}

type APIError struct {
	StatusCode int
	Status     string
	Summary    string
}

var ErrAPI = errors.New("okta api error")

func (e *APIError) Error() string {
	if e == nil {
		return ""
	}
	status := strings.TrimSpace(e.Status)
	summary := strings.TrimSpace(e.Summary)
	if status != "" && summary != "" {
		return fmt.Sprintf("okta api error: %s: %s", status, summary)
	}
	if status != "" {
		return fmt.Sprintf("okta api error: %s", status)
	}
	if summary != "" {
		return fmt.Sprintf("okta api error: %s", summary)
	}
	return "okta api error"
}

func (e *APIError) Unwrap() error {
	return ErrAPI
}

type Policy struct {
	ID       string
	Type     string
	Name     string
	Status   string
	Priority *int32
	System   *bool
}

type Application struct {
	ID             string
	Label          string
	Name           string
	AccessPolicyID string
}

type SignOnPolicyRuleSession struct {
	MaxSessionIdleMinutes     *int32
	MaxSessionLifetimeMinutes *int32
	UsePersistentCookie       *bool
}

type SignOnPolicyRule struct {
	ID       string
	Name     string
	Status   string
	Priority *int32
	System   *bool
	Session  SignOnPolicyRuleSession
}

type AccessPolicyRuleVerificationMethod struct {
	Type        string
	FactorMode  string
	Constraints []AccessPolicyConstraint
	Chains      []AuthenticationMethodChain
}

type AccessPolicyConstraint struct {
	Possession *AccessPolicyConstraintPossession
}

type AccessPolicyConstraintPossession struct {
	PhishingResistant string
}

type AuthenticationMethodChain struct {
	AuthenticationMethods []AuthenticationMethod
}

type AuthenticationMethod struct {
	Key               string
	Method            string
	PhishingResistant string
}

type AccessPolicyRule struct {
	ID       string
	Name     string
	Status   string
	Priority *int32
	System   *bool

	AppSignOnVerificationMethod AccessPolicyRuleVerificationMethod
}

type PasswordPolicyComplexitySettings struct {
	MinLength               *int32
	MinUpperCase            *int32
	MinLowerCase            *int32
	MinNumber               *int32
	MinSymbol               *int32
	CommonDictionaryExclude *bool
}

type PasswordPolicyAgeSettings struct {
	MinAgeMinutes *int32
	MaxAgeDays    *int32
	HistoryCount  *int32
}

type PasswordPolicyLockoutSettings struct {
	MaxAttempts       *int32
	AutoUnlockMinutes *int32
}

type PasswordPolicySettings struct {
	Complexity PasswordPolicyComplexitySettings
	Age        PasswordPolicyAgeSettings
	Lockout    PasswordPolicyLockoutSettings
}

type PasswordPolicy struct {
	ID       string
	Name     string
	Status   string
	Priority *int32
	System   *bool

	Settings PasswordPolicySettings
}

type Authenticator struct {
	ID     string
	Key    string
	Name   string
	Status string

	OktaVerifyComplianceFips *string
}

type AdminConsoleSettings struct {
	SessionIdleTimeoutMinutes *int32
	SessionMaxLifetimeMinutes *int32
}

package okta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	sdk "github.com/okta/okta-sdk-golang/v6/okta"
	"github.com/open-sspm/open-sspm/internal/connectors/oktaapi"
)

func (c *Client) ListAccessPolicyRules(ctx context.Context, policyID string) ([]oktaapi.AccessPolicyRule, error) {
	if c == nil {
		return nil, errors.New("okta client is nil")
	}
	policyID = strings.TrimSpace(policyID)
	if policyID == "" {
		return nil, errors.New("okta policy id is required")
	}

	baseURL := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if baseURL == "" {
		return nil, errors.New("okta base URL is required")
	}

	token := strings.TrimSpace(c.Token)
	if token == "" {
		return nil, errors.New("okta token is required")
	}

	var out []oktaapi.AccessPolicyRule
	after := ""

	for {
		u, err := url.Parse(baseURL + "/api/v1/policies/" + url.PathEscape(policyID) + "/rules")
		if err != nil {
			return nil, err
		}
		q := u.Query()
		q.Set("limit", "200")
		if after != "" {
			q.Set("after", after)
		}
		u.RawQuery = q.Encode()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Authorization", "SSWS "+token)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode >= 300 {
			return nil, &oktaapi.APIError{
				StatusCode: resp.StatusCode,
				Status:     resp.Status,
				Summary:    oktaErrorSummaryFromBody(body),
			}
		}

		var rules []oktaListAccessPolicyRule
		if err := json.Unmarshal(body, &rules); err != nil {
			return nil, fmt.Errorf("decode okta access policy rules: %w", err)
		}
		for _, rule := range rules {
			mapped := oktaapi.AccessPolicyRule{
				ID:       strings.TrimSpace(rule.ID),
				Name:     strings.TrimSpace(rule.Name),
				Status:   strings.TrimSpace(rule.Status),
				Priority: rule.Priority,
				System:   rule.System,
			}

			if rule.Actions != nil && rule.Actions.AppSignOn != nil && len(rule.Actions.AppSignOn.VerificationMethod) > 0 {
				mapped.AppSignOnVerificationMethod = oktaParseAccessPolicyRuleVerificationMethod(rule.Actions.AppSignOn.VerificationMethod)
			}

			out = append(out, mapped)
		}

		nextAfter := oktaNextAfterFromLink(resp.Header.Get("Link"))
		if nextAfter == "" {
			break
		}
		after = nextAfter
	}

	return out, nil
}

func oktaParseAccessPolicyRuleVerificationMethod(raw json.RawMessage) oktaapi.AccessPolicyRuleVerificationMethod {
	var out oktaapi.AccessPolicyRuleVerificationMethod
	if len(raw) == 0 {
		return out
	}

	var base struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &base); err != nil {
		return out
	}
	out.Type = strings.TrimSpace(base.Type)

	switch out.Type {
	case "ASSURANCE":
		var m struct {
			FactorMode  string `json:"factorMode"`
			Constraints []struct {
				Possession *struct {
					PhishingResistant string `json:"phishingResistant"`
				} `json:"possession"`
			} `json:"constraints"`
		}
		if err := json.Unmarshal(raw, &m); err != nil {
			return out
		}
		out.FactorMode = strings.TrimSpace(m.FactorMode)
		for _, c := range m.Constraints {
			var constraint oktaapi.AccessPolicyConstraint
			if c.Possession != nil {
				constraint.Possession = &oktaapi.AccessPolicyConstraintPossession{
					PhishingResistant: strings.TrimSpace(c.Possession.PhishingResistant),
				}
			}
			out.Constraints = append(out.Constraints, constraint)
		}

	case "AUTH_METHOD_CHAIN":
		var m struct {
			Chains []struct {
				AuthenticationMethods []struct {
					Key               string `json:"key"`
					Method            string `json:"method"`
					PhishingResistant string `json:"phishingResistant"`
				} `json:"authenticationMethods"`
			} `json:"chains"`
		}
		if err := json.Unmarshal(raw, &m); err != nil {
			return out
		}
		for _, c := range m.Chains {
			var chain oktaapi.AuthenticationMethodChain
			for _, am := range c.AuthenticationMethods {
				chain.AuthenticationMethods = append(chain.AuthenticationMethods, oktaapi.AuthenticationMethod{
					Key:               strings.TrimSpace(am.Key),
					Method:            strings.TrimSpace(am.Method),
					PhishingResistant: strings.TrimSpace(am.PhishingResistant),
				})
			}
			out.Chains = append(out.Chains, chain)
		}
	}

	return out
}

func (c *Client) ListPasswordPolicies(ctx context.Context) ([]oktaapi.PasswordPolicy, error) {
	if c == nil {
		return nil, errors.New("okta client is nil")
	}

	baseURL := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if baseURL == "" {
		return nil, errors.New("okta base URL is required")
	}

	token := strings.TrimSpace(c.Token)
	if token == "" {
		return nil, errors.New("okta token is required")
	}

	var out []oktaapi.PasswordPolicy
	after := ""

	for {
		u, err := url.Parse(baseURL + "/api/v1/policies")
		if err != nil {
			return nil, err
		}
		q := u.Query()
		q.Set("type", "PASSWORD")
		q.Set("status", "ACTIVE")
		q.Set("limit", "200")
		if after != "" {
			q.Set("after", after)
		}
		u.RawQuery = q.Encode()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Authorization", "SSWS "+token)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode >= 300 {
			return nil, &oktaapi.APIError{
				StatusCode: resp.StatusCode,
				Status:     resp.Status,
				Summary:    oktaErrorSummaryFromBody(body),
			}
		}

		var policies []sdk.PasswordPolicy
		if err := json.Unmarshal(body, &policies); err != nil {
			return nil, fmt.Errorf("decode okta password policies: %w", err)
		}
		for _, p := range policies {
			mapped := oktaapi.PasswordPolicy{
				ID:       strings.TrimSpace(p.GetId()),
				Name:     strings.TrimSpace(p.Name),
				Status:   strings.TrimSpace(p.GetStatus()),
				Priority: p.Priority,
				System:   p.System,
			}

			if p.Settings != nil && p.Settings.Password != nil {
				pass := p.Settings.Password
				if pass.Age != nil {
					mapped.Settings.Age.MinAgeMinutes = pass.Age.MinAgeMinutes
					mapped.Settings.Age.MaxAgeDays = pass.Age.MaxAgeDays
					mapped.Settings.Age.HistoryCount = pass.Age.HistoryCount
				}
				if pass.Complexity != nil {
					complexity := pass.Complexity
					mapped.Settings.Complexity.MinLength = complexity.MinLength
					mapped.Settings.Complexity.MinUpperCase = complexity.MinUpperCase
					mapped.Settings.Complexity.MinLowerCase = complexity.MinLowerCase
					mapped.Settings.Complexity.MinNumber = complexity.MinNumber
					mapped.Settings.Complexity.MinSymbol = complexity.MinSymbol
					if complexity.Dictionary != nil && complexity.Dictionary.Common != nil {
						mapped.Settings.Complexity.CommonDictionaryExclude = complexity.Dictionary.Common.Exclude
					}
				}
				if pass.Lockout != nil {
					mapped.Settings.Lockout.MaxAttempts = pass.Lockout.MaxAttempts
					mapped.Settings.Lockout.AutoUnlockMinutes = pass.Lockout.AutoUnlockMinutes
				}
			}

			out = append(out, mapped)
		}

		nextAfter := oktaNextAfterFromLink(resp.Header.Get("Link"))
		if nextAfter == "" {
			break
		}
		after = nextAfter
	}

	return out, nil
}

func (c *Client) ListAuthenticators(ctx context.Context) ([]oktaapi.Authenticator, error) {
	if c == nil {
		return nil, errors.New("okta client is nil")
	}

	baseURL := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if baseURL == "" {
		return nil, errors.New("okta base URL is required")
	}

	token := strings.TrimSpace(c.Token)
	if token == "" {
		return nil, errors.New("okta token is required")
	}

	var out []oktaapi.Authenticator
	after := ""

	for {
		u, err := url.Parse(baseURL + "/api/v1/authenticators")
		if err != nil {
			return nil, err
		}
		q := u.Query()
		q.Set("limit", "200")
		if after != "" {
			q.Set("after", after)
		}
		u.RawQuery = q.Encode()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Authorization", "SSWS "+token)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode >= 300 {
			return nil, &oktaapi.APIError{
				StatusCode: resp.StatusCode,
				Status:     resp.Status,
				Summary:    oktaErrorSummaryFromBody(body),
			}
		}

		var authenticators []oktaListAuthenticator
		if err := json.Unmarshal(body, &authenticators); err != nil {
			return nil, fmt.Errorf("decode okta authenticators: %w", err)
		}
		for _, a := range authenticators {
			mapped := oktaapi.Authenticator{
				ID:     strings.TrimSpace(a.ID),
				Key:    strings.TrimSpace(a.Key),
				Name:   strings.TrimSpace(a.Name),
				Status: strings.TrimSpace(a.Status),
			}
			if mapped.Key == "okta_verify" && a.Settings != nil && a.Settings.Compliance != nil {
				fips := strings.TrimSpace(a.Settings.Compliance.Fips)
				if fips != "" {
					mapped.OktaVerifyComplianceFips = &fips
				}
			}
			out = append(out, mapped)
		}

		nextAfter := oktaNextAfterFromLink(resp.Header.Get("Link"))
		if nextAfter == "" {
			break
		}
		after = nextAfter
	}

	return out, nil
}

type oktaListAccessPolicyRule struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Status   string `json:"status"`
	Priority *int32 `json:"priority"`
	System   *bool  `json:"system"`
	Actions  *struct {
		AppSignOn *struct {
			VerificationMethod json.RawMessage `json:"verificationMethod"`
		} `json:"appSignOn"`
	} `json:"actions"`
}

type oktaListAuthenticator struct {
	ID     string `json:"id"`
	Key    string `json:"key"`
	Name   string `json:"name"`
	Status string `json:"status"`

	Settings *struct {
		Compliance *struct {
			Fips string `json:"fips"`
		} `json:"compliance"`
	} `json:"settings"`
}

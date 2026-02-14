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

func (c *Client) ListPolicies(ctx context.Context, policyType string) ([]oktaapi.Policy, error) {
	if c == nil {
		return nil, errors.New("okta client is nil")
	}
	policyType = strings.TrimSpace(policyType)
	if policyType == "" {
		return nil, errors.New("okta policy type is required")
	}

	baseURL := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if baseURL == "" {
		return nil, errors.New("okta base URL is required")
	}

	token := strings.TrimSpace(c.Token)
	if token == "" {
		return nil, errors.New("okta token is required")
	}

	var out []oktaapi.Policy
	after := ""

	for {
		u, err := url.Parse(baseURL + "/api/v1/policies")
		if err != nil {
			return nil, err
		}
		q := u.Query()
		q.Set("type", policyType)
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

		var policies []sdk.Policy
		if err := json.Unmarshal(body, &policies); err != nil {
			return nil, err
		}
		for _, p := range policies {
			out = append(out, oktaapi.Policy{
				ID:       strings.TrimSpace(p.GetId()),
				Type:     strings.TrimSpace(p.Type),
				Name:     strings.TrimSpace(p.Name),
				Status:   strings.TrimSpace(p.GetStatus()),
				Priority: p.Priority,
				System:   p.System,
			})
		}

		nextAfter := oktaNextAfterFromLink(resp.Header.Get("Link"))
		if nextAfter == "" {
			break
		}
		after = nextAfter
	}

	return out, nil
}

func (c *Client) ListOktaSignOnPolicyRules(ctx context.Context, policyID string) ([]oktaapi.SignOnPolicyRule, error) {
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

	var out []oktaapi.SignOnPolicyRule
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

		var rules []oktaListPolicyRule
		if err := json.Unmarshal(body, &rules); err != nil {
			return nil, fmt.Errorf("decode okta sign-on policy rules: %w", err)
		}
		for _, rule := range rules {
			mapped := oktaapi.SignOnPolicyRule{
				ID:       strings.TrimSpace(rule.ID),
				Name:     strings.TrimSpace(rule.Name),
				Status:   strings.TrimSpace(rule.Status),
				Priority: rule.Priority,
				System:   rule.System,
			}

			if rule.Actions != nil && rule.Actions.Signon != nil && rule.Actions.Signon.Session != nil {
				mapped.Session = oktaapi.SignOnPolicyRuleSession{
					MaxSessionIdleMinutes:     rule.Actions.Signon.Session.MaxSessionIdleMinutes,
					MaxSessionLifetimeMinutes: rule.Actions.Signon.Session.MaxSessionLifetimeMinutes,
					UsePersistentCookie:       rule.Actions.Signon.Session.UsePersistentCookie,
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

func (c *Client) SearchApplications(ctx context.Context, query string) ([]oktaapi.Application, error) {
	if c == nil {
		return nil, errors.New("okta client is nil")
	}
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, errors.New("okta app query is required")
	}

	baseURL := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if baseURL == "" {
		return nil, errors.New("okta base URL is required")
	}

	token := strings.TrimSpace(c.Token)
	if token == "" {
		return nil, errors.New("okta token is required")
	}

	var out []oktaapi.Application
	after := ""

	for {
		u, err := url.Parse(baseURL + "/api/v1/apps")
		if err != nil {
			return nil, err
		}
		q := u.Query()
		q.Set("q", query)
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

		var apps []oktaListApplication
		if err := json.Unmarshal(body, &apps); err != nil {
			return nil, fmt.Errorf("decode okta applications: %w", err)
		}
		for _, app := range apps {
			accessPolicyID := ""
			if app.Links.AccessPolicy != nil {
				accessPolicyID = oktaPolicyIDFromHref(app.Links.AccessPolicy.Href)
			}
			out = append(out, oktaapi.Application{
				ID:             strings.TrimSpace(app.ID),
				Label:          strings.TrimSpace(app.Label),
				Name:           strings.TrimSpace(app.Name),
				AccessPolicyID: accessPolicyID,
			})
		}

		nextAfter := oktaNextAfterFromLink(resp.Header.Get("Link"))
		if nextAfter == "" {
			break
		}
		after = nextAfter
	}

	return out, nil
}

func (c *Client) GetAdminConsoleSettings(ctx context.Context) (oktaapi.AdminConsoleSettings, error) {
	if err := c.ensureClient(); err != nil {
		return oktaapi.AdminConsoleSettings{}, err
	}

	settings, resp, err := c.api.OktaApplicationSettingsAPI.GetFirstPartyAppSettings(ctx, "admin-console").Execute()
	if err != nil {
		return oktaapi.AdminConsoleSettings{}, oktaAPIErrorFromSDK(err, resp)
	}
	if settings == nil {
		return oktaapi.AdminConsoleSettings{}, errors.New("okta admin console settings response is empty")
	}

	var idle *int32
	if v, ok := settings.GetSessionIdleTimeoutMinutesOk(); ok {
		idle = v
	}
	var lifetime *int32
	if v, ok := settings.GetSessionMaxLifetimeMinutesOk(); ok {
		lifetime = v
	}

	return oktaapi.AdminConsoleSettings{
		SessionIdleTimeoutMinutes: idle,
		SessionMaxLifetimeMinutes: lifetime,
	}, nil
}

func oktaErrorSummaryFromBody(body []byte) string {
	type payload struct {
		ErrorSummary string `json:"errorSummary"`
	}
	var p payload
	if err := json.Unmarshal(body, &p); err == nil {
		if s := strings.TrimSpace(p.ErrorSummary); s != "" {
			return s
		}
	}
	s := strings.TrimSpace(string(body))
	if len(s) > 500 {
		s = s[:500] + "…"
	}
	return s
}

type oktaListApplication struct {
	ID    string `json:"id"`
	Label string `json:"label"`
	Name  string `json:"name"`
	Links struct {
		AccessPolicy *struct {
			Href string `json:"href"`
		} `json:"accessPolicy"`
	} `json:"_links"`
}

func oktaPolicyIDFromHref(href string) string {
	href = strings.TrimSpace(href)
	if href == "" {
		return ""
	}
	u, err := url.Parse(href)
	if err != nil {
		return ""
	}
	path := strings.Trim(u.Path, "/")
	if path == "" {
		return ""
	}
	parts := strings.Split(path, "/")
	for i := 0; i+1 < len(parts); i++ {
		if parts[i] == "policies" {
			return strings.TrimSpace(parts[i+1])
		}
	}
	return ""
}

type oktaListPolicyRule struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Status   string `json:"status"`
	Priority *int32 `json:"priority"`
	System   *bool  `json:"system"`
	Actions  *struct {
		Signon *struct {
			Session *struct {
				MaxSessionIdleMinutes     *int32 `json:"maxSessionIdleMinutes"`
				MaxSessionLifetimeMinutes *int32 `json:"maxSessionLifetimeMinutes"`
				UsePersistentCookie       *bool  `json:"usePersistentCookie"`
			} `json:"session"`
		} `json:"signon"`
	} `json:"actions"`
}

func oktaAPIErrorFromSDK(err error, resp *sdk.APIResponse) error {
	if err == nil {
		return nil
	}

	statusCode := 0
	status := ""
	if resp != nil && resp.Response != nil {
		statusCode = resp.Response.StatusCode
		status = resp.Response.Status
	}

	summary := ""
	var apiErr *sdk.GenericOpenAPIError
	if errors.As(err, &apiErr) {
		if model := apiErr.Model(); model != nil {
			switch v := model.(type) {
			case sdk.Error:
				summary = strings.TrimSpace(v.GetErrorSummary())
			case *sdk.Error:
				summary = strings.TrimSpace(v.GetErrorSummary())
			}
		}
		if summary == "" {
			summary = oktaErrorSummaryFromBody(apiErr.Body())
		}
	}

	if statusCode == 0 && status == "" {
		return err
	}
	if summary == "" {
		summary = strings.TrimSpace(err.Error())
	}
	return &oktaapi.APIError{StatusCode: statusCode, Status: status, Summary: summary}
}

func oktaNextAfterFromLink(link string) string {
	link = strings.TrimSpace(link)
	if link == "" {
		return ""
	}
	parts := strings.SplitSeq(link, ",")
	for part := range parts {
		part = strings.TrimSpace(part)
		if !strings.Contains(part, `rel="next"`) {
			continue
		}

		start := strings.Index(part, "<")
		end := strings.Index(part, ">")
		if start == -1 || end == -1 || end <= start+1 {
			continue
		}

		u, err := url.Parse(part[start+1 : end])
		if err != nil {
			continue
		}
		after := strings.TrimSpace(u.Query().Get("after"))
		if after != "" {
			return after
		}
	}
	return ""
}

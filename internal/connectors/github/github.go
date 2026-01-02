package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

type Member struct {
	Login       string
	ID          int64
	Role        string
	Email       string
	DisplayName string
	RawJSON     []byte
}

type Team struct {
	ID   int64
	Name string
	Slug string
}

type TeamMember struct {
	Login string
	ID    int64
}

type TeamRepo struct {
	FullName   string
	Permission string
	RawJSON    []byte
}

type repoPermissions struct {
	Admin    bool `json:"admin"`
	Maintain bool `json:"maintain"`
	Push     bool `json:"push"`
	Triage   bool `json:"triage"`
	Pull     bool `json:"pull"`
}

// New creates a new GitHub client. It validates that both baseURL and token are provided.
func New(baseURL, token string) (*Client, error) {
	base := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	token = strings.TrimSpace(token)

	if base == "" {
		return nil, errors.New("github base URL is required")
	}
	if token == "" {
		return nil, errors.New("github token is required")
	}

	return &Client{
		BaseURL: base,
		Token:   token,
		HTTP:    &http.Client{},
	}, nil
}

func (c *Client) ListOrgMembers(ctx context.Context, org string) ([]Member, error) {
	members, err := c.listOrgMembersGraphQL(ctx, org)
	if err == nil {
		return members, nil
	}
	members, restErr := c.listOrgMembersREST(ctx, org)
	if restErr == nil {
		return members, nil
	}
	return nil, errors.Join(err, restErr)
}

func (c *Client) listOrgMembersREST(ctx context.Context, org string) ([]Member, error) {
	if c.BaseURL == "" || c.Token == "" {
		return nil, errors.New("github base URL and token are required")
	}
	url := fmt.Sprintf("%s/orgs/%s/members?per_page=100", c.BaseURL, org)
	var out []Member
	for url != "" {
		rawItems, next, err := c.getRawPage(ctx, url)
		if err != nil {
			return nil, err
		}
		for _, raw := range rawItems {
			var u struct {
				Login string `json:"login"`
				ID    int64  `json:"id"`
			}
			if err := json.Unmarshal(raw, &u); err != nil {
				return nil, err
			}
			role, err := c.getOrgRole(ctx, org, u.Login)
			if err != nil {
				return nil, err
			}
			name, email, err := c.getUserDetails(ctx, u.Login)
			if err != nil {
				return nil, err
			}
			out = append(out, Member{
				Login:       u.Login,
				ID:          u.ID,
				Role:        role,
				Email:       email,
				DisplayName: name,
				RawJSON:     raw,
			})
		}
		url = next
	}
	return out, nil
}

func (c *Client) ListTeams(ctx context.Context, org string) ([]Team, error) {
	url := fmt.Sprintf("%s/orgs/%s/teams?per_page=100", c.BaseURL, org)
	var out []Team
	for url != "" {
		rawItems, next, err := c.getRawPage(ctx, url)
		if err != nil {
			return nil, err
		}
		for _, raw := range rawItems {
			var t struct {
				ID   int64  `json:"id"`
				Name string `json:"name"`
				Slug string `json:"slug"`
			}
			if err := json.Unmarshal(raw, &t); err != nil {
				return nil, err
			}
			out = append(out, Team{ID: t.ID, Name: t.Name, Slug: t.Slug})
		}
		url = next
	}
	return out, nil
}

func (c *Client) ListTeamMembers(ctx context.Context, org, teamSlug string) ([]TeamMember, error) {
	url := fmt.Sprintf("%s/orgs/%s/teams/%s/members?per_page=100", c.BaseURL, org, teamSlug)
	var out []TeamMember
	for url != "" {
		rawItems, next, err := c.getRawPage(ctx, url)
		if err != nil {
			return nil, err
		}
		for _, raw := range rawItems {
			var u struct {
				Login string `json:"login"`
				ID    int64  `json:"id"`
			}
			if err := json.Unmarshal(raw, &u); err != nil {
				return nil, err
			}
			out = append(out, TeamMember{Login: u.Login, ID: u.ID})
		}
		url = next
	}
	return out, nil
}

func (c *Client) ListTeamRepos(ctx context.Context, org, teamSlug string) ([]TeamRepo, error) {
	url := fmt.Sprintf("%s/orgs/%s/teams/%s/repos?per_page=100", c.BaseURL, org, teamSlug)
	var out []TeamRepo
	for url != "" {
		rawItems, next, err := c.getRawPage(ctx, url)
		if err != nil {
			return nil, err
		}
		for _, raw := range rawItems {
			var r struct {
				FullName    string          `json:"full_name"`
				Permissions repoPermissions `json:"permissions"`
			}
			if err := json.Unmarshal(raw, &r); err != nil {
				return nil, err
			}
			perm := highestPermission(r.Permissions)
			out = append(out, TeamRepo{
				FullName:   r.FullName,
				Permission: perm,
				RawJSON:    raw,
			})
		}
		url = next
	}
	return out, nil
}

func highestPermission(p repoPermissions) string {
	if p.Admin {
		return "admin"
	}
	if p.Maintain {
		return "maintain"
	}
	if p.Push {
		return "push"
	}
	if p.Triage {
		return "triage"
	}
	if p.Pull {
		return "pull"
	}
	return ""
}

func (c *Client) getOrgRole(ctx context.Context, org, login string) (string, error) {
	url := fmt.Sprintf("%s/orgs/%s/memberships/%s", c.BaseURL, org, login)
	resp, err := c.doRequest(ctx, url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return "", formatGitHubAPIError("github org membership failed", url, resp, body)
	}
	var payload struct {
		Role string `json:"role"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", err
	}
	return payload.Role, nil
}

func (c *Client) getUserDetails(ctx context.Context, login string) (string, string, error) {
	url := fmt.Sprintf("%s/users/%s", c.BaseURL, login)
	resp, err := c.doRequest(ctx, url)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return "", "", formatGitHubAPIError("github user lookup failed", url, resp, body)
	}
	var payload struct {
		Name  string `json:"name"`
		Email string `json:"email"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", "", err
	}
	return payload.Name, payload.Email, nil
}

func (c *Client) getRawPage(ctx context.Context, url string) ([]json.RawMessage, string, error) {
	resp, err := c.doRequest(ctx, url)
	if err != nil {
		return nil, "", err
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "", formatGitHubAPIError("github api failed", url, resp, body)
	}
	var items []json.RawMessage
	if err := json.Unmarshal(body, &items); err != nil {
		return nil, "", err
	}
	return items, parseNextLink(resp.Header.Get("Link")), nil
}

func (c *Client) listOrgMembersGraphQL(ctx context.Context, org string) ([]Member, error) {
	if c.BaseURL == "" || c.Token == "" {
		return nil, errors.New("github base URL and token are required")
	}

	type gqlPayload struct {
		Data struct {
			Organization *struct {
				MembersWithRole struct {
					Edges []struct {
						Role string `json:"role"`
						Node struct {
							Login      string `json:"login"`
							DatabaseID *int64 `json:"databaseId"`
							Name       string `json:"name"`
							Email      string `json:"email"`
						} `json:"node"`
					} `json:"edges"`
					PageInfo struct {
						HasNextPage bool   `json:"hasNextPage"`
						EndCursor   string `json:"endCursor"`
					} `json:"pageInfo"`
				} `json:"membersWithRole"`
			} `json:"organization"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}

	const query = `query($org: String!, $cursor: String) {
  organization(login: $org) {
    membersWithRole(first: 100, after: $cursor) {
      edges {
        role
        node {
          login
          databaseId
          name
          email
        }
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
}`

	var out []Member
	cursor := ""
	for {
		var payload gqlPayload
		vars := map[string]any{
			"org": org,
		}
		if cursor != "" {
			vars["cursor"] = cursor
		} else {
			vars["cursor"] = nil
		}
		if err := c.doGraphQL(ctx, query, vars, &payload); err != nil {
			return nil, err
		}
		if len(payload.Errors) > 0 {
			msg := strings.TrimSpace(payload.Errors[0].Message)
			if msg == "" {
				msg = "unknown error"
			}
			return nil, fmt.Errorf("github graphql error: %s", msg)
		}
		if payload.Data.Organization == nil {
			return nil, fmt.Errorf("github graphql error: organization %q not found", org)
		}

		for _, edge := range payload.Data.Organization.MembersWithRole.Edges {
			role := strings.ToLower(strings.TrimSpace(edge.Role))
			var id int64
			if edge.Node.DatabaseID != nil {
				id = *edge.Node.DatabaseID
			}

			raw, _ := json.Marshal(map[string]any{
				"login": edge.Node.Login,
				"id":    id,
				"role":  role,
				"name":  edge.Node.Name,
				"email": edge.Node.Email,
			})

			out = append(out, Member{
				Login:       edge.Node.Login,
				ID:          id,
				Role:        role,
				Email:       edge.Node.Email,
				DisplayName: edge.Node.Name,
				RawJSON:     raw,
			})
		}

		if !payload.Data.Organization.MembersWithRole.PageInfo.HasNextPage {
			break
		}
		cursor = payload.Data.Organization.MembersWithRole.PageInfo.EndCursor
		if cursor == "" {
			break
		}
	}
	return out, nil
}

func (c *Client) doGraphQL(ctx context.Context, query string, variables map[string]any, out any) error {
	endpoint := c.graphQLEndpoint()
	if endpoint == "" {
		return errors.New("github graphql endpoint is not configured")
	}

	reqBody, err := json.Marshal(map[string]any{
		"query":     query,
		"variables": variables,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(reqBody))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", "open-sspm")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return formatGitHubAPIError("github graphql failed", endpoint, resp, body)
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(body, out); err != nil {
		return err
	}
	return nil
}

func (c *Client) graphQLEndpoint() string {
	if c.BaseURL == "" {
		return ""
	}
	u, err := url.Parse(c.BaseURL)
	if err != nil {
		return ""
	}

	if u.Host == "api.github.com" {
		u.Path = "/graphql"
		u.RawQuery = ""
		u.Fragment = ""
		return u.String()
	}

	if strings.HasSuffix(u.Path, "/api/v3") {
		u.Path = strings.TrimSuffix(u.Path, "/api/v3") + "/api/graphql"
		u.RawQuery = ""
		u.Fragment = ""
		return u.String()
	}

	return ""
}

func (c *Client) doRequest(ctx context.Context, url string) (*http.Response, error) {
	if c.BaseURL == "" || c.Token == "" {
		return nil, errors.New("github base URL and token are required")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", "open-sspm")
	return c.HTTP.Do(req)
}

func formatGitHubAPIError(prefix, reqURL string, resp *http.Response, body []byte) error {
	message := extractGitHubAPIErrorMessage(body)
	details := formatGitHubAPIErrorDetails(reqURL, resp)

	if message != "" && details != "" {
		return fmt.Errorf("%s: %s: %s (%s)", prefix, resp.Status, message, details)
	}
	if message != "" {
		return fmt.Errorf("%s: %s: %s", prefix, resp.Status, message)
	}
	if details != "" {
		return fmt.Errorf("%s: %s (%s)", prefix, resp.Status, details)
	}
	return fmt.Errorf("%s: %s", prefix, resp.Status)
}

func extractGitHubAPIErrorMessage(body []byte) string {
	var payload struct {
		Message          string `json:"message"`
		DocumentationURL string `json:"documentation_url"`
	}
	if err := json.Unmarshal(body, &payload); err == nil && payload.Message != "" {
		if payload.DocumentationURL != "" {
			return fmt.Sprintf("%s (docs: %s)", payload.Message, payload.DocumentationURL)
		}
		return payload.Message
	}

	msg := strings.TrimSpace(string(body))
	if msg == "" {
		return ""
	}
	// Avoid dumping HTML error pages (common when a base URL is misconfigured).
	if strings.HasPrefix(msg, "<!DOCTYPE html") || strings.HasPrefix(msg, "<html") {
		return ""
	}

	// Collapse whitespace and cap length to keep logs readable.
	msg = strings.Join(strings.Fields(msg), " ")
	const maxLen = 300
	if len(msg) > maxLen {
		msg = msg[:maxLen] + "…"
	}
	return msg
}

func formatGitHubAPIErrorDetails(reqURL string, resp *http.Response) string {
	var parts []string

	if v := safeGitHubURL(reqURL); v != "" {
		parts = append(parts, "url="+v)
	}
	if v := resp.Header.Get("X-GitHub-Request-Id"); v != "" {
		parts = append(parts, "request_id="+v)
	}
	if v := resp.Header.Get("X-GitHub-SSO"); v != "" {
		parts = append(parts, "github_sso="+v)
	}
	if v := resp.Header.Get("X-OAuth-Scopes"); v != "" {
		parts = append(parts, "oauth_scopes="+v)
	}
	if v := resp.Header.Get("X-Accepted-OAuth-Scopes"); v != "" {
		parts = append(parts, "accepted_scopes="+v)
	}
	if v := resp.Header.Get("X-Accepted-GitHub-Permissions"); v != "" {
		parts = append(parts, "accepted_permissions="+v)
	}
	if v := resp.Header.Get("X-RateLimit-Remaining"); v != "" {
		parts = append(parts, "rate_remaining="+v)
	}
	if v := resp.Header.Get("X-RateLimit-Reset"); v != "" {
		parts = append(parts, "rate_reset="+v)
	}
	if v := resp.Header.Get("Retry-After"); v != "" {
		parts = append(parts, "retry_after="+v)
	}

	return strings.Join(parts, ", ")
}

func safeGitHubURL(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	if u.RawQuery != "" {
		return u.Scheme + "://" + u.Host + u.Path + "?" + u.RawQuery
	}
	return u.Scheme + "://" + u.Host + u.Path
}

func parseNextLink(linkHeader string) string {
	if linkHeader == "" {
		return ""
	}
	parts := strings.Split(linkHeader, ",")
	for _, part := range parts {
		if !strings.Contains(part, "rel=\"next\"") {
			continue
		}
		start := strings.Index(part, "<")
		end := strings.Index(part, ">")
		if start >= 0 && end > start {
			return strings.TrimSpace(part[start+1 : end])
		}
	}
	return ""
}

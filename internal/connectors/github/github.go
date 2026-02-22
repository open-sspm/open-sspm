package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const defaultTimeout = 120 * time.Second
const maxRetries = 3

const maxRetryAfter = 30 * time.Second

var ErrDatasetUnavailable = errors.New("github dataset unavailable")

type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

type Member struct {
	Login       string
	ID          int64
	Role        string
	AccountType string
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

type Repository struct {
	ID            int64
	Name          string
	FullName      string
	Private       bool
	Archived      bool
	Disabled      bool
	DefaultBranch string
	CreatedAtRaw  string
	UpdatedAtRaw  string
	PushedAtRaw   string
	RawJSON       []byte
}

type DeployKey struct {
	ID            int64
	Key           string
	Title         string
	ReadOnly      bool
	Verified      bool
	AddedBy       string
	CreatedAtRaw  string
	LastUsedAtRaw string
	RawJSON       []byte
}

type AppInstallation struct {
	ID                  int64
	AppID               int64
	AppSlug             string
	AppName             string
	AccountLogin        string
	AccountType         string
	AccountID           int64
	RepositorySelection string
	Permissions         map[string]string
	CreatedAtRaw        string
	UpdatedAtRaw        string
	SuspendedAtRaw      string
	RawJSON             []byte
}

type PersonalAccessTokenRequest struct {
	ID                  int64
	TokenID             int64
	TokenName           string
	Status              string
	OwnerLogin          string
	OwnerID             int64
	RepositorySelection string
	Permissions         map[string]string
	CreatedAtRaw        string
	UpdatedAtRaw        string
	LastUsedAtRaw       string
	ExpiresAtRaw        string
	ReviewerLogin       string
	ReviewerID          int64
	ReviewedAtRaw       string
	RawJSON             []byte
}

type PersonalAccessToken struct {
	ID                  int64
	Name                string
	Status              string
	OwnerLogin          string
	OwnerID             int64
	RepositorySelection string
	Permissions         map[string]string
	CreatedAtRaw        string
	UpdatedAtRaw        string
	LastUsedAtRaw       string
	ExpiresAtRaw        string
	Expired             bool
	Revoked             bool
	ReviewerLogin       string
	ReviewerID          int64
	ReviewedAtRaw       string
	RawJSON             []byte
}

type AuditLogEvent struct {
	DocumentID        string `json:"_document_id"`
	ID                string `json:"id"`
	Action            string `json:"action"`
	Actor             string `json:"actor"`
	User              string `json:"user"`
	CreatedAtRaw      string `json:"@timestamp"`
	Repository        string `json:"repository"`
	Repo              string `json:"repo"`
	RequestID         string `json:"request_id"`
	PATRequestID      string `json:"pat_request_id"`
	PersonalTokenID   string `json:"personal_access_token_id"`
	KeyID             string `json:"key_id"`
	DeployKeyID       string `json:"deploy_key_id"`
	TokenID           string `json:"token_id"`
	Fingerprint       string `json:"fingerprint"`
	OperationType     string `json:"operation_type"`
	ProgrammaticActor string `json:"programmatic_access_type"`
	RawJSON           []byte `json:"-"`
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
		HTTP:    &http.Client{Timeout: defaultTimeout},
	}, nil
}

func (c *Client) httpClient() (*http.Client, error) {
	if c.BaseURL == "" || c.Token == "" {
		return nil, errors.New("github base URL and token are required")
	}
	if c.HTTP == nil {
		return &http.Client{Timeout: defaultTimeout}, nil
	}
	if c.HTTP.Timeout > 0 {
		return c.HTTP, nil
	}
	copy := *c.HTTP
	copy.Timeout = defaultTimeout
	return &copy, nil
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
				Type  string `json:"type"`
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
				AccountType: u.Type,
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

func (c *Client) ListOrgRepos(ctx context.Context, org string) ([]Repository, error) {
	url := fmt.Sprintf("%s/orgs/%s/repos?per_page=100&type=all", c.BaseURL, org)
	var out []Repository
	for url != "" {
		rawItems, next, err := c.getRawPage(ctx, url)
		if err != nil {
			return nil, err
		}
		for _, raw := range rawItems {
			var repo struct {
				ID            int64  `json:"id"`
				Name          string `json:"name"`
				FullName      string `json:"full_name"`
				Private       bool   `json:"private"`
				Archived      bool   `json:"archived"`
				Disabled      bool   `json:"disabled"`
				DefaultBranch string `json:"default_branch"`
				CreatedAtRaw  string `json:"created_at"`
				UpdatedAtRaw  string `json:"updated_at"`
				PushedAtRaw   string `json:"pushed_at"`
			}
			if err := json.Unmarshal(raw, &repo); err != nil {
				return nil, err
			}
			out = append(out, Repository{
				ID:            repo.ID,
				Name:          repo.Name,
				FullName:      repo.FullName,
				Private:       repo.Private,
				Archived:      repo.Archived,
				Disabled:      repo.Disabled,
				DefaultBranch: repo.DefaultBranch,
				CreatedAtRaw:  repo.CreatedAtRaw,
				UpdatedAtRaw:  repo.UpdatedAtRaw,
				PushedAtRaw:   repo.PushedAtRaw,
				RawJSON:       raw,
			})
		}
		url = next
	}
	return out, nil
}

func (c *Client) ListRepoDeployKeys(ctx context.Context, org, repo string) ([]DeployKey, error) {
	url := fmt.Sprintf("%s/repos/%s/%s/keys?per_page=100", c.BaseURL, org, repo)
	var out []DeployKey
	for url != "" {
		rawItems, next, err := c.getRawPage(ctx, url)
		if err != nil {
			return nil, err
		}
		for _, raw := range rawItems {
			var item struct {
				ID            int64  `json:"id"`
				Key           string `json:"key"`
				Title         string `json:"title"`
				ReadOnly      bool   `json:"read_only"`
				Verified      bool   `json:"verified"`
				AddedBy       string `json:"added_by"`
				CreatedAtRaw  string `json:"created_at"`
				LastUsedAtRaw string `json:"last_used"`
			}
			if err := json.Unmarshal(raw, &item); err != nil {
				return nil, err
			}
			out = append(out, DeployKey{
				ID:            item.ID,
				Key:           item.Key,
				Title:         item.Title,
				ReadOnly:      item.ReadOnly,
				Verified:      item.Verified,
				AddedBy:       item.AddedBy,
				CreatedAtRaw:  item.CreatedAtRaw,
				LastUsedAtRaw: item.LastUsedAtRaw,
				RawJSON:       raw,
			})
		}
		url = next
	}
	return out, nil
}

func (c *Client) ListOrgPersonalAccessTokenRequests(ctx context.Context, org string) ([]PersonalAccessTokenRequest, error) {
	url := fmt.Sprintf("%s/orgs/%s/personal-access-token-requests?per_page=100", c.BaseURL, org)
	var out []PersonalAccessTokenRequest

	for url != "" {
		resp, err := c.doRequest(ctx, url)
		if err != nil {
			return nil, err
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}

		if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("%w: personal-access-token-requests endpoint unavailable (%s)", ErrDatasetUnavailable, resp.Status)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, formatGitHubAPIError("github personal access token requests api failed", url, resp, body)
		}

		var rawItems []json.RawMessage
		if err := json.Unmarshal(body, &rawItems); err != nil {
			return nil, err
		}
		for _, raw := range rawItems {
			item, err := parseGitHubPATRequest(raw)
			if err != nil {
				return nil, err
			}
			out = append(out, item)
		}

		url = parseNextLink(resp.Header.Get("Link"))
	}

	return out, nil
}

func (c *Client) ListOrgPersonalAccessTokens(ctx context.Context, org string) ([]PersonalAccessToken, error) {
	url := fmt.Sprintf("%s/orgs/%s/personal-access-tokens?per_page=100", c.BaseURL, org)
	var out []PersonalAccessToken

	for url != "" {
		resp, err := c.doRequest(ctx, url)
		if err != nil {
			return nil, err
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}

		if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("%w: personal-access-tokens endpoint unavailable (%s)", ErrDatasetUnavailable, resp.Status)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, formatGitHubAPIError("github personal access tokens api failed", url, resp, body)
		}

		var rawItems []json.RawMessage
		if err := json.Unmarshal(body, &rawItems); err != nil {
			return nil, err
		}
		for _, raw := range rawItems {
			item, err := parseGitHubPAT(raw)
			if err != nil {
				return nil, err
			}
			out = append(out, item)
		}

		url = parseNextLink(resp.Header.Get("Link"))
	}

	return out, nil
}

func (c *Client) ListOrgInstallations(ctx context.Context, org string) ([]AppInstallation, error) {
	url := fmt.Sprintf("%s/orgs/%s/installations?per_page=100", c.BaseURL, org)
	var out []AppInstallation

	for url != "" {
		resp, err := c.doRequest(ctx, url)
		if err != nil {
			return nil, err
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}

		if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("%w: org installations endpoint unavailable (%s)", ErrDatasetUnavailable, resp.Status)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, formatGitHubAPIError("github installations api failed", url, resp, body)
		}

		var payload struct {
			Installations []json.RawMessage `json:"installations"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			return nil, err
		}

		for _, raw := range payload.Installations {
			var item struct {
				ID                  int64             `json:"id"`
				AppID               int64             `json:"app_id"`
				AppSlug             string            `json:"app_slug"`
				AppName             string            `json:"app_name"`
				Account             map[string]any    `json:"account"`
				RepositorySelection string            `json:"repository_selection"`
				Permissions         map[string]string `json:"permissions"`
				CreatedAtRaw        string            `json:"created_at"`
				UpdatedAtRaw        string            `json:"updated_at"`
				SuspendedAtRaw      string            `json:"suspended_at"`
			}
			if err := json.Unmarshal(raw, &item); err != nil {
				return nil, err
			}
			accountLogin, _ := item.Account["login"].(string)
			accountType, _ := item.Account["type"].(string)
			var accountID int64
			switch id := item.Account["id"].(type) {
			case float64:
				accountID = int64(id)
			case int64:
				accountID = id
			}

			out = append(out, AppInstallation{
				ID:                  item.ID,
				AppID:               item.AppID,
				AppSlug:             item.AppSlug,
				AppName:             item.AppName,
				AccountLogin:        accountLogin,
				AccountType:         accountType,
				AccountID:           accountID,
				RepositorySelection: item.RepositorySelection,
				Permissions:         item.Permissions,
				CreatedAtRaw:        item.CreatedAtRaw,
				UpdatedAtRaw:        item.UpdatedAtRaw,
				SuspendedAtRaw:      item.SuspendedAtRaw,
				RawJSON:             raw,
			})
		}

		url = parseNextLink(resp.Header.Get("Link"))
	}

	return out, nil
}

func (c *Client) ListOrgAuditLog(ctx context.Context, org string) ([]AuditLogEvent, error) {
	url := fmt.Sprintf("%s/orgs/%s/audit-log?per_page=100", c.BaseURL, org)
	var out []AuditLogEvent

	for url != "" {
		resp, err := c.doRequest(ctx, url)
		if err != nil {
			return nil, err
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}

		if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("%w: org audit log endpoint unavailable (%s)", ErrDatasetUnavailable, resp.Status)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, formatGitHubAPIError("github org audit log failed", url, resp, body)
		}

		var rawItems []json.RawMessage
		if err := json.Unmarshal(body, &rawItems); err != nil {
			return nil, err
		}
		for _, raw := range rawItems {
			var event AuditLogEvent
			if err := json.Unmarshal(raw, &event); err != nil {
				return nil, err
			}
			event.RawJSON = raw
			out = append(out, event)
		}

		url = parseNextLink(resp.Header.Get("Link"))
	}

	return out, nil
}

func parseGitHubPATRequest(raw json.RawMessage) (PersonalAccessTokenRequest, error) {
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return PersonalAccessTokenRequest{}, err
	}

	owner := firstMap(payload["owner"], payload["requester"], payload["actor"])
	reviewer := firstMap(payload["reviewer"], payload["approved_by"])

	id := asInt64(payload["id"])
	tokenID := firstNonZeroInt64(asInt64(payload["token_id"]), asInt64(payload["pat_id"]), asInt64(payload["personal_access_token_id"]))
	if id == 0 && tokenID != 0 {
		id = tokenID
	}

	return PersonalAccessTokenRequest{
		ID:                  id,
		TokenID:             tokenID,
		TokenName:           firstStringValue(payload["token_name"], payload["token_display_name"], payload["name"]),
		Status:              firstStringValue(payload["status"], payload["request_state"], payload["decision"]),
		OwnerLogin:          firstStringValue(owner["login"], owner["name"], payload["owner"]),
		OwnerID:             asInt64(owner["id"]),
		RepositorySelection: firstStringValue(payload["repository_selection"], payload["repository_access"]),
		Permissions:         asStringMap(payload["permissions"]),
		CreatedAtRaw:        firstStringValue(payload["created_at"], payload["requested_at"]),
		UpdatedAtRaw:        firstStringValue(payload["updated_at"]),
		LastUsedAtRaw:       firstStringValue(payload["token_last_used_at"], payload["last_used_at"]),
		ExpiresAtRaw:        firstStringValue(payload["token_expires_at"], payload["expires_at"]),
		ReviewerLogin:       firstStringValue(reviewer["login"], reviewer["name"]),
		ReviewerID:          asInt64(reviewer["id"]),
		ReviewedAtRaw:       firstStringValue(payload["reviewed_at"], payload["approved_at"]),
		RawJSON:             raw,
	}, nil
}

func parseGitHubPAT(raw json.RawMessage) (PersonalAccessToken, error) {
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return PersonalAccessToken{}, err
	}

	owner := firstMap(payload["owner"], payload["actor"], payload["user"])
	reviewer := firstMap(payload["reviewer"], payload["approved_by"])

	id := asInt64(payload["id"])
	if id == 0 {
		id = firstNonZeroInt64(asInt64(payload["token_id"]), asInt64(payload["pat_id"]), asInt64(payload["personal_access_token_id"]))
	}

	return PersonalAccessToken{
		ID:                  id,
		Name:                firstStringValue(payload["name"], payload["token_name"], payload["token_display_name"]),
		Status:              firstStringValue(payload["status"], payload["state"]),
		OwnerLogin:          firstStringValue(owner["login"], owner["name"], payload["owner"]),
		OwnerID:             asInt64(owner["id"]),
		RepositorySelection: firstStringValue(payload["repository_selection"], payload["repository_access"]),
		Permissions:         asStringMap(payload["permissions"]),
		CreatedAtRaw:        firstStringValue(payload["created_at"]),
		UpdatedAtRaw:        firstStringValue(payload["updated_at"]),
		LastUsedAtRaw:       firstStringValue(payload["last_used_at"], payload["token_last_used_at"]),
		ExpiresAtRaw:        firstStringValue(payload["expires_at"], payload["token_expires_at"]),
		Expired:             asBool(payload["token_expired"]) || asBool(payload["expired"]),
		Revoked:             asBool(payload["revoked"]),
		ReviewerLogin:       firstStringValue(reviewer["login"], reviewer["name"]),
		ReviewerID:          asInt64(reviewer["id"]),
		ReviewedAtRaw:       firstStringValue(payload["reviewed_at"], payload["approved_at"]),
		RawJSON:             raw,
	}, nil
}

func firstStringValue(values ...any) string {
	for _, value := range values {
		s := asString(value)
		if s != "" {
			return s
		}
	}
	return ""
}

func firstNonZeroInt64(values ...int64) int64 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func firstMap(values ...any) map[string]any {
	for _, value := range values {
		m := asMap(value)
		if len(m) > 0 {
			return m
		}
	}
	return map[string]any{}
}

func asMap(value any) map[string]any {
	switch v := value.(type) {
	case map[string]any:
		return v
	default:
		return map[string]any{}
	}
}

func asStringMap(value any) map[string]string {
	out := map[string]string{}

	switch v := value.(type) {
	case map[string]string:
		for key, val := range v {
			key = strings.TrimSpace(key)
			val = strings.TrimSpace(val)
			if key == "" || val == "" {
				continue
			}
			out[key] = val
		}
	case map[string]any:
		for key, rawVal := range v {
			key = strings.TrimSpace(key)
			val := asString(rawVal)
			if key == "" || val == "" {
				continue
			}
			out[key] = val
		}
	}

	return out
}

func asString(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case json.Number:
		return strings.TrimSpace(v.String())
	case float64:
		return strings.TrimSpace(strconv.FormatInt(int64(v), 10))
	case int64:
		return strings.TrimSpace(strconv.FormatInt(v, 10))
	case int:
		return strings.TrimSpace(strconv.Itoa(v))
	case bool:
		return strings.TrimSpace(strconv.FormatBool(v))
	default:
		return ""
	}
}

func asInt64(value any) int64 {
	switch v := value.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	case json.Number:
		if n, err := v.Int64(); err == nil {
			return n
		}
	case string:
		if n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
			return n
		}
	}
	return 0
}

func asBool(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "1", "t", "true", "yes", "y":
			return true
		}
	case int:
		return v != 0
	case int64:
		return v != 0
	case float64:
		return v != 0
	}
	return false
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
							Typename   string `json:"__typename"`
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
          __typename
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
				"type":  edge.Node.Typename,
				"name":  edge.Node.Name,
				"email": edge.Node.Email,
			})

			out = append(out, Member{
				Login:       edge.Node.Login,
				ID:          id,
				Role:        role,
				AccountType: edge.Node.Typename,
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
	httpClient, err := c.httpClient()
	if err != nil {
		return err
	}

	reqBody, err := json.Marshal(map[string]any{
		"query":     query,
		"variables": variables,
	})
	if err != nil {
		return err
	}

	var resp *http.Response
	var body []byte
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
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

		resp, err = httpClient.Do(req)
		if err != nil {
			if attempt < maxRetries && shouldRetryError(ctx, err) {
				if err := sleepWithContext(ctx, backoffDelay(attempt)); err != nil {
					return err
				}
				continue
			}
			return err
		}

		body, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return err
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			if attempt < maxRetries && shouldRetryStatus(resp) {
				if err := sleepWithContext(ctx, retryDelay(resp, attempt)); err != nil {
					return err
				}
				continue
			}
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
	return formatGitHubAPIError("github graphql failed", endpoint, resp, body)
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

	if before, ok := strings.CutSuffix(u.Path, "/api/v3"); ok {
		u.Path = before + "/api/graphql"
		u.RawQuery = ""
		u.Fragment = ""
		return u.String()
	}

	return ""
}

func (c *Client) doRequest(ctx context.Context, url string) (*http.Response, error) {
	httpClient, err := c.httpClient()
	if err != nil {
		return nil, err
	}

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+c.Token)
		req.Header.Set("Accept", "application/vnd.github+json")
		req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
		req.Header.Set("User-Agent", "open-sspm")

		resp, err := httpClient.Do(req)
		if err != nil {
			if attempt < maxRetries && shouldRetryError(ctx, err) {
				if err := sleepWithContext(ctx, backoffDelay(attempt)); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return resp, nil
		}
		if attempt < maxRetries && shouldRetryStatus(resp) {
			drainAndClose(resp.Body)
			if err := sleepWithContext(ctx, retryDelay(resp, attempt)); err != nil {
				return nil, err
			}
			continue
		}
		return resp, nil
	}
	return nil, errors.New("github request failed after retries")
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
	parts := strings.SplitSeq(linkHeader, ",")
	for part := range parts {
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

func shouldRetryStatus(resp *http.Response) bool {
	if resp == nil {
		return false
	}
	switch resp.StatusCode {
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func shouldRetryError(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if ctx != nil && ctx.Err() != nil {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}
	return false
}

func retryDelay(resp *http.Response, attempt int) time.Duration {
	if resp == nil {
		return backoffDelay(attempt)
	}
	if d := retryAfter(resp); d > 0 {
		return d
	}
	return backoffDelay(attempt)
}

func retryAfter(resp *http.Response) time.Duration {
	if resp == nil {
		return 0
	}
	v := strings.TrimSpace(resp.Header.Get("Retry-After"))
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		d := time.Duration(secs) * time.Second
		if d > maxRetryAfter {
			return maxRetryAfter
		}
		return d
	}
	if t, err := http.ParseTime(v); err == nil {
		d := time.Until(t)
		if d < 0 {
			return 0
		}
		if d > maxRetryAfter {
			return maxRetryAfter
		}
		return d
	}
	return 0
}

func backoffDelay(attempt int) time.Duration {
	if attempt < 0 {
		return 0
	}
	d := 200 * time.Millisecond
	for range attempt {
		d *= 2
		if d >= 5*time.Second {
			return 5 * time.Second
		}
	}
	return d
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func drainAndClose(r io.ReadCloser) {
	if r == nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(r, 1<<20))
	_ = r.Close()
}

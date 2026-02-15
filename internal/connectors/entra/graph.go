package entra

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultTimeout     = 120 * time.Second
	maxRetriesOn429    = 5
	maxErrorBodySize   = 1 << 20 // 1 MiB
	directoryAuditsTop = "200"
	defaultGraphBase   = "https://graph.microsoft.com/v1.0"
	defaultAuthority   = "https://login.microsoftonline.com"
	defaultTokenScope  = "https://graph.microsoft.com/.default"
	tokenExpiryLeeway  = 30 * time.Second
)

type Options struct {
	HTTPClient       *http.Client
	GraphBaseURL     string
	AuthorityBaseURL string
}

type Client struct {
	tenantID     string
	clientID     string
	clientSecret string

	http          *http.Client
	graphBaseURL  string
	authorityBase string

	mu                sync.Mutex
	cachedToken       string
	cachedTokenExpiry time.Time
}

type User struct {
	ID                 string   `json:"id"`
	DisplayName        string   `json:"displayName"`
	Mail               string   `json:"mail"`
	UserPrincipalName  string   `json:"userPrincipalName"`
	OtherMails         []string `json:"otherMails"`
	ProxyAddresses     []string `json:"proxyAddresses"`
	UserType           string   `json:"userType"`
	AccountEnabled     *bool    `json:"accountEnabled"`
	CreatedDateTimeRaw string   `json:"createdDateTime"`
	RawJSON            []byte   `json:"-"`
}

type PasswordCredential struct {
	KeyID            string `json:"keyId"`
	DisplayName      string `json:"displayName"`
	StartDateTimeRaw string `json:"startDateTime"`
	EndDateTimeRaw   string `json:"endDateTime"`
	Hint             string `json:"hint"`
}

type KeyCredential struct {
	KeyID               string `json:"keyId"`
	DisplayName         string `json:"displayName"`
	Type                string `json:"type"`
	Usage               string `json:"usage"`
	StartDateTimeRaw    string `json:"startDateTime"`
	EndDateTimeRaw      string `json:"endDateTime"`
	CustomKeyIdentifier string `json:"customKeyIdentifier"`
}

type VerifiedPublisher struct {
	DisplayName string `json:"displayName"`
}

type Application struct {
	ID                  string               `json:"id"`
	AppID               string               `json:"appId"`
	DisplayName         string               `json:"displayName"`
	PublisherDomain     string               `json:"publisherDomain"`
	VerifiedPublisher   VerifiedPublisher    `json:"verifiedPublisher"`
	CreatedDateTimeRaw  string               `json:"createdDateTime"`
	PasswordCredentials []PasswordCredential `json:"passwordCredentials"`
	KeyCredentials      []KeyCredential      `json:"keyCredentials"`
	RawJSON             []byte               `json:"-"`
}

type ServicePrincipal struct {
	ID                   string               `json:"id"`
	AppID                string               `json:"appId"`
	DisplayName          string               `json:"displayName"`
	PublisherName        string               `json:"publisherName"`
	AccountEnabled       *bool                `json:"accountEnabled"`
	ServicePrincipalType string               `json:"servicePrincipalType"`
	CreatedDateTimeRaw   string               `json:"createdDateTime"`
	PasswordCredentials  []PasswordCredential `json:"passwordCredentials"`
	KeyCredentials       []KeyCredential      `json:"keyCredentials"`
	RawJSON              []byte               `json:"-"`
}

type DirectoryOwner struct {
	ID                string `json:"id"`
	ODataType         string `json:"@odata.type"`
	DisplayName       string `json:"displayName"`
	Mail              string `json:"mail"`
	UserPrincipalName string `json:"userPrincipalName"`
	AppID             string `json:"appId"`
	RawJSON           []byte `json:"-"`
}

type DirectoryAuditActorUser struct {
	ID                string `json:"id"`
	DisplayName       string `json:"displayName"`
	UserPrincipalName string `json:"userPrincipalName"`
	IPAddress         string `json:"ipAddress"`
}

type DirectoryAuditActorApp struct {
	AppID              string `json:"appId"`
	DisplayName        string `json:"displayName"`
	ServicePrincipalID string `json:"servicePrincipalId"`
}

type DirectoryAuditInitiatedBy struct {
	User *DirectoryAuditActorUser `json:"user"`
	App  *DirectoryAuditActorApp  `json:"app"`
}

type DirectoryAuditModifiedProperty struct {
	DisplayName string `json:"displayName"`
	OldValue    string `json:"oldValue"`
	NewValue    string `json:"newValue"`
}

type DirectoryAuditTargetResource struct {
	ID                 string                           `json:"id"`
	DisplayName        string                           `json:"displayName"`
	Type               string                           `json:"type"`
	ModifiedProperties []DirectoryAuditModifiedProperty `json:"modifiedProperties"`
}

type DirectoryAuditEvent struct {
	ID                  string                         `json:"id"`
	Category            string                         `json:"category"`
	Result              string                         `json:"result"`
	ActivityDisplayName string                         `json:"activityDisplayName"`
	ActivityDateTimeRaw string                         `json:"activityDateTime"`
	InitiatedBy         DirectoryAuditInitiatedBy      `json:"initiatedBy"`
	TargetResources     []DirectoryAuditTargetResource `json:"targetResources"`
	RawJSON             []byte                         `json:"-"`
}

type SignInEvent struct {
	ID                  string `json:"id"`
	CreatedDateTimeRaw  string `json:"createdDateTime"`
	AppID               string `json:"appId"`
	AppDisplayName      string `json:"appDisplayName"`
	ResourceDisplayName string `json:"resourceDisplayName"`
	UserID              string `json:"userId"`
	UserDisplayName     string `json:"userDisplayName"`
	UserPrincipalName   string `json:"userPrincipalName"`
	RawJSON             []byte `json:"-"`
}

type OAuth2PermissionGrant struct {
	ID                 string `json:"id"`
	ClientID           string `json:"clientId"`
	ConsentType        string `json:"consentType"`
	PrincipalID        string `json:"principalId"`
	ResourceID         string `json:"resourceId"`
	Scope              string `json:"scope"`
	CreatedDateTimeRaw string `json:"createdDateTime"`
	RawJSON            []byte `json:"-"`
}

func New(tenantID, clientID, clientSecret string) (*Client, error) {
	return NewWithOptions(tenantID, clientID, clientSecret, Options{})
}

func NewWithOptions(tenantID, clientID, clientSecret string, opts Options) (*Client, error) {
	tenantID = normalizeGUID(tenantID)
	clientID = normalizeGUID(clientID)
	clientSecret = strings.TrimSpace(clientSecret)

	if tenantID == "" {
		return nil, errors.New("entra tenant id is required")
	}
	if clientID == "" {
		return nil, errors.New("entra client id is required")
	}
	if clientSecret == "" {
		return nil, errors.New("entra client secret is required")
	}

	graphBase := strings.TrimRight(strings.TrimSpace(opts.GraphBaseURL), "/")
	if graphBase == "" {
		graphBase = defaultGraphBase
	}
	authorityBase := strings.TrimRight(strings.TrimSpace(opts.AuthorityBaseURL), "/")
	if authorityBase == "" {
		authorityBase = defaultAuthority
	}

	httpClient := opts.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultTimeout}
	}

	return &Client{
		tenantID:      tenantID,
		clientID:      clientID,
		clientSecret:  clientSecret,
		http:          httpClient,
		graphBaseURL:  graphBase,
		authorityBase: authorityBase,
	}, nil
}

func (c *Client) ListUsers(ctx context.Context) ([]User, error) {
	endpoint, err := c.graphURL("/users", url.Values{
		"$select": []string{"id,displayName,mail,userPrincipalName,otherMails,proxyAddresses,userType,accountEnabled,createdDateTime"},
		"$top":    []string{"999"},
	})
	if err != nil {
		return nil, err
	}

	rawItems, err := c.listPagedRaw(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	out := make([]User, 0, len(rawItems))
	for _, raw := range rawItems {
		var u User
		if err := json.Unmarshal(raw, &u); err != nil {
			return nil, err
		}
		u.RawJSON = raw
		out = append(out, u)
	}
	return out, nil
}

func (c *Client) ListApplications(ctx context.Context) ([]Application, error) {
	endpoint, err := c.graphURL("/applications", url.Values{
		"$select": []string{"id,appId,displayName,publisherDomain,verifiedPublisher,createdDateTime,passwordCredentials,keyCredentials"},
		"$top":    []string{"999"},
	})
	if err != nil {
		return nil, err
	}

	rawItems, err := c.listPagedRaw(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	out := make([]Application, 0, len(rawItems))
	for _, raw := range rawItems {
		var app Application
		if err := json.Unmarshal(raw, &app); err != nil {
			return nil, err
		}
		app.RawJSON = raw
		out = append(out, app)
	}
	return out, nil
}

func (c *Client) ListServicePrincipals(ctx context.Context) ([]ServicePrincipal, error) {
	endpoint, err := c.graphURL("/servicePrincipals", url.Values{
		"$select": []string{"id,appId,displayName,publisherName,accountEnabled,servicePrincipalType,createdDateTime,passwordCredentials,keyCredentials"},
		"$top":    []string{"999"},
	})
	if err != nil {
		return nil, err
	}

	rawItems, err := c.listPagedRaw(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	out := make([]ServicePrincipal, 0, len(rawItems))
	for _, raw := range rawItems {
		var sp ServicePrincipal
		if err := json.Unmarshal(raw, &sp); err != nil {
			return nil, err
		}
		sp.RawJSON = raw
		out = append(out, sp)
	}
	return out, nil
}

func (c *Client) ListApplicationOwners(ctx context.Context, applicationID string) ([]DirectoryOwner, error) {
	applicationID = strings.TrimSpace(applicationID)
	if applicationID == "" {
		return nil, errors.New("application id is required")
	}
	endpoint, err := c.graphURL("/applications/"+url.PathEscape(applicationID)+"/owners", url.Values{
		"$select": []string{"id,displayName,mail,userPrincipalName,appId"},
		"$top":    []string{"999"},
	})
	if err != nil {
		return nil, err
	}
	return c.listOwners(ctx, endpoint)
}

func (c *Client) ListServicePrincipalOwners(ctx context.Context, servicePrincipalID string) ([]DirectoryOwner, error) {
	servicePrincipalID = strings.TrimSpace(servicePrincipalID)
	if servicePrincipalID == "" {
		return nil, errors.New("service principal id is required")
	}
	endpoint, err := c.graphURL("/servicePrincipals/"+url.PathEscape(servicePrincipalID)+"/owners", url.Values{
		"$select": []string{"id,displayName,mail,userPrincipalName,appId"},
		"$top":    []string{"999"},
	})
	if err != nil {
		return nil, err
	}
	return c.listOwners(ctx, endpoint)
}

func (c *Client) ListDirectoryAudits(ctx context.Context, since *time.Time) ([]DirectoryAuditEvent, error) {
	query := url.Values{
		"$select":  []string{"id,category,result,activityDisplayName,activityDateTime,initiatedBy,targetResources"},
		"$orderby": []string{"activityDateTime desc"},
		"$top":     []string{directoryAuditsTop},
	}
	if since != nil && !since.IsZero() {
		query.Set("$filter", "activityDateTime ge "+since.UTC().Format(time.RFC3339))
	}

	endpoint, err := c.graphURL("/auditLogs/directoryAudits", query)
	if err != nil {
		return nil, err
	}

	rawItems, err := c.listPagedRaw(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	out := make([]DirectoryAuditEvent, 0, len(rawItems))
	for _, raw := range rawItems {
		var event DirectoryAuditEvent
		if err := json.Unmarshal(raw, &event); err != nil {
			return nil, err
		}
		event.RawJSON = raw
		out = append(out, event)
	}
	return out, nil
}

func (c *Client) ListSignIns(ctx context.Context, since *time.Time) ([]SignInEvent, error) {
	query := url.Values{
		"$select":  []string{"id,createdDateTime,appId,appDisplayName,resourceDisplayName,userId,userDisplayName,userPrincipalName"},
		"$orderby": []string{"createdDateTime desc"},
		"$top":     []string{"999"},
	}
	if since != nil && !since.IsZero() {
		query.Set("$filter", "createdDateTime ge "+since.UTC().Format(time.RFC3339))
	}

	endpoint, err := c.graphURL("/auditLogs/signIns", query)
	if err != nil {
		return nil, err
	}

	rawItems, err := c.listPagedRaw(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	out := make([]SignInEvent, 0, len(rawItems))
	for _, raw := range rawItems {
		var event SignInEvent
		if err := json.Unmarshal(raw, &event); err != nil {
			return nil, err
		}
		event.RawJSON = raw
		out = append(out, event)
	}
	return out, nil
}

func (c *Client) ListOAuth2PermissionGrants(ctx context.Context) ([]OAuth2PermissionGrant, error) {
	endpoint, err := c.graphURL("/oauth2PermissionGrants", url.Values{
		"$select": []string{"id,clientId,consentType,principalId,resourceId,scope"},
		"$top":    []string{"999"},
	})
	if err != nil {
		return nil, err
	}

	rawItems, err := c.listPagedRaw(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	out := make([]OAuth2PermissionGrant, 0, len(rawItems))
	for _, raw := range rawItems {
		var grant OAuth2PermissionGrant
		if err := json.Unmarshal(raw, &grant); err != nil {
			return nil, err
		}
		grant.RawJSON = raw
		out = append(out, grant)
	}
	return out, nil
}

func (c *Client) listOwners(ctx context.Context, endpoint string) ([]DirectoryOwner, error) {
	rawItems, err := c.listPagedRaw(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	out := make([]DirectoryOwner, 0, len(rawItems))
	for _, raw := range rawItems {
		var owner DirectoryOwner
		if err := json.Unmarshal(raw, &owner); err != nil {
			return nil, err
		}
		owner.RawJSON = raw
		out = append(out, owner)
	}
	return out, nil
}

func (c *Client) listPagedRaw(ctx context.Context, endpoint string) ([]json.RawMessage, error) {
	var out []json.RawMessage
	for {
		body, err := c.get(ctx, endpoint)
		if err != nil {
			return nil, err
		}
		var page struct {
			Value    []json.RawMessage `json:"value"`
			NextLink string            `json:"@odata.nextLink"`
		}
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, err
		}
		out = append(out, page.Value...)

		next := strings.TrimSpace(page.NextLink)
		if next == "" {
			break
		}
		endpoint = next
	}
	return out, nil
}

func (c *Client) graphURL(path string, query url.Values) (string, error) {
	base := strings.TrimRight(strings.TrimSpace(c.graphBaseURL), "/")
	if base == "" {
		return "", errors.New("entra graph base url is required")
	}
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	u.Path = strings.TrimRight(u.Path, "/") + path
	if query != nil {
		u.RawQuery = query.Encode()
	}
	u.Fragment = ""
	return u.String(), nil
}

func (c *Client) get(ctx context.Context, endpoint string) ([]byte, error) {
	token, err := c.token(ctx)
	if err != nil {
		return nil, err
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetriesOn429; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "open-sspm")

		resp, err := c.http.Do(req)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusGatewayTimeout {
			body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodySize))
			resp.Body.Close()
			if readErr != nil {
				return nil, readErr
			}
			lastErr = formatGraphAPIError("graph api throttled", endpoint, resp, body)
			if attempt == maxRetriesOn429 {
				return nil, lastErr
			}
			wait, ok := retryAfterDuration(resp.Header.Get("Retry-After"))
			if !ok {
				wait = retryBackoff(attempt)
			}
			if err := sleep(ctx, wait); err != nil {
				return nil, err
			}
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodySize))
			resp.Body.Close()
			if readErr != nil {
				return nil, readErr
			}
			return nil, formatGraphAPIError("graph api failed", endpoint, resp, body)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		return body, nil
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, errors.New("entra request failed")
}

func (c *Client) token(ctx context.Context) (string, error) {
	now := time.Now()

	c.mu.Lock()
	cached := c.cachedToken
	exp := c.cachedTokenExpiry
	c.mu.Unlock()

	if strings.TrimSpace(cached) != "" && exp.After(now.Add(tokenExpiryLeeway)) {
		return cached, nil
	}

	accessToken, expiresAt, err := c.fetchToken(ctx)
	if err != nil {
		return "", err
	}

	c.mu.Lock()
	c.cachedToken = accessToken
	c.cachedTokenExpiry = expiresAt
	c.mu.Unlock()

	return accessToken, nil
}

func (c *Client) fetchToken(ctx context.Context) (string, time.Time, error) {
	authority := strings.TrimRight(strings.TrimSpace(c.authorityBase), "/")
	if authority == "" {
		return "", time.Time{}, errors.New("entra authority base url is required")
	}
	u, err := url.Parse(authority)
	if err != nil {
		return "", time.Time{}, err
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/" + url.PathEscape(c.tenantID) + "/oauth2/v2.0/token"
	u.RawQuery = ""
	u.Fragment = ""

	form := url.Values{}
	form.Set("client_id", c.clientID)
	form.Set("client_secret", c.clientSecret)
	form.Set("scope", defaultTokenScope)
	form.Set("grant_type", "client_credentials")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), strings.NewReader(form.Encode()))
	if err != nil {
		return "", time.Time{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "open-sspm")

	resp, err := c.http.Do(req)
	if err != nil {
		return "", time.Time{}, err
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodySize))
	resp.Body.Close()
	if readErr != nil {
		return "", time.Time{}, readErr
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", time.Time{}, formatGraphAPIError("entra token request failed", u.String(), resp, body)
	}

	var payload struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   any    `json:"expires_in"`
		TokenType   string `json:"token_type"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", time.Time{}, err
	}

	accessToken := strings.TrimSpace(payload.AccessToken)
	if accessToken == "" {
		return "", time.Time{}, errors.New("entra token response missing access_token")
	}

	expiresIn, ok := parseExpiresInSeconds(payload.ExpiresIn)
	if !ok {
		expiresIn = 3600
	}
	expiresAt := time.Now().Add(time.Duration(expiresIn) * time.Second)
	return accessToken, expiresAt, nil
}

func parseExpiresInSeconds(v any) (int, bool) {
	switch t := v.(type) {
	case float64:
		if t <= 0 {
			return 0, false
		}
		return int(t), true
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(t))
		if err != nil || n <= 0 {
			return 0, false
		}
		return n, true
	default:
		return 0, false
	}
}

func retryAfterDuration(header string) (time.Duration, bool) {
	header = strings.TrimSpace(header)
	if header == "" {
		return 0, false
	}
	secs, err := strconv.Atoi(header)
	if err != nil || secs < 0 {
		return 0, false
	}
	return time.Duration(secs) * time.Second, true
}

func retryBackoff(attempt int) time.Duration {
	if attempt <= 0 {
		return time.Second
	}
	wait := time.Second * time.Duration(1<<attempt)
	const max = 30 * time.Second
	if wait > max {
		wait = max
	}
	return wait
}

func sleep(ctx context.Context, d time.Duration) error {
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

func normalizeGUID(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.TrimPrefix(s, "{")
	s = strings.TrimSuffix(s, "}")
	return strings.TrimSpace(s)
}

func formatGraphAPIError(prefix, reqURL string, resp *http.Response, body []byte) error {
	message := extractGraphAPIErrorMessage(body)
	details := formatGraphAPIErrorDetails(reqURL, resp)

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

func extractGraphAPIErrorMessage(body []byte) string {
	var payload struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &payload); err == nil {
		msg := strings.TrimSpace(payload.Error.Message)
		code := strings.TrimSpace(payload.Error.Code)
		if msg != "" && code != "" {
			return code + ": " + msg
		}
		if msg != "" {
			return msg
		}
		if code != "" {
			return code
		}
	}

	msg := strings.TrimSpace(string(body))
	if msg == "" {
		return ""
	}
	msg = strings.Join(strings.Fields(msg), " ")
	const maxLen = 300
	if len(msg) > maxLen {
		msg = msg[:maxLen] + "…"
	}
	return msg
}

func formatGraphAPIErrorDetails(reqURL string, resp *http.Response) string {
	var parts []string
	if v := safeURL(reqURL); v != "" {
		parts = append(parts, "url="+v)
	}
	if v := strings.TrimSpace(resp.Header.Get("request-id")); v != "" {
		parts = append(parts, "request_id="+v)
	}
	if v := strings.TrimSpace(resp.Header.Get("client-request-id")); v != "" {
		parts = append(parts, "client_request_id="+v)
	}
	if v := strings.TrimSpace(resp.Header.Get("Retry-After")); v != "" {
		parts = append(parts, "retry_after="+v)
	}
	return strings.Join(parts, ", ")
}

func safeURL(raw string) string {
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

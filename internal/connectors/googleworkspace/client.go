package googleworkspace

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

const (
	defaultGoogleDirectoryBaseURL = "https://admin.googleapis.com/admin/directory/v1"
	defaultGoogleReportsBaseURL   = "https://admin.googleapis.com/admin/reports/v1"
	defaultGoogleTokenURL         = "https://oauth2.googleapis.com/token"
	defaultGoogleIAMBaseURL       = "https://iamcredentials.googleapis.com/v1"
	defaultGoogleTimeout          = 120 * time.Second
	googleTokenLeeway             = 30 * time.Second
	googleMaxRetries              = 5
)

var googleWorkspaceDefaultScopes = []string{
	"https://www.googleapis.com/auth/admin.directory.user.readonly",
	"https://www.googleapis.com/auth/admin.directory.group.readonly",
	"https://www.googleapis.com/auth/admin.directory.group.member.readonly",
	"https://www.googleapis.com/auth/admin.directory.rolemanagement.readonly",
	"https://www.googleapis.com/auth/admin.directory.user.security",
	"https://www.googleapis.com/auth/admin.reports.audit.readonly",
}

type ClientOptions struct {
	HTTPClient            *http.Client
	DirectoryBaseURL      string
	ReportsBaseURL        string
	TokenURL              string
	IAMCredentialsBaseURL string
	Scopes                []string
	ADCTokenSource        oauth2.TokenSource
}

type Client struct {
	cfg configstore.GoogleWorkspaceConfig

	http               *http.Client
	directoryBaseURL   string
	reportsBaseURL     string
	tokenURL           string
	iamCredentialsBase string
	scopes             []string

	adcTokenSource oauth2.TokenSource

	mu              sync.Mutex
	cachedToken     string
	cachedTokenExp  time.Time
	parsedPrivatePK *rsa.PrivateKey
	saClientEmail   string
}

type WorkspaceUser struct {
	ID           string `json:"id"`
	PrimaryEmail string `json:"primaryEmail"`
	Suspended    bool   `json:"suspended"`
	Name         struct {
		FullName string `json:"fullName"`
	} `json:"name"`
	RawJSON []byte `json:"-"`
}

type WorkspaceGroup struct {
	ID      string `json:"id"`
	Email   string `json:"email"`
	Name    string `json:"name"`
	RawJSON []byte `json:"-"`
}

type WorkspaceGroupMember struct {
	ID      string `json:"id"`
	Email   string `json:"email"`
	Type    string `json:"type"`
	Role    string `json:"role"`
	Status  string `json:"status"`
	RawJSON []byte `json:"-"`
}

type WorkspaceAdminRole struct {
	RoleID       string `json:"roleId"`
	RoleName     string `json:"roleName"`
	IsSuperAdmin bool   `json:"isSuperAdminRole"`
	RawJSON      []byte `json:"-"`
}

type WorkspaceAdminRoleAssignment struct {
	RoleID       string `json:"roleId"`
	AssignedTo   string `json:"assignedTo"`
	AssigneeType string `json:"assigneeType"`
	ScopeType    string `json:"scopeType"`
	OrgUnitID    string `json:"orgUnitId"`
	RawJSON      []byte `json:"-"`
}

type WorkspaceOAuthTokenGrant struct {
	UserKey     string   `json:"userKey"`
	ClientID    string   `json:"clientId"`
	DisplayText string   `json:"displayText"`
	NativeApp   bool     `json:"nativeApp"`
	Anonymous   bool     `json:"anonymous"`
	Scopes      []string `json:"scopes"`
	RawJSON     []byte   `json:"-"`
}

type WorkspaceActivity struct {
	ID struct {
		Time            string `json:"time"`
		UniqueQualifier string `json:"uniqueQualifier"`
		ApplicationName string `json:"applicationName"`
		CustomerID      string `json:"customerId"`
	} `json:"id"`
	Actor struct {
		Email     string `json:"email"`
		ProfileID string `json:"profileId"`
	} `json:"actor"`
	Events []struct {
		Name       string `json:"name"`
		Type       string `json:"type"`
		Parameters []struct {
			Name         string   `json:"name"`
			Value        string   `json:"value"`
			IntValue     string   `json:"intValue"`
			BoolValue    bool     `json:"boolValue"`
			MultiValue   []string `json:"multiValue"`
			MessageValue struct {
				Value []struct {
					Name  string `json:"name"`
					Value string `json:"value"`
				} `json:"parameter"`
			} `json:"messageValue"`
		} `json:"parameters"`
	} `json:"events"`
	IPAddress string `json:"ipAddress"`
	RawJSON   []byte `json:"-"`
}

func NewClient(cfg configstore.GoogleWorkspaceConfig) (*Client, error) {
	return NewClientWithOptions(cfg, ClientOptions{})
}

func NewClientWithOptions(cfg configstore.GoogleWorkspaceConfig, opts ClientOptions) (*Client, error) {
	cfg = cfg.Normalized()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	httpClient := opts.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultGoogleTimeout}
	}

	directoryBaseURL := strings.TrimRight(strings.TrimSpace(opts.DirectoryBaseURL), "/")
	if directoryBaseURL == "" {
		directoryBaseURL = defaultGoogleDirectoryBaseURL
	}
	reportsBaseURL := strings.TrimRight(strings.TrimSpace(opts.ReportsBaseURL), "/")
	if reportsBaseURL == "" {
		reportsBaseURL = defaultGoogleReportsBaseURL
	}
	tokenURL := strings.TrimSpace(opts.TokenURL)
	if tokenURL == "" {
		tokenURL = defaultGoogleTokenURL
	}
	iamBaseURL := strings.TrimRight(strings.TrimSpace(opts.IAMCredentialsBaseURL), "/")
	if iamBaseURL == "" {
		iamBaseURL = defaultGoogleIAMBaseURL
	}

	scopes := normalizeScopes(opts.Scopes)
	if len(scopes) == 0 {
		scopes = append([]string(nil), googleWorkspaceDefaultScopes...)
	}

	client := &Client{
		cfg:                cfg,
		http:               httpClient,
		directoryBaseURL:   directoryBaseURL,
		reportsBaseURL:     reportsBaseURL,
		tokenURL:           tokenURL,
		iamCredentialsBase: iamBaseURL,
		scopes:             scopes,
		adcTokenSource:     opts.ADCTokenSource,
	}

	switch cfg.AuthType {
	case configstore.GoogleWorkspaceAuthTypeServiceAccountJSON:
		if err := client.initServiceAccountJSON(); err != nil {
			return nil, err
		}
	case configstore.GoogleWorkspaceAuthTypeADC:
		if strings.TrimSpace(cfg.ServiceAccountEmail) == "" {
			return nil, errors.New("google workspace service account email is required for adc auth")
		}
	default:
		return nil, errors.New("google workspace auth type is invalid")
	}

	return client, nil
}

func normalizeScopes(scopes []string) []string {
	if len(scopes) == 0 {
		return nil
	}
	out := make([]string, 0, len(scopes))
	seen := make(map[string]struct{}, len(scopes))
	for _, scope := range scopes {
		scope = strings.TrimSpace(scope)
		if scope == "" {
			continue
		}
		if _, ok := seen[scope]; ok {
			continue
		}
		seen[scope] = struct{}{}
		out = append(out, scope)
	}
	return out
}

func (c *Client) initServiceAccountJSON() error {
	var payload struct {
		ClientEmail string `json:"client_email"`
		PrivateKey  string `json:"private_key"`
		TokenURI    string `json:"token_uri"`
	}
	if err := json.Unmarshal([]byte(c.cfg.ServiceAccountJSON), &payload); err != nil {
		return fmt.Errorf("decode service account json: %w", err)
	}
	privateKey, err := parseRSAPrivateKey(payload.PrivateKey)
	if err != nil {
		return fmt.Errorf("parse service account private key: %w", err)
	}

	c.saClientEmail = strings.TrimSpace(payload.ClientEmail)
	if c.saClientEmail == "" {
		return errors.New("service account json missing client_email")
	}
	c.parsedPrivatePK = privateKey
	if tokenURI := strings.TrimSpace(payload.TokenURI); tokenURI != "" {
		c.tokenURL = tokenURI
	}
	return nil
}

func parseRSAPrivateKey(raw string) (*rsa.PrivateKey, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, errors.New("private key is required")
	}
	block, _ := pem.Decode([]byte(raw))
	if block == nil {
		return nil, errors.New("invalid PEM private key")
	}

	if key, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		rsaKey, ok := key.(*rsa.PrivateKey)
		if !ok {
			return nil, errors.New("private key is not RSA")
		}
		return rsaKey, nil
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	return nil, errors.New("unsupported private key format")
}

func (c *Client) ListUsers(ctx context.Context, customerID string) ([]WorkspaceUser, error) {
	customerID = strings.TrimSpace(customerID)
	if customerID == "" {
		return nil, errors.New("google workspace customer id is required")
	}
	endpoint := c.directoryBaseURL + "/users"
	items, err := c.listDirectoryPaged(ctx, endpoint, "users", url.Values{
		"customer":   []string{customerID},
		"maxResults": []string{"500"},
		"orderBy":    []string{"email"},
	})
	if err != nil {
		return nil, err
	}

	out := make([]WorkspaceUser, 0, len(items))
	for _, raw := range items {
		var user WorkspaceUser
		if err := json.Unmarshal(raw, &user); err != nil {
			return nil, fmt.Errorf("decode google workspace user: %w", err)
		}
		user.RawJSON = raw
		out = append(out, user)
	}
	return out, nil
}

func (c *Client) ListGroups(ctx context.Context, customerID string) ([]WorkspaceGroup, error) {
	customerID = strings.TrimSpace(customerID)
	if customerID == "" {
		return nil, errors.New("google workspace customer id is required")
	}
	endpoint := c.directoryBaseURL + "/groups"
	items, err := c.listDirectoryPaged(ctx, endpoint, "groups", url.Values{
		"customer":   []string{customerID},
		"maxResults": []string{"200"},
	})
	if err != nil {
		return nil, err
	}

	out := make([]WorkspaceGroup, 0, len(items))
	for _, raw := range items {
		var group WorkspaceGroup
		if err := json.Unmarshal(raw, &group); err != nil {
			return nil, fmt.Errorf("decode google workspace group: %w", err)
		}
		group.RawJSON = raw
		out = append(out, group)
	}
	return out, nil
}

func (c *Client) ListGroupMembers(ctx context.Context, groupID string) ([]WorkspaceGroupMember, error) {
	groupID = strings.TrimSpace(groupID)
	if groupID == "" {
		return nil, errors.New("google workspace group id is required")
	}
	endpoint := c.directoryBaseURL + "/groups/" + url.PathEscape(groupID) + "/members"
	items, err := c.listDirectoryPaged(ctx, endpoint, "members", url.Values{
		"maxResults": []string{"200"},
	})
	if err != nil {
		if errors.Is(err, errNotFound) {
			return nil, nil
		}
		return nil, err
	}

	out := make([]WorkspaceGroupMember, 0, len(items))
	for _, raw := range items {
		var member WorkspaceGroupMember
		if err := json.Unmarshal(raw, &member); err != nil {
			return nil, fmt.Errorf("decode google workspace group member: %w", err)
		}
		member.RawJSON = raw
		out = append(out, member)
	}
	return out, nil
}

func (c *Client) ListAdminRoles(ctx context.Context, customerID string) ([]WorkspaceAdminRole, error) {
	customerID = strings.TrimSpace(customerID)
	if customerID == "" {
		return nil, errors.New("google workspace customer id is required")
	}
	endpoint := c.directoryBaseURL + "/customer/" + url.PathEscape(customerID) + "/roles"
	items, err := c.listDirectoryPaged(ctx, endpoint, "items", url.Values{
		"maxResults": []string{"200"},
	})
	if err != nil {
		return nil, err
	}

	out := make([]WorkspaceAdminRole, 0, len(items))
	for _, raw := range items {
		var role WorkspaceAdminRole
		if err := json.Unmarshal(raw, &role); err != nil {
			return nil, fmt.Errorf("decode google workspace admin role: %w", err)
		}
		role.RawJSON = raw
		out = append(out, role)
	}
	return out, nil
}

func (c *Client) ListAdminRoleAssignments(ctx context.Context, customerID string) ([]WorkspaceAdminRoleAssignment, error) {
	customerID = strings.TrimSpace(customerID)
	if customerID == "" {
		return nil, errors.New("google workspace customer id is required")
	}
	endpoint := c.directoryBaseURL + "/customer/" + url.PathEscape(customerID) + "/roleassignments"
	items, err := c.listDirectoryPaged(ctx, endpoint, "items", url.Values{
		"maxResults": []string{"200"},
	})
	if err != nil {
		return nil, err
	}

	out := make([]WorkspaceAdminRoleAssignment, 0, len(items))
	for _, raw := range items {
		var assignment WorkspaceAdminRoleAssignment
		if err := json.Unmarshal(raw, &assignment); err != nil {
			return nil, fmt.Errorf("decode google workspace admin role assignment: %w", err)
		}
		assignment.RawJSON = raw
		out = append(out, assignment)
	}
	return out, nil
}

func (c *Client) ListOAuthTokenGrants(ctx context.Context) ([]WorkspaceOAuthTokenGrant, error) {
	endpoint := c.directoryBaseURL + "/users/all/tokens"
	items, err := c.listDirectoryPaged(ctx, endpoint, "items", url.Values{
		"maxResults": []string{"500"},
	})
	if err != nil {
		if errors.Is(err, errNotFound) {
			return nil, nil
		}
		return nil, err
	}

	out := make([]WorkspaceOAuthTokenGrant, 0, len(items))
	for _, raw := range items {
		var grant WorkspaceOAuthTokenGrant
		if err := json.Unmarshal(raw, &grant); err != nil {
			return nil, fmt.Errorf("decode google workspace oauth grant: %w", err)
		}
		grant.RawJSON = raw
		out = append(out, grant)
	}
	return out, nil
}

func (c *Client) ListTokenActivities(ctx context.Context, since *time.Time) ([]WorkspaceActivity, error) {
	return c.listActivities(ctx, "token", since)
}

func (c *Client) ListLoginActivities(ctx context.Context, since *time.Time) ([]WorkspaceActivity, error) {
	return c.listActivities(ctx, "login", since)
}

func (c *Client) listActivities(ctx context.Context, applicationName string, since *time.Time) ([]WorkspaceActivity, error) {
	applicationName = strings.TrimSpace(applicationName)
	if applicationName == "" {
		return nil, errors.New("google workspace reports application name is required")
	}
	values := url.Values{}
	values.Set("maxResults", "1000")
	if since != nil && !since.IsZero() {
		values.Set("startTime", since.UTC().Format(time.RFC3339))
	}
	endpoint := c.reportsBaseURL + "/activity/users/all/applications/" + url.PathEscape(applicationName)
	items, err := c.listReportsPaged(ctx, endpoint, "items", values)
	if err != nil {
		if errors.Is(err, errNotFound) {
			return nil, nil
		}
		return nil, err
	}

	out := make([]WorkspaceActivity, 0, len(items))
	for _, raw := range items {
		var activity WorkspaceActivity
		if err := json.Unmarshal(raw, &activity); err != nil {
			return nil, fmt.Errorf("decode google workspace activity: %w", err)
		}
		activity.RawJSON = raw
		out = append(out, activity)
	}
	return out, nil
}

func (a WorkspaceActivity) EventName() string {
	for _, event := range a.Events {
		if name := strings.TrimSpace(event.Name); name != "" {
			return name
		}
	}
	return ""
}

func (a WorkspaceActivity) EventType() string {
	for _, event := range a.Events {
		if eventType := strings.TrimSpace(event.Type); eventType != "" {
			return eventType
		}
	}
	return ""
}

func (a WorkspaceActivity) ParameterValues(name string) []string {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	values := make([]string, 0, 4)
	for _, event := range a.Events {
		for _, param := range event.Parameters {
			if !strings.EqualFold(strings.TrimSpace(param.Name), name) {
				continue
			}
			if v := strings.TrimSpace(param.Value); v != "" {
				values = append(values, v)
			}
			if v := strings.TrimSpace(param.IntValue); v != "" {
				values = append(values, v)
			}
			if len(param.MultiValue) > 0 {
				for _, multi := range param.MultiValue {
					if v := strings.TrimSpace(multi); v != "" {
						values = append(values, v)
					}
				}
			}
			for _, messageValue := range param.MessageValue.Value {
				if v := strings.TrimSpace(messageValue.Value); v != "" {
					values = append(values, v)
				}
			}
			if !param.BoolValue {
				continue
			}
			values = append(values, "true")
		}
	}
	return values
}

func (c *Client) listDirectoryPaged(ctx context.Context, endpoint, key string, values url.Values) ([]json.RawMessage, error) {
	return c.listPaged(ctx, endpoint, key, values)
}

func (c *Client) listReportsPaged(ctx context.Context, endpoint, key string, values url.Values) ([]json.RawMessage, error) {
	return c.listPaged(ctx, endpoint, key, values)
}

func (c *Client) listPaged(ctx context.Context, endpoint, key string, values url.Values) ([]json.RawMessage, error) {
	all := make([]json.RawMessage, 0)
	nextPageToken := ""

	for {
		query := cloneURLValues(values)
		if nextPageToken != "" {
			query.Set("pageToken", nextPageToken)
		}
		requestURL := endpoint
		if encoded := query.Encode(); encoded != "" {
			requestURL += "?" + encoded
		}

		respBody, statusCode, err := c.doAuthorizedJSONRequest(ctx, http.MethodGet, requestURL, nil)
		if err != nil {
			if statusCode == http.StatusNotFound {
				return nil, errNotFound
			}
			return nil, err
		}

		var payload struct {
			NextPageToken string            `json:"nextPageToken"`
			Items         []json.RawMessage `json:"items"`
			Users         []json.RawMessage `json:"users"`
			Groups        []json.RawMessage `json:"groups"`
			Members       []json.RawMessage `json:"members"`
		}
		if err := json.Unmarshal(respBody, &payload); err != nil {
			return nil, fmt.Errorf("decode google api page response: %w", err)
		}

		switch key {
		case "items":
			all = append(all, payload.Items...)
		case "users":
			all = append(all, payload.Users...)
		case "groups":
			all = append(all, payload.Groups...)
		case "members":
			all = append(all, payload.Members...)
		default:
			all = append(all, payload.Items...)
		}

		nextPageToken = strings.TrimSpace(payload.NextPageToken)
		if nextPageToken == "" {
			break
		}
	}

	return all, nil
}

func cloneURLValues(values url.Values) url.Values {
	if len(values) == 0 {
		return url.Values{}
	}
	cloned := make(url.Values, len(values))
	for key, items := range values {
		cp := make([]string, len(items))
		copy(cp, items)
		cloned[key] = cp
	}
	return cloned
}

var errNotFound = errors.New("google api resource not found")

func (c *Client) doAuthorizedJSONRequest(ctx context.Context, method, requestURL string, body []byte) ([]byte, int, error) {
	var lastErr error
	statusCode := 0
	backoff := 500 * time.Millisecond

	for attempt := 0; attempt < googleMaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, statusCode, ctx.Err()
			case <-time.After(backoff):
			}
			backoff = minDuration(backoff*2, 8*time.Second)
		}

		accessToken, err := c.accessToken(ctx)
		if err != nil {
			return nil, statusCode, err
		}

		req, err := http.NewRequestWithContext(ctx, method, requestURL, strings.NewReader(string(body)))
		if err != nil {
			return nil, statusCode, err
		}
		req.Header.Set("Authorization", "Bearer "+accessToken)
		req.Header.Set("Accept", "application/json")
		if len(body) > 0 {
			req.Header.Set("Content-Type", "application/json")
		}

		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		statusCode = resp.StatusCode
		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		_ = resp.Body.Close()
		if readErr != nil {
			lastErr = readErr
			continue
		}

		if statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden {
			c.invalidateToken()
		}

		if statusCode >= 200 && statusCode < 300 {
			return respBody, statusCode, nil
		}

		if !shouldRetryGoogleStatus(statusCode) {
			return nil, statusCode, fmt.Errorf("google api request failed: status=%d body=%s", statusCode, strings.TrimSpace(string(respBody)))
		}

		lastErr = fmt.Errorf("google api temporary failure: status=%d body=%s", statusCode, strings.TrimSpace(string(respBody)))
	}

	if lastErr == nil {
		lastErr = errors.New("google api request failed")
	}
	return nil, statusCode, lastErr
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func shouldRetryGoogleStatus(statusCode int) bool {
	if statusCode == http.StatusTooManyRequests {
		return true
	}
	return statusCode >= 500 && statusCode < 600
}

func (c *Client) invalidateToken() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cachedToken = ""
	c.cachedTokenExp = time.Time{}
}

func (c *Client) accessToken(ctx context.Context) (string, error) {
	c.mu.Lock()
	if token := strings.TrimSpace(c.cachedToken); token != "" && time.Now().UTC().Add(googleTokenLeeway).Before(c.cachedTokenExp) {
		c.mu.Unlock()
		return token, nil
	}
	c.mu.Unlock()

	token, expiry, err := c.fetchAccessToken(ctx)
	if err != nil {
		return "", err
	}

	c.mu.Lock()
	c.cachedToken = token
	c.cachedTokenExp = expiry
	c.mu.Unlock()
	return token, nil
}

func (c *Client) fetchAccessToken(ctx context.Context) (string, time.Time, error) {
	assertion, err := c.signedAssertion(ctx)
	if err != nil {
		return "", time.Time{}, err
	}

	form := url.Values{}
	form.Set("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
	form.Set("assertion", assertion)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", time.Time{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return "", time.Time{}, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return "", time.Time{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", time.Time{}, fmt.Errorf("google oauth token exchange failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	var payload struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
		TokenType   string `json:"token_type"`
	}
	if err := json.Unmarshal(respBody, &payload); err != nil {
		return "", time.Time{}, fmt.Errorf("decode google oauth token response: %w", err)
	}
	if strings.TrimSpace(payload.AccessToken) == "" {
		return "", time.Time{}, errors.New("google oauth token response missing access_token")
	}
	expiresIn := payload.ExpiresIn
	if expiresIn <= 0 {
		expiresIn = 3600
	}
	expiry := time.Now().UTC().Add(time.Duration(expiresIn) * time.Second)
	return payload.AccessToken, expiry, nil
}

func (c *Client) signedAssertion(ctx context.Context) (string, error) {
	issuedAt := time.Now().UTC()
	expiresAt := issuedAt.Add(1 * time.Hour)

	claims := map[string]any{
		"iss":   c.issuer(),
		"sub":   strings.TrimSpace(c.cfg.DelegatedAdminEmail),
		"scope": strings.Join(c.scopes, " "),
		"aud":   c.tokenURL,
		"iat":   issuedAt.Unix(),
		"exp":   expiresAt.Unix(),
	}

	switch c.cfg.AuthType {
	case configstore.GoogleWorkspaceAuthTypeServiceAccountJSON:
		return signJWTAssertion(claims, c.parsedPrivatePK)
	case configstore.GoogleWorkspaceAuthTypeADC:
		return c.signJWTViaIAM(ctx, claims)
	default:
		return "", errors.New("unsupported google workspace auth type")
	}
}

func (c *Client) issuer() string {
	switch c.cfg.AuthType {
	case configstore.GoogleWorkspaceAuthTypeADC:
		return strings.TrimSpace(c.cfg.ServiceAccountEmail)
	default:
		return strings.TrimSpace(c.saClientEmail)
	}
}

func signJWTAssertion(claims map[string]any, privateKey *rsa.PrivateKey) (string, error) {
	if privateKey == nil {
		return "", errors.New("rsa private key is required")
	}
	headerJSON, err := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT"})
	if err != nil {
		return "", err
	}
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	encodedHeader := base64.RawURLEncoding.EncodeToString(headerJSON)
	encodedClaims := base64.RawURLEncoding.EncodeToString(claimsJSON)
	signingInput := encodedHeader + "." + encodedClaims

	hash := sha256.Sum256([]byte(signingInput))
	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, hash[:])
	if err != nil {
		return "", err
	}
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func (c *Client) signJWTViaIAM(ctx context.Context, claims map[string]any) (string, error) {
	serviceAccountEmail := strings.TrimSpace(c.cfg.ServiceAccountEmail)
	if serviceAccountEmail == "" {
		return "", errors.New("service account email is required for adc auth")
	}

	adcTS := c.adcTokenSource
	if adcTS == nil {
		var err error
		adcTS, err = google.DefaultTokenSource(ctx, "https://www.googleapis.com/auth/cloud-platform")
		if err != nil {
			return "", fmt.Errorf("google adc token source: %w", err)
		}
	}
	adcToken, err := adcTS.Token()
	if err != nil {
		return "", fmt.Errorf("google adc token: %w", err)
	}

	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}

	requestURL := c.iamCredentialsBase + "/projects/-/serviceAccounts/" + url.PathEscape(serviceAccountEmail) + ":signJwt"
	requestBody, err := json.Marshal(map[string]string{"payload": string(claimsJSON)})
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, strings.NewReader(string(requestBody)))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+adcToken.AccessToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("google iam signJwt failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	var payload struct {
		SignedJWT string `json:"signedJwt"`
	}
	if err := json.Unmarshal(respBody, &payload); err != nil {
		return "", fmt.Errorf("decode google iam signJwt response: %w", err)
	}
	if strings.TrimSpace(payload.SignedJWT) == "" {
		return "", errors.New("google iam signJwt response missing signedJwt")
	}
	return payload.SignedJWT, nil
}

func parseGoogleTime(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err == nil {
		return parsed.UTC()
	}
	if unixMS, err := strconv.ParseInt(raw, 10, 64); err == nil {
		if unixMS > 0 {
			return time.UnixMilli(unixMS).UTC()
		}
	}
	return time.Time{}
}

package datadog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	defaultTimeout   = 120 * time.Second
	defaultPageSize  = 100
	maxRetriesOn429  = 3
	maxErrorBodySize = 1 << 20 // 1 MiB
)

type Client struct {
	BaseURL string
	APIKey  string
	AppKey  string
	HTTP    *http.Client
}

type User struct {
	ID          string
	UserName    string
	Status      string
	LastLoginAt *time.Time
	RawJSON     []byte
}

type Role struct {
	ID          string
	Name        string
	Description string
	RawJSON     []byte
}

// New creates a new Datadog client. It validates that baseURL, apiKey, and appKey are provided.
func New(baseURL, apiKey, appKey string) (*Client, error) {
	base := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	apiKey = strings.TrimSpace(apiKey)
	appKey = strings.TrimSpace(appKey)

	if base == "" {
		return nil, errors.New("datadog base URL is required")
	}
	if apiKey == "" {
		return nil, errors.New("datadog api key is required")
	}
	if appKey == "" {
		return nil, errors.New("datadog app key is required")
	}

	return &Client{
		BaseURL: base,
		APIKey:  apiKey,
		AppKey:  appKey,
		HTTP:    &http.Client{Timeout: defaultTimeout},
	}, nil
}

func (c *Client) ensureClient() error {
	if c.BaseURL == "" {
		return errors.New("datadog base URL is required")
	}
	if c.APIKey == "" || c.AppKey == "" {
		return errors.New("datadog api key and app key are required")
	}
	if c.HTTP == nil {
		return errors.New("datadog http client is not configured")
	}
	return nil
}

func (c *Client) ListUsers(ctx context.Context) ([]User, error) {
	if err := c.ensureClient(); err != nil {
		return nil, err
	}

	var out []User
	for page := 0; ; page++ {
		endpoint, err := c.endpoint("/api/v2/users", page)
		if err != nil {
			return nil, err
		}
		body, err := c.get(ctx, endpoint)
		if err != nil {
			return nil, err
		}
		var payload struct {
			Data []json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			return nil, err
		}
		if len(payload.Data) == 0 {
			break
		}
		for _, raw := range payload.Data {
			user, err := mapUser(raw)
			if err != nil {
				return nil, err
			}
			out = append(out, user)
		}
	}
	return out, nil
}

func (c *Client) ListRoles(ctx context.Context) ([]Role, error) {
	if err := c.ensureClient(); err != nil {
		return nil, err
	}

	var out []Role
	for page := 0; ; page++ {
		endpoint, err := c.endpoint("/api/v2/roles", page)
		if err != nil {
			return nil, err
		}
		body, err := c.get(ctx, endpoint)
		if err != nil {
			return nil, err
		}
		var payload struct {
			Data []json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			return nil, err
		}
		if len(payload.Data) == 0 {
			break
		}
		for _, raw := range payload.Data {
			role, err := mapRole(raw)
			if err != nil {
				return nil, err
			}
			out = append(out, role)
		}
	}
	return out, nil
}

func (c *Client) ListRoleUsers(ctx context.Context, roleID string) ([]User, error) {
	if err := c.ensureClient(); err != nil {
		return nil, err
	}
	roleID = strings.TrimSpace(roleID)
	if roleID == "" {
		return nil, errors.New("datadog role id is required")
	}

	var out []User
	path := fmt.Sprintf("/api/v2/roles/%s/users", url.PathEscape(roleID))
	for page := 0; ; page++ {
		endpoint, err := c.endpoint(path, page)
		if err != nil {
			return nil, err
		}
		body, err := c.get(ctx, endpoint)
		if err != nil {
			return nil, err
		}
		var payload struct {
			Data []json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			return nil, err
		}
		if len(payload.Data) == 0 {
			break
		}
		for _, raw := range payload.Data {
			user, err := mapUser(raw)
			if err != nil {
				return nil, err
			}
			out = append(out, user)
		}
	}
	return out, nil
}

func (c *Client) endpoint(path string, page int) (string, error) {
	base := strings.TrimRight(c.BaseURL, "/")
	if base == "" {
		return "", errors.New("datadog base URL is required")
	}
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	u.Path = strings.TrimRight(u.Path, "/") + path
	q := u.Query()
	q.Set("page[size]", strconv.Itoa(defaultPageSize))
	q.Set("page[number]", strconv.Itoa(page))
	u.RawQuery = q.Encode()
	u.Fragment = ""
	return u.String(), nil
}

func (c *Client) get(ctx context.Context, endpoint string) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt <= maxRetriesOn429; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("DD-API-KEY", c.APIKey)
		req.Header.Set("DD-APPLICATION-KEY", c.AppKey)
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "open-sspm")

		resp, err := c.HTTP.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodySize))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			lastErr = formatDatadogAPIError("datadog api rate limited", endpoint, resp, body)
			if attempt == maxRetriesOn429 {
				return nil, lastErr
			}
			wait, ok := retryAfterDuration(resp.Header.Get("Retry-After"))
			if !ok {
				wait = time.Second
			}
			if err := sleep(ctx, wait); err != nil {
				return nil, err
			}
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, formatDatadogAPIError("datadog api failed", endpoint, resp, body)
		}
		return body, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, errors.New("datadog request failed")
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

func mapUser(raw json.RawMessage) (User, error) {
	var payload struct {
		ID         string `json:"id"`
		Attributes struct {
			Name        string          `json:"name"`
			Handle      string          `json:"handle"`
			Status      string          `json:"status"`
			Disabled    *bool           `json:"disabled"`
			LastLogin   json.RawMessage `json:"last_login"`
			LastLoginAt json.RawMessage `json:"last_login_at"`
		} `json:"attributes"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return User{}, err
	}

	userName := strings.TrimSpace(payload.Attributes.Handle)
	if userName == "" {
		userName = strings.TrimSpace(payload.Attributes.Name)
	}
	userName = strings.TrimSpace(userName)
	if userName == "" {
		userName = strings.TrimSpace(payload.ID)
	}

	status := strings.TrimSpace(payload.Attributes.Status)
	if status == "" && payload.Attributes.Disabled != nil {
		if *payload.Attributes.Disabled {
			status = "Inactive"
		} else {
			status = "Active"
		}
	}

	type sanitizedUser struct {
		UserName    string     `json:"user_name"`
		Status      string     `json:"status"`
		LastLoginAt *time.Time `json:"last_login_at,omitempty"`
	}
	lastLoginAt, err := parseOptionalRFC3339Time(payload.Attributes.LastLoginAt)
	if err != nil {
		slog.Warn("datadog user last_login_at parse failed", "user_id", strings.TrimSpace(payload.ID), "err", err)
		lastLoginAt = nil
	}
	if lastLoginAt == nil {
		lastLoginAt, err = parseOptionalRFC3339Time(payload.Attributes.LastLogin)
		if err != nil {
			slog.Warn("datadog user last_login parse failed", "user_id", strings.TrimSpace(payload.ID), "err", err)
			lastLoginAt = nil
		}
	}

	sanitized, err := json.Marshal(sanitizedUser{
		UserName:    userName,
		Status:      status,
		LastLoginAt: lastLoginAt,
	})
	if err != nil {
		return User{}, err
	}

	return User{
		ID:          payload.ID,
		UserName:    userName,
		Status:      status,
		LastLoginAt: lastLoginAt,
		RawJSON:     sanitized,
	}, nil
}

func parseOptionalRFC3339Time(raw json.RawMessage) (*time.Time, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, nil
	}

	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, err
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}

	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return &t, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return &t, nil
	}
	return nil, fmt.Errorf("invalid timestamp %q", s)
}

func mapRole(raw json.RawMessage) (Role, error) {
	var payload struct {
		ID         string `json:"id"`
		Attributes struct {
			Name        string `json:"name"`
			Description string `json:"description"`
		} `json:"attributes"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return Role{}, err
	}
	return Role{
		ID:          payload.ID,
		Name:        payload.Attributes.Name,
		Description: payload.Attributes.Description,
		RawJSON:     raw,
	}, nil
}

func formatDatadogAPIError(prefix, reqURL string, resp *http.Response, body []byte) error {
	message := extractDatadogAPIErrorMessage(body)
	details := formatDatadogAPIErrorDetails(reqURL, resp)

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

func extractDatadogAPIErrorMessage(body []byte) string {
	var payload struct {
		Errors  []string `json:"errors"`
		Error   string   `json:"error"`
		Message string   `json:"message"`
	}
	if err := json.Unmarshal(body, &payload); err == nil {
		if len(payload.Errors) > 0 {
			first := strings.TrimSpace(payload.Errors[0])
			if first != "" {
				return first
			}
		}
		if msg := strings.TrimSpace(payload.Error); msg != "" {
			return msg
		}
		if msg := strings.TrimSpace(payload.Message); msg != "" {
			return msg
		}
	}

	msg := strings.TrimSpace(string(body))
	if msg == "" {
		return ""
	}
	if strings.HasPrefix(msg, "<!DOCTYPE html") || strings.HasPrefix(msg, "<html") {
		return ""
	}
	msg = strings.Join(strings.Fields(msg), " ")
	const maxLen = 300
	if len(msg) > maxLen {
		msg = msg[:maxLen] + "…"
	}
	return msg
}

func formatDatadogAPIErrorDetails(reqURL string, resp *http.Response) string {
	var parts []string
	if v := safeURL(reqURL); v != "" {
		parts = append(parts, "url="+v)
	}
	if v := headerAny(resp.Header, "x-request-id", "x-datadog-trace-id"); v != "" {
		parts = append(parts, "request_id="+v)
	}
	if v := resp.Header.Get("x-ratelimit-remaining"); v != "" {
		parts = append(parts, "rate_remaining="+v)
	}
	if v := resp.Header.Get("x-ratelimit-limit"); v != "" {
		parts = append(parts, "rate_limit="+v)
	}
	if v := resp.Header.Get("x-ratelimit-reset"); v != "" {
		parts = append(parts, "rate_reset="+v)
	}
	if v := resp.Header.Get("Retry-After"); v != "" {
		parts = append(parts, "retry_after="+v)
	}
	return strings.Join(parts, ", ")
}

func headerAny(h http.Header, keys ...string) string {
	for _, k := range keys {
		if v := strings.TrimSpace(h.Get(k)); v != "" {
			return v
		}
	}
	return ""
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

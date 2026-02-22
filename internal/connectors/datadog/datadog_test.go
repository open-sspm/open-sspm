package datadog

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

func TestListUsersPaginationStopsOnEmptyPage(t *testing.T) {
	var calls int32

	c, err := New("https://example.test", "api", "app")
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	c.HTTP.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		if req.URL.Path != "/api/v2/users" {
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"errors":["not found"]}`)),
				Request:    req,
			}, nil
		}
		switch req.URL.Query().Get("page[number]") {
		case "0":
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body: io.NopCloser(strings.NewReader(
					`{"data":[` +
						`{"id":"u1","attributes":{"name":"Alice","handle":"alice@example.com","status":"Active"}},` +
						`{"id":"u2","attributes":{"name":"","handle":"bob@example.com","status":"Inactive"}}` +
						`]}`,
				)),
				Request: req,
			}, nil
		default:
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"data":[]}`)),
				Request:    req,
			}, nil
		}
	})

	users, err := c.ListUsers(context.Background())
	if err != nil {
		t.Fatalf("ListUsers error: %v", err)
	}
	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users))
	}
	if users[0].ID != "u1" || users[0].UserName != "alice@example.com" || users[0].Status != "Active" {
		t.Fatalf("unexpected user[0]: %#v", users[0])
	}
	if users[1].ID != "u2" || users[1].UserName != "bob@example.com" || users[1].Status != "Inactive" {
		t.Fatalf("unexpected user[1]: %#v", users[1])
	}
	if got := strings.TrimSpace(string(users[0].RawJSON)); got == "" || strings.Contains(got, `"email"`) || strings.Contains(got, `"groups"`) {
		t.Fatalf("expected sanitized RawJSON without email/groups, got: %s", got)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("expected 2 requests, got %d", got)
	}
}

func TestListUsersRetriesOn429(t *testing.T) {
	var calls int32

	c, err := New("https://example.test", "api", "app")
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	c.HTTP.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		n := atomic.AddInt32(&calls, 1)
		if req.URL.Path != "/api/v2/users" {
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"errors":["not found"]}`)),
				Request:    req,
			}, nil
		}
		if n == 1 {
			h := make(http.Header)
			h.Set("Retry-After", "0")
			return &http.Response{
				StatusCode: http.StatusTooManyRequests,
				Header:     h,
				Body:       io.NopCloser(strings.NewReader(`{"errors":["rate limit"]}`)),
				Request:    req,
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"data":[]}`)),
			Request:    req,
		}, nil
	})

	_, listErr := c.ListUsers(context.Background())
	if listErr != nil {
		t.Fatalf("expected retry to succeed, got error: %v", listErr)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("expected 2 requests, got %d", got)
	}
}

func TestListUsersUsesNameWhenHandleMissing(t *testing.T) {
	c, err := New("https://example.test", "api", "app")
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	c.HTTP.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != "/api/v2/users" {
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"errors":["not found"]}`)),
				Request:    req,
			}, nil
		}
		switch req.URL.Query().Get("page[number]") {
		case "0":
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body: io.NopCloser(strings.NewReader(
					`{"data":[` +
						`{"id":"u1","attributes":{"name":"Alice","handle":"","status":"Active"}}` +
						`]}`,
				)),
				Request: req,
			}, nil
		default:
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"data":[]}`)),
				Request:    req,
			}, nil
		}
	})

	users, err := c.ListUsers(context.Background())
	if err != nil {
		t.Fatalf("ListUsers error: %v", err)
	}
	if len(users) != 1 {
		t.Fatalf("expected 1 user, got %d", len(users))
	}
	if users[0].UserName != "Alice" {
		t.Fatalf("expected user name fallback to name, got %#v", users[0])
	}
}

func TestErrorMessageIncludesDatadogErrorsArray(t *testing.T) {
	c, err := New("https://example.test", "api", "app")
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	c.HTTP.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusForbidden,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"errors":["forbidden"]}`)),
			Request:    req,
		}, nil
	})

	_, listErr := c.ListUsers(context.Background())
	if listErr == nil {
		t.Fatalf("expected error")
	}
	if got := listErr.Error(); got == "" || !strings.Contains(got, "forbidden") {
		t.Fatalf("expected error to include 'forbidden', got: %s", got)
	}
}

func TestListServiceAccountsPaginationStopsOnEmptyPage(t *testing.T) {
	var calls int32

	c, err := New("https://example.test", "api", "app")
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	c.HTTP.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		if req.URL.Path != "/api/v2/service_accounts" {
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"errors":["not found"]}`)),
				Request:    req,
			}, nil
		}
		switch req.URL.Query().Get("page[number]") {
		case "0":
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body: io.NopCloser(strings.NewReader(
					`{"data":[` +
						`{"id":"sa1","attributes":{"name":"CI Service Account","email":"ci@example.com","status":"Active"}},` +
						`{"id":"sa2","attributes":{"name":"","handle":"robot@example.com","disabled":true}}` +
						`]}`,
				)),
				Request: req,
			}, nil
		default:
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"data":[]}`)),
				Request:    req,
			}, nil
		}
	})

	serviceAccounts, err := c.ListServiceAccounts(context.Background())
	if err != nil {
		t.Fatalf("ListServiceAccounts error: %v", err)
	}
	if len(serviceAccounts) != 2 {
		t.Fatalf("expected 2 service accounts, got %d", len(serviceAccounts))
	}
	if serviceAccounts[0].ID != "sa1" || serviceAccounts[0].Name != "CI Service Account" {
		t.Fatalf("unexpected serviceAccounts[0]: %#v", serviceAccounts[0])
	}
	if serviceAccounts[1].Status != "Inactive" {
		t.Fatalf("unexpected serviceAccounts[1] status: %#v", serviceAccounts[1])
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("expected 2 requests, got %d", got)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

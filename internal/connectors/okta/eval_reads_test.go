package okta

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/oktaapi"
)

func TestClient_ListOktaSignOnPolicyRules_SuccessAndPagination(t *testing.T) {
	t.Parallel()

	const policyID = "p1"
	const token = "testtoken"

	var baseURL string
	var requestCount int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)

		if got := r.Header.Get("Authorization"); got != "SSWS "+token {
			t.Errorf("Authorization header = %q, want %q", got, "SSWS "+token)
		}
		if got := r.Header.Get("Accept"); got != "application/json" {
			t.Errorf("Accept header = %q, want %q", got, "application/json")
		}
		if got := r.URL.Path; got != "/api/v1/policies/"+policyID+"/rules" {
			t.Errorf("path = %q, want %q", got, "/api/v1/policies/"+policyID+"/rules")
		}
		if got := r.URL.Query().Get("limit"); got != "200" {
			t.Errorf("limit = %q, want %q", got, "200")
		}

		after := r.URL.Query().Get("after")
		switch after {
		case "":
			w.Header().Set("Link", fmt.Sprintf("<%s/api/v1/policies/%s/rules?after=abc>; rel=\"next\"", baseURL, policyID))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"id":"r1","name":"Rule 1","status":"ACTIVE","priority":1,"system":false,"actions":{"signon":{"session":{"maxSessionIdleMinutes":10,"maxSessionLifetimeMinutes":100,"usePersistentCookie":true}}}}]`))
		case "abc":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"id":"r2","name":"Rule 2","status":"INACTIVE","priority":2,"system":true}]`))
		default:
			t.Errorf("unexpected after=%q", after)
			http.Error(w, "unexpected after", http.StatusInternalServerError)
		}
	}))
	defer srv.Close()
	baseURL = srv.URL

	c := &Client{BaseURL: srv.URL, Token: token}
	rules, err := c.ListOktaSignOnPolicyRules(context.Background(), policyID)
	if err != nil {
		t.Fatalf("ListOktaSignOnPolicyRules() err = %v", err)
	}
	if got, want := len(rules), 2; got != want {
		t.Fatalf("len(rules) = %d, want %d", got, want)
	}
	if got := atomic.LoadInt32(&requestCount); got != 2 {
		t.Fatalf("requestCount = %d, want %d", got, 2)
	}

	if got, want := rules[0].ID, "r1"; got != want {
		t.Fatalf("rules[0].ID = %q, want %q", got, want)
	}
	if rules[0].Session.MaxSessionIdleMinutes == nil || *rules[0].Session.MaxSessionIdleMinutes != 10 {
		t.Fatalf("rules[0].Session.MaxSessionIdleMinutes = %v, want %d", rules[0].Session.MaxSessionIdleMinutes, 10)
	}
	if rules[0].Session.MaxSessionLifetimeMinutes == nil || *rules[0].Session.MaxSessionLifetimeMinutes != 100 {
		t.Fatalf("rules[0].Session.MaxSessionLifetimeMinutes = %v, want %d", rules[0].Session.MaxSessionLifetimeMinutes, 100)
	}
	if rules[0].Session.UsePersistentCookie == nil || *rules[0].Session.UsePersistentCookie != true {
		t.Fatalf("rules[0].Session.UsePersistentCookie = %v, want %v", rules[0].Session.UsePersistentCookie, true)
	}

	if got, want := rules[1].ID, "r2"; got != want {
		t.Fatalf("rules[1].ID = %q, want %q", got, want)
	}
	if rules[1].Session.MaxSessionIdleMinutes != nil {
		t.Fatalf("rules[1].Session.MaxSessionIdleMinutes = %v, want nil", rules[1].Session.MaxSessionIdleMinutes)
	}
}

func TestClient_ListOktaSignOnPolicyRules_InvalidJSONReturnsNonAPIError(t *testing.T) {
	t.Parallel()

	const policyID = "p1"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`this is not json`))
	}))
	defer srv.Close()

	c := &Client{BaseURL: srv.URL, Token: "testtoken"}
	_, err := c.ListOktaSignOnPolicyRules(context.Background(), policyID)
	if err == nil {
		t.Fatalf("ListOktaSignOnPolicyRules() err = nil, want non-nil")
	}
	var apiErr *oktaapi.APIError
	if errors.As(err, &apiErr) {
		t.Fatalf("err is oktaapi.APIError, want non-API error: %v", err)
	}
	if got := err.Error(); got == "" || !strings.Contains(got, "decode okta sign-on policy rules") {
		t.Fatalf("err = %q, want decode context", got)
	}
}

func TestClient_ListOktaSignOnPolicyRules_APIErrorIncludesSummary(t *testing.T) {
	t.Parallel()

	const policyID = "p1"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"errorSummary":"forbidden"}`))
	}))
	defer srv.Close()

	c := &Client{BaseURL: srv.URL, Token: "testtoken"}
	_, err := c.ListOktaSignOnPolicyRules(context.Background(), policyID)
	if err == nil {
		t.Fatalf("ListOktaSignOnPolicyRules() err = nil, want non-nil")
	}
	var apiErr *oktaapi.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("err = %T, want oktaapi.APIError", err)
	}
	if got, want := apiErr.StatusCode, http.StatusForbidden; got != want {
		t.Fatalf("apiErr.StatusCode = %d, want %d", got, want)
	}
	if got, want := apiErr.Summary, "forbidden"; got != want {
		t.Fatalf("apiErr.Summary = %q, want %q", got, want)
	}
}

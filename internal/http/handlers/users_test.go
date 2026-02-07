package handlers

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
)

func TestParseCreateLinkFormSupportsIdentityPayload(t *testing.T) {
	t.Parallel()

	ctx := newLinkFormContext(t, map[string]string{
		"identity_id": "101",
		"account_id":  "202",
	})

	identityID, accountID, reason, err := parseCreateLinkForm(ctx)
	if err != nil {
		t.Fatalf("parseCreateLinkForm() error = %v", err)
	}
	if identityID != 101 {
		t.Fatalf("identityID = %d, want 101", identityID)
	}
	if accountID != 202 {
		t.Fatalf("accountID = %d, want 202", accountID)
	}
	if reason != "manual" {
		t.Fatalf("reason = %q, want %q", reason, "manual")
	}
}

func TestParseCreateLinkFormSupportsLegacyPayload(t *testing.T) {
	t.Parallel()

	ctx := newLinkFormContext(t, map[string]string{
		"idp_user_id": "301",
		"app_user_id": "401",
		"reason":      "seed_migration",
	})

	identityID, accountID, reason, err := parseCreateLinkForm(ctx)
	if err != nil {
		t.Fatalf("parseCreateLinkForm() error = %v", err)
	}
	if identityID != 301 {
		t.Fatalf("identityID = %d, want 301", identityID)
	}
	if accountID != 401 {
		t.Fatalf("accountID = %d, want 401", accountID)
	}
	if reason != "seed_migration" {
		t.Fatalf("reason = %q, want %q", reason, "seed_migration")
	}
}

func TestParseCreateLinkFormRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	ctx := newLinkFormContext(t, map[string]string{
		"identity_id": "abc",
		"account_id":  "202",
	})

	if _, _, _, err := parseCreateLinkForm(ctx); err == nil {
		t.Fatalf("parseCreateLinkForm() error = nil, want invalid identity_id")
	}
}

func newLinkFormContext(t *testing.T, values map[string]string) *echo.Context {
	t.Helper()

	form := url.Values{}
	for key, value := range values {
		form.Set(key, value)
	}
	req := httptest.NewRequest(http.MethodPost, "/links", strings.NewReader(form.Encode()))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationForm)
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	return c
}

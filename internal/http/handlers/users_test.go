package handlers

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/db/gen"
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

func TestOktaGroupBadgesFromEntitlementsUsesGenericMemberships(t *testing.T) {
	t.Parallel()

	entitlements := []gen.ListEntitlementsForAccountIDsRow{
		{
			Kind:     "group_membership",
			Resource: "group:00g-eng",
			RawJson:  []byte(`{"attributes":{"target":{"external_id":"00g-eng","display_name":"Engineering"}}}`),
		},
		{
			Kind:     "application_assignment",
			Resource: "0oa-payroll",
		},
		{
			Kind:     "group_membership",
			Resource: "group:00g-it",
			RawJson:  []byte(`{"id":"00g-it","profile":{"name":"IT Admins"}}`),
		},
	}

	badges, names := oktaGroupBadgesFromEntitlements(entitlements)
	if len(badges) != 2 {
		t.Fatalf("badges len = %d, want 2: %+v", len(badges), badges)
	}
	if badges[0].Name != "Engineering" || badges[0].ExternalID != "00g-eng" {
		t.Fatalf("first badge = %+v, want Engineering/00g-eng", badges[0])
	}
	if badges[1].Name != "IT Admins" || badges[1].ExternalID != "00g-it" {
		t.Fatalf("second badge = %+v, want IT Admins/00g-it", badges[1])
	}
	if names["00g-eng"] != "Engineering" || names["00g-it"] != "IT Admins" {
		t.Fatalf("names = %+v, want generic group membership names", names)
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

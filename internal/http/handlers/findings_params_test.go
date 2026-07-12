package handlers

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
)

func TestParseOverrideParamsFromFormUsesDefaultTypes(t *testing.T) {
	t.Parallel()

	form := url.Values{
		"param_name":    {"admins"},
		"param_enabled": {"false"},
		"param_limit":   {"30"},
		"param_options": {`{"strict":false}`},
	}
	req := httptest.NewRequest(http.MethodPost, "/override", strings.NewReader(form.Encode()))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationForm)
	c := echo.New().NewContext(req, httptest.NewRecorder())

	params, err := parseOverrideParamsFromForm(c, map[string]any{
		"name":    "users",
		"enabled": true,
		"limit":   float64(15),
		"options": map[string]any{"strict": true},
	})
	if err != nil {
		t.Fatalf("parseOverrideParamsFromForm() error = %v", err)
	}

	want := map[string]any{
		"name":    "admins",
		"enabled": false,
		"limit":   float64(30),
		"options": map[string]any{"strict": false},
	}
	if !reflect.DeepEqual(params, want) {
		t.Fatalf("parseOverrideParamsFromForm() = %#v, want %#v", params, want)
	}
}

func TestBuildRuleOverrideViewInfersFieldsFromDefaults(t *testing.T) {
	t.Parallel()

	view := buildRuleOverrideView(map[string]any{
		"enabled": true,
		"options": map[string]any{"strict": true},
	}, nil)

	if !view.HasParameters || len(view.Fields) != 2 {
		t.Fatalf("buildRuleOverrideView() = %+v", view)
	}
	if view.Fields[0].Key != "enabled" || view.Fields[0].Type != "boolean" || view.Fields[0].DefaultValue != "true" {
		t.Fatalf("enabled field = %+v", view.Fields[0])
	}
	if view.Fields[1].Key != "options" || view.Fields[1].Type != "json" || view.Fields[1].DefaultValue != `{"strict":true}` {
		t.Fatalf("options field = %+v", view.Fields[1])
	}
}

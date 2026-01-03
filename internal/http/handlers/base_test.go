package handlers

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
)

func TestRenderErrorDoesNotLeakError(t *testing.T) {
	e := echo.New()
	e.Logger.SetOutput(io.Discard)

	req := httptest.NewRequest(http.MethodGet, "http://example.com/test", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.Set(ContextKeyRequestID, "req-123")

	h := &Handlers{}
	if err := h.RenderError(c, errors.New("db password=secret")); err != nil {
		t.Fatalf("RenderError: %v", err)
	}

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d want %d", rec.Code, http.StatusInternalServerError)
	}

	body := rec.Body.String()
	if strings.Contains(body, "db password") || strings.Contains(body, "secret") {
		t.Fatalf("response leaked error details: %q", body)
	}
	if !strings.Contains(body, "Internal server error") {
		t.Fatalf("response missing generic message: %q", body)
	}
	if !strings.Contains(body, "Reference: req-123") {
		t.Fatalf("response missing request reference: %q", body)
	}
	if !strings.Contains(body, "Code: "+InternalErrorCode) {
		t.Fatalf("response missing error code: %q", body)
	}
}

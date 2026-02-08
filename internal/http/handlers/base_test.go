package handlers

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
)

func TestRenderErrorDoesNotLeakError(t *testing.T) {
	e := echo.New()
	e.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))

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
	if got := rec.Header().Get(echo.HeaderContentType); got != echo.MIMETextPlainCharsetUTF8 {
		t.Fatalf("content-type=%q want %q", got, echo.MIMETextPlainCharsetUTF8)
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

func TestRenderNotFoundSetsPlainTextContentType(t *testing.T) {
	e := echo.New()
	e.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	req := httptest.NewRequest(http.MethodGet, "http://example.com/missing", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	if err := RenderNotFound(c); err != nil {
		t.Fatalf("RenderNotFound: %v", err)
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d want %d", rec.Code, http.StatusNotFound)
	}
	if got := rec.Header().Get(echo.HeaderContentType); got != echo.MIMETextPlainCharsetUTF8 {
		t.Fatalf("content-type=%q want %q", got, echo.MIMETextPlainCharsetUTF8)
	}
}

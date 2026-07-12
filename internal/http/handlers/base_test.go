package handlers

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/a-h/templ"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
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

func TestRenderErrorRendersStyledPageForBrowserNavigation(t *testing.T) {
	e := echo.New()
	e.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	req := httptest.NewRequest(http.MethodGet, "http://example.com/test", nil)
	req.Header.Set(echo.HeaderAccept, echo.MIMETextHTML)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.Set(ContextKeyBrowserRequest, true)
	c.Set(ContextKeyRequestID, "req-123")

	h := &Handlers{}
	if err := h.RenderError(c, errors.New("db password=secret")); err != nil {
		t.Fatalf("RenderError: %v", err)
	}

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d want %d", rec.Code, http.StatusInternalServerError)
	}
	if got := rec.Header().Get(echo.HeaderContentType); got != echo.MIMETextHTMLCharsetUTF8 {
		t.Fatalf("content-type=%q want %q", got, echo.MIMETextHTMLCharsetUTF8)
	}
	if got := rec.Header().Get(HeaderErrorPage); got != "1" {
		t.Fatalf("%s=%q want 1", HeaderErrorPage, got)
	}
	body := rec.Body.String()
	for _, want := range []string{"Something went wrong", "Reference:", "req-123", InternalErrorCode, "Go to dashboard"} {
		if !strings.Contains(body, want) {
			t.Fatalf("response missing %q: %s", want, body)
		}
	}
	if strings.Contains(body, "db password") || strings.Contains(body, "secret") {
		t.Fatalf("response leaked error details: %q", body)
	}
}

func TestRenderErrorRendersStyledPageForBoostedNavigation(t *testing.T) {
	e := echo.New()
	e.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	req := httptest.NewRequest(http.MethodGet, "http://example.com/test", nil)
	req.Header.Set("HX-Request", "true")
	req.Header.Set("HX-Boosted", "true")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.Set(ContextKeyBrowserRequest, true)

	h := &Handlers{}
	if err := h.RenderError(c, errors.New("database unavailable")); err != nil {
		t.Fatalf("RenderError: %v", err)
	}
	if got := rec.Header().Get(HeaderErrorPage); got != "1" {
		t.Fatalf("%s=%q want 1", HeaderErrorPage, got)
	}
	if got := rec.Header().Get(echo.HeaderContentType); got != echo.MIMETextHTMLCharsetUTF8 {
		t.Fatalf("content-type=%q want %q", got, echo.MIMETextHTMLCharsetUTF8)
	}
}

func TestRenderErrorKeepsTargetedHTMXResponsePlain(t *testing.T) {
	e := echo.New()
	e.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	req := httptest.NewRequest(http.MethodGet, "http://example.com/test", nil)
	req.Header.Set(echo.HeaderAccept, echo.MIMETextHTML)
	req.Header.Set("HX-Request", "true")
	req.Header.Set("HX-Target", "results")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.Set(ContextKeyBrowserRequest, true)

	h := &Handlers{}
	if err := h.RenderError(c, errors.New("database unavailable")); err != nil {
		t.Fatalf("RenderError: %v", err)
	}
	if got := rec.Header().Get(HeaderErrorPage); got != "" {
		t.Fatalf("%s=%q want empty", HeaderErrorPage, got)
	}
	if got := rec.Header().Get(echo.HeaderContentType); got != echo.MIMETextPlainCharsetUTF8 {
		t.Fatalf("content-type=%q want %q", got, echo.MIMETextPlainCharsetUTF8)
	}
}

func TestRenderRawErrorKeepsBrowserDownloadResponsePlain(t *testing.T) {
	e := echo.New()
	e.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	req := httptest.NewRequest(http.MethodGet, "http://example.com/export", nil)
	req.Header.Set(echo.HeaderAccept, echo.MIMETextHTML)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.Set(ContextKeyBrowserRequest, true)

	h := &Handlers{}
	if err := h.RenderRawError(c, errors.New("database unavailable")); err != nil {
		t.Fatalf("RenderRawError: %v", err)
	}
	if got := rec.Header().Get(HeaderErrorPage); got != "" {
		t.Fatalf("%s=%q want empty", HeaderErrorPage, got)
	}
	if got := rec.Header().Get(echo.HeaderContentType); got != echo.MIMETextPlainCharsetUTF8 {
		t.Fatalf("content-type=%q want %q", got, echo.MIMETextPlainCharsetUTF8)
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

func TestRenderNotFoundPageMarksFullDocument(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "http://example.com/missing", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	h := &Handlers{}
	if err := h.renderNotFoundPage(c, viewmodels.LayoutData{Title: "Not found"}); err != nil {
		t.Fatalf("renderNotFoundPage: %v", err)
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d want %d", rec.Code, http.StatusNotFound)
	}
	if got := rec.Header().Get(HeaderErrorPage); got != "1" {
		t.Fatalf("%s=%q want 1", HeaderErrorPage, got)
	}
	if got := rec.Header().Get(echo.HeaderContentType); got != echo.MIMETextHTMLCharsetUTF8 {
		t.Fatalf("content-type=%q want %q", got, echo.MIMETextHTMLCharsetUTF8)
	}
	for _, want := range []string{"Page not found", "Go to dashboard"} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("response missing %q: %s", want, rec.Body.String())
		}
	}
}

func TestRenderPageNotFoundKeepsTargetedHTMXResponsePlain(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "http://example.com/missing", nil)
	req.Header.Set(echo.HeaderAccept, echo.MIMETextHTML)
	req.Header.Set("HX-Request", "true")
	req.Header.Set("HX-Target", "results")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	h := &Handlers{}
	if err := h.RenderPageNotFound(c); err != nil {
		t.Fatalf("RenderPageNotFound: %v", err)
	}
	if got := rec.Header().Get(HeaderErrorPage); got != "" {
		t.Fatalf("%s=%q want empty", HeaderErrorPage, got)
	}
	if got := rec.Header().Get(echo.HeaderContentType); got != echo.MIMETextPlainCharsetUTF8 {
		t.Fatalf("content-type=%q want %q", got, echo.MIMETextPlainCharsetUTF8)
	}
}

func TestRenderComponentIgnoresClientCanceledRender(t *testing.T) {
	e := echo.New()
	e.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	req := httptest.NewRequest(http.MethodGet, "http://example.com/test", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	h := &Handlers{}
	component := templ.ComponentFunc(func(context.Context, io.Writer) error {
		return context.Canceled
	})
	if err := h.RenderComponent(c, component); err != nil {
		t.Fatalf("RenderComponent: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want untouched default %d", rec.Code, http.StatusOK)
	}
	if got := rec.Body.String(); got != "" {
		t.Fatalf("body=%q want empty", got)
	}
}

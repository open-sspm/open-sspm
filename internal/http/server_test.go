package httpapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/http/handlers"
)

func TestNewEchoUsesDefaultLogger(t *testing.T) {
	var out bytes.Buffer
	oldDefault := slog.Default()
	t.Cleanup(func() { slog.SetDefault(oldDefault) })
	slog.SetDefault(slog.New(slog.NewJSONHandler(&out, nil)).With("app", "open-sspm", "command", "open-sspm serve"))

	e := newEcho(config.Config{})
	e.Logger.Info("logger wiring check")

	line := strings.TrimSpace(out.String())
	if line == "" {
		t.Fatal("expected logger output")
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(line), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if got := payload["app"]; got != "open-sspm" {
		t.Fatalf("app = %v, want %q", got, "open-sspm")
	}
	if got := payload["command"]; got != "open-sspm serve" {
		t.Fatalf("command = %v, want %q", got, "open-sspm serve")
	}
	if got := payload["component"]; got != "http" {
		t.Fatalf("component = %v, want %q", got, "http")
	}
}

func TestNewEchoUsesTrustedXFFIPExtractor(t *testing.T) {
	e := newEcho(config.Config{})

	if got := realIPForRequest(e, "10.0.0.5:4321", "198.51.100.20, 10.0.0.4"); got != "198.51.100.20" {
		t.Fatalf("real ip = %q, want %q", got, "198.51.100.20")
	}
}

func TestNewEchoIgnoresSpoofedXFFOnDirectRequests(t *testing.T) {
	e := newEcho(config.Config{})

	if got := realIPForRequest(e, "203.0.113.10:4321", "198.51.100.20"); got != "203.0.113.10" {
		t.Fatalf("real ip = %q, want %q", got, "203.0.113.10")
	}
}

func TestNewEchoTrustsConfiguredPublicProxyRange(t *testing.T) {
	e := newEcho(config.Config{TrustedProxyCIDRs: []string{"35.191.0.0/16"}})

	if got := realIPForRequest(e, "35.191.42.10:4321", "198.51.100.20, 35.191.42.10"); got != "198.51.100.20" {
		t.Fatalf("real ip = %q, want %q", got, "198.51.100.20")
	}
}

func realIPForRequest(e *echo.Echo, remoteAddr, forwardedFor string) string {
	req := httptest.NewRequest(http.MethodGet, "http://example.com/test", nil)
	req.RemoteAddr = remoteAddr
	req.Header.Set(echo.HeaderXForwardedFor, forwardedFor)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	return c.RealIP()
}

func TestHTTPErrorHandlerInternalErrorIsGeneric(t *testing.T) {
	e := echo.New()
	e.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	req := httptest.NewRequest(http.MethodGet, "http://example.com/test", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.Set(handlers.ContextKeyRequestID, "req-123")

	es := &EchoServer{h: &handlers.Handlers{}, e: e}
	es.httpErrorHandler(c, errors.New("very sensitive error"))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d want %d", rec.Code, http.StatusInternalServerError)
	}

	body := rec.Body.String()
	if strings.Contains(body, "very sensitive") {
		t.Fatalf("response leaked error details: %q", body)
	}
	if !strings.Contains(body, "Internal server error") {
		t.Fatalf("response missing generic message: %q", body)
	}
	if !strings.Contains(body, "Reference: req-123") {
		t.Fatalf("response missing request reference: %q", body)
	}
	if !strings.Contains(body, "Code: "+handlers.InternalErrorCode) {
		t.Fatalf("response missing error code: %q", body)
	}
}

func TestHTTPErrorHandlerIgnoresRequestCanceled(t *testing.T) {
	e := echo.New()
	e.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	req := httptest.NewRequest(http.MethodGet, "http://example.com/test", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	es := &EchoServer{h: &handlers.Handlers{}, e: e}
	es.httpErrorHandler(c, context.Canceled)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want untouched default %d", rec.Code, http.StatusOK)
	}
	if got := rec.Body.String(); got != "" {
		t.Fatalf("body=%q want empty", got)
	}
}

func TestHTTPErrorHandlerNotFoundDoesNotLeakMessage(t *testing.T) {
	e := echo.New()
	e.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	req := httptest.NewRequest(http.MethodGet, "http://example.com/missing", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	es := &EchoServer{h: &handlers.Handlers{}, e: e}
	es.httpErrorHandler(c, echo.NewHTTPError(http.StatusNotFound, "leaky not found"))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d want %d", rec.Code, http.StatusNotFound)
	}

	body := rec.Body.String()
	if strings.Contains(body, "leaky") {
		t.Fatalf("response leaked error details: %q", body)
	}
	if !strings.Contains(body, "404 page not found") {
		t.Fatalf("response missing not found message: %q", body)
	}
}

func TestHTTPErrorHandlerEchoErrNotFoundUsesNotFoundStatus(t *testing.T) {
	e := echo.New()
	e.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	req := httptest.NewRequest(http.MethodGet, "http://example.com/missing", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	es := &EchoServer{h: &handlers.Handlers{}, e: e}
	es.httpErrorHandler(c, echo.ErrNotFound)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d want %d", rec.Code, http.StatusNotFound)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "404 page not found") {
		t.Fatalf("response missing not found message: %q", body)
	}
}

func TestHTTPStatusFromErrorUsesStatusCoder(t *testing.T) {
	if got := httpStatusFromError(echo.ErrNotFound); got != http.StatusNotFound {
		t.Fatalf("status=%d want %d", got, http.StatusNotFound)
	}
	if got := httpStatusFromError(echo.ErrForbidden); got != http.StatusForbidden {
		t.Fatalf("status=%d want %d", got, http.StatusForbidden)
	}
	if got := httpStatusFromError(errors.New("boom")); got != http.StatusInternalServerError {
		t.Fatalf("status=%d want %d", got, http.StatusInternalServerError)
	}
}

func TestFaviconRedirectSetsHTMLContentType(t *testing.T) {
	e := echo.New()
	e.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	es := &EchoServer{h: &handlers.Handlers{}, e: e}
	es.registerRoutes()

	req := httptest.NewRequest(http.MethodGet, "http://example.com/favicon.ico", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusMovedPermanently {
		t.Fatalf("status=%d want %d", rec.Code, http.StatusMovedPermanently)
	}
	if got := rec.Header().Get(echo.HeaderLocation); got != "/static/favicon.ico" {
		t.Fatalf("location=%q want %q", got, "/static/favicon.ico")
	}
	if got := rec.Header().Get(echo.HeaderContentType); got != echo.MIMETextHTMLCharsetUTF8 {
		t.Fatalf("content-type=%q want %q", got, echo.MIMETextHTMLCharsetUTF8)
	}
}

func TestRegisterRoutesKeepsCapabilityFirstSurface(t *testing.T) {
	e := echo.New()
	e.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	es := &EchoServer{h: &handlers.Handlers{}, e: e}
	es.registerRoutes()

	paths := make(map[string]struct{})
	for _, route := range e.Router().Routes() {
		paths[route.Path] = struct{}{}
	}

	for _, want := range []string{
		"/assigned-apps",
		"/oauth-apps",
		"/non-human-identities",
		"/accounts/okta",
		"/accounts/needs-anchor/github/:org",
		"/app-assets/:id/governance",
	} {
		if _, ok := paths[want]; !ok {
			t.Fatalf("capability-first route %q not registered", want)
		}
	}

	for _, removedRoute := range []string{
		"/apps",
		"/connected-apps",
		"/okta-accounts",
		"/github-users",
	} {
		if _, ok := paths[removedRoute]; ok {
			t.Fatalf("removed route %q still registered", removedRoute)
		}
	}
}

func TestHTTPErrorHandlerBadRequestUsesStatusText(t *testing.T) {
	e := echo.New()
	e.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	req := httptest.NewRequest(http.MethodGet, "http://example.com/bad", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	es := &EchoServer{h: &handlers.Handlers{}, e: e}
	es.httpErrorHandler(c, echo.NewHTTPError(http.StatusBadRequest, "leaky bad request"))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want %d", rec.Code, http.StatusBadRequest)
	}

	body := rec.Body.String()
	if strings.Contains(body, "leaky") {
		t.Fatalf("response leaked error details: %q", body)
	}
	if got := strings.TrimSpace(body); got != http.StatusText(http.StatusBadRequest) {
		t.Fatalf("body=%q want %q", got, http.StatusText(http.StatusBadRequest))
	}
}

func TestStaticServesAssetsAndBlocksTraversal(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	staticDir := filepath.Join(tmpDir, "static")
	if err := os.MkdirAll(staticDir, 0o755); err != nil {
		t.Fatalf("mkdir static dir: %v", err)
	}

	const safeName = "app.txt"
	const safeContent = "safe-content"
	if err := os.WriteFile(filepath.Join(staticDir, safeName), []byte(safeContent), 0o600); err != nil {
		t.Fatalf("write safe file: %v", err)
	}

	const outsideName = "outside.txt"
	const outsideContent = "outside-secret"
	if err := os.WriteFile(filepath.Join(tmpDir, outsideName), []byte(outsideContent), 0o600); err != nil {
		t.Fatalf("write outside file: %v", err)
	}

	e := echo.New()
	e.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	e.Static("/static", staticDir)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://example.com/static/"+safeName, nil)
	e.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("safe static status=%d want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Body.String(); !strings.Contains(got, safeContent) {
		t.Fatalf("safe static body=%q missing %q", got, safeContent)
	}

	tests := []struct {
		name string
		path string
	}{
		{name: "dotdot-slash", path: "/static/../" + outsideName},
		{name: "url-encoded-dotdot-slash", path: "/static/%2e%2e%2f" + outsideName},
		{name: "url-encoded-windows-separator", path: "/static/..%5c..%5c" + outsideName},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "http://example.com"+tt.path, nil)
			e.ServeHTTP(rec, req)

			if rec.Code == http.StatusOK {
				t.Fatalf("traversal path %q unexpectedly returned 200", tt.path)
			}
			if strings.Contains(rec.Body.String(), outsideContent) {
				t.Fatalf("traversal path %q leaked outside file content", tt.path)
			}
		})
	}
}

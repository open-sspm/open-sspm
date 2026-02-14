package httpapp

import (
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
	"github.com/open-sspm/open-sspm/internal/http/handlers"
)

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

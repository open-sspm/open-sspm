package httpapp

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/open-sspm/open-sspm/internal/http/handlers"
)

func TestHTTPErrorHandlerInternalErrorIsGeneric(t *testing.T) {
	e := echo.New()
	e.Logger.SetOutput(io.Discard)

	req := httptest.NewRequest(http.MethodGet, "http://example.com/test", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.Set(handlers.ContextKeyRequestID, "req-123")

	es := &EchoServer{h: &handlers.Handlers{}, e: e}
	es.httpErrorHandler(errors.New("very sensitive error"), c)

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
	e.Logger.SetOutput(io.Discard)

	req := httptest.NewRequest(http.MethodGet, "http://example.com/missing", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	es := &EchoServer{h: &handlers.Handlers{}, e: e}
	es.httpErrorHandler(echo.NewHTTPError(http.StatusNotFound, "leaky not found"), c)

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

func TestHTTPErrorHandlerBadRequestUsesStatusText(t *testing.T) {
	e := echo.New()
	e.Logger.SetOutput(io.Discard)

	req := httptest.NewRequest(http.MethodGet, "http://example.com/bad", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	es := &EchoServer{h: &handlers.Handlers{}, e: e}
	es.httpErrorHandler(echo.NewHTTPError(http.StatusBadRequest, "leaky bad request"), c)

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

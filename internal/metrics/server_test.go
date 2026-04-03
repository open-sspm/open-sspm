package metrics

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsHandlerRefreshesBeforeServing(t *testing.T) {
	t.Parallel()

	called := 0
	handler := metricsHandler(func(context.Context) error {
		called++
		return nil
	})

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if called != 1 {
		t.Fatalf("refresh call count = %d, want 1", called)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if !strings.Contains(rec.Body.String(), "promhttp_metric_handler_requests_in_flight") {
		t.Fatalf("metrics response missing promhttp handler metrics: %s", rec.Body.String())
	}
}

func TestMetricsHandlerContinuesWhenRefreshFails(t *testing.T) {
	t.Parallel()

	handler := metricsHandler(func(context.Context) error {
		return errors.New("boom")
	})

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if !strings.Contains(rec.Body.String(), "promhttp_metric_handler_requests_in_flight") {
		t.Fatalf("metrics response missing promhttp handler metrics: %s", rec.Body.String())
	}
}

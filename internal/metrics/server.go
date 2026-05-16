package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const metricsReadHeaderTimeout = 5 * time.Second
const metricsRefreshTimeout = 5 * time.Second

type RefreshFunc func(context.Context) error

func StartServer(ctx context.Context, addr string, refresh RefreshFunc) (*http.Server, <-chan error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil, nil
	}
	switch strings.ToLower(addr) {
	case "off", "disabled", "false":
		return nil, nil
	}

	if ctx == nil {
		ctx = context.Background()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.Handle("/metrics", metricsHandler(refresh))

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: metricsReadHeaderTimeout,
	}

	errCh := make(chan error, 1)
	go func() {
		slog.Info("metrics listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	return srv, errCh
}

func metricsHandler(refresh RefreshFunc) http.Handler {
	handler := promhttp.Handler()
	if refresh == nil {
		return handler
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		refreshCtx, cancel := context.WithTimeout(r.Context(), metricsRefreshTimeout)
		defer cancel()
		if err := refresh(refreshCtx); err != nil {
			slog.Warn("metrics refresh failed", "err", err)
		}
		handler.ServeHTTP(w, r)
	})
}

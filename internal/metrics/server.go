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

func StartServer(ctx context.Context, addr string) (*http.Server, <-chan error) {
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
	mux.Handle("/metrics", promhttp.Handler())

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

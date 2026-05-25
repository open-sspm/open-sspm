package sync

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/timing"
)

const syncJobSignalRetryDelay = 2 * time.Second

func ListenForSyncJobSignalsWithObserver(ctx context.Context, pool *pgxpool.Pool, channel string, out chan<- struct{}, onConnect func(channel string)) error {
	if pool == nil {
		return errors.New("sync pool is nil")
	}
	if out == nil {
		return errors.New("sync job signal channel is nil")
	}

	channel = strings.TrimSpace(channel)
	if channel == "" {
		return errors.New("sync job signal channel is empty")
	}

	cfg := pool.Config()
	listenSQL := "LISTEN " + pgx.Identifier{channel}.Sanitize()

	for {
		if ctx.Err() != nil {
			return nil
		}

		conn, err := pgx.ConnectConfig(ctx, cfg.ConnConfig.Copy())
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
				return nil
			}
			slog.Warn("sync job listener connect failed", "channel", channel, "err", err)
			if !timing.SleepContext(ctx, syncJobSignalRetryDelay) {
				return nil
			}
			continue
		}
		if onConnect != nil {
			onConnect(channel)
		}

		runErr := listenForSyncJobSignals(ctx, conn, listenSQL, out)

		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = conn.Close(closeCtx)
		cancel()

		if runErr == nil {
			return nil
		}
		if errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) || ctx.Err() != nil {
			return nil
		}
		slog.Warn("sync job listener disconnected; retrying", "channel", channel, "err", runErr)
		if !timing.SleepContext(ctx, syncJobSignalRetryDelay) {
			return nil
		}
	}
}

func listenForSyncJobSignals(ctx context.Context, conn *pgx.Conn, listenSQL string, out chan<- struct{}) error {
	if conn == nil {
		return errors.New("sync job listener connection is nil")
	}
	if _, err := conn.Exec(ctx, listenSQL); err != nil {
		return err
	}

	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		if notification == nil {
			continue
		}
		select {
		case out <- struct{}{}:
		default:
		}
	}
}

package main

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/mailer"
)

type runtimeDependencies struct {
	pool    *pgxpool.Pool
	queries *gen.Queries
	mailer  mailer.Mailer
}

func openRuntimeDependencies(ctx context.Context, cfg config.Config) (*runtimeDependencies, error) {
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	queries := gen.New(pool)
	connectorConfigs := configstore.NewStore(pool, queries, cfg.ConnectorSecretKey)
	if err := connectorConfigs.Bootstrap(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	mailAdapter, err := openMailer(cfg)
	if err != nil {
		pool.Close()
		return nil, err
	}
	return &runtimeDependencies{
		pool:    pool,
		queries: queries,
		mailer:  mailAdapter,
	}, nil
}

func openMailer(cfg config.Config) (mailer.Mailer, error) {
	if !cfg.SMTP.Enabled {
		return mailer.NewNoop(), nil
	}
	return mailer.NewSMTP(mailer.SMTPConfig{
		Host:        cfg.SMTP.Host,
		Port:        cfg.SMTP.Port,
		Username:    cfg.SMTP.Username,
		Password:    cfg.SMTP.Password,
		FromAddress: cfg.SMTP.FromAddress,
		FromName:    cfg.SMTP.FromName,
		TLSMode:     cfg.SMTP.TLSMode,
	})
}

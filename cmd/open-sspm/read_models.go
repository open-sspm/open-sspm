package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/readmodels"
)

type startupReadModelsAction string

const (
	startupReadModelsActionRefreshConnectorState startupReadModelsAction = "refresh_connector_source_state"
	startupReadModelsActionRebuildAll            startupReadModelsAction = "rebuild_all_read_models"
)

func rebuildStoredReadModels(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, cfg config.Config) error {
	projector := readmodels.NewProjector(pool, q, readmodels.RefreshConfigFromConfig(cfg))

	needsRebuild := false
	if cfg.StartupReadModelRebuildMode == config.StartupReadModelRebuildAuto {
		var err error
		needsRebuild, err = projector.StoredReadModelsNeedRebuild(ctx)
		if err != nil {
			return err
		}
	}

	action, err := chooseStartupReadModelsAction(cfg.StartupReadModelRebuildMode, needsRebuild)
	if err != nil {
		return err
	}

	switch action {
	case startupReadModelsActionRefreshConnectorState:
		slog.Info("refreshing connector source state and policy-backed risk read models on startup")
		if err := projector.RefreshConnectorSourceState(ctx); err != nil {
			return err
		}
		if err := projector.RefreshAllSaaSAppRiskReadModels(ctx); err != nil {
			return err
		}
		if err := projector.RefreshAllCredentialArtifactRiskReadModels(ctx); err != nil {
			return err
		}
		return projector.RefreshAllNonHumanPrincipalReadModels(ctx)
	case startupReadModelsActionRebuildAll:
		reason := "forced"
		if needsRebuild {
			reason = "missing_projections"
		}
		slog.Info("rebuilding stored read models on startup", "reason", reason)
		return projector.RebuildAllReadModels(ctx)
	default:
		return fmt.Errorf("unsupported startup read models action %q", action)
	}
}

func chooseStartupReadModelsAction(mode string, needsRebuild bool) (startupReadModelsAction, error) {
	switch mode {
	case config.StartupReadModelRebuildAlways:
		return startupReadModelsActionRebuildAll, nil
	case config.StartupReadModelRebuildAuto:
		if needsRebuild {
			return startupReadModelsActionRebuildAll, nil
		}
		return startupReadModelsActionRefreshConnectorState, nil
	default:
		return "", fmt.Errorf("unsupported startup read model rebuild mode %q", mode)
	}
}

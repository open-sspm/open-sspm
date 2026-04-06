package readmodels

import (
	"time"

	"github.com/open-sspm/open-sspm/internal/config"
)

// RefreshConfig carries only the sync interval overrides needed to compute
// connector freshness windows for read-model refreshes.
type RefreshConfig struct {
	SyncInterval                time.Duration
	SyncOktaInterval            time.Duration
	SyncEntraInterval           time.Duration
	SyncGoogleWorkspaceInterval time.Duration
	SyncGitHubInterval          time.Duration
	SyncDatadogInterval         time.Duration
	SyncAWSInterval             time.Duration
}

func RefreshConfigFromConfig(cfg config.Config) RefreshConfig {
	return RefreshConfig{
		SyncInterval:                cfg.SyncInterval,
		SyncOktaInterval:            cfg.SyncOktaInterval,
		SyncEntraInterval:           cfg.SyncEntraInterval,
		SyncGoogleWorkspaceInterval: cfg.SyncGoogleWorkspaceInterval,
		SyncGitHubInterval:          cfg.SyncGitHubInterval,
		SyncDatadogInterval:         cfg.SyncDatadogInterval,
		SyncAWSInterval:             cfg.SyncAWSInterval,
	}
}

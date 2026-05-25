package registry

import (
	"strings"
)

// ConnectorState represents the runtime state of a connector.
type ConnectorState struct {
	Definition  ConnectorDefinition
	Config      any // Decoded, normalized config
	ConfigError string
	Enabled     bool
	Configured  bool
	SourceName  string
	Metrics     *ConnectorMetrics // nil if not fetched
}

// StatusLabel returns the human-readable status label.
func (s *ConnectorState) StatusLabel() string {
	if strings.TrimSpace(s.ConfigError) != "" {
		return "Invalid config"
	}
	if !s.Configured {
		return "Not configured"
	}
	if !s.Enabled {
		return "Disabled"
	}
	return "Enabled"
}

// CoverageScore calculates the sync/coverage score (0-100).
func (s *ConnectorState) CoverageScore() int {
	if s.Metrics == nil {
		// Fallback logic for when metrics aren't loaded or available
		if s.Configured {
			return 30 // Base score for being configured
		}
		return 0
	}

	// Special case for Okta (IdP)
	if s.Definition.Role() == RoleIdP {
		if s.Metrics.Total > 0 || s.Metrics.Extras["apps"] > 0 {
			return 100
		}
		if s.Configured {
			return 30
		}
		return 0
	}

	// Standard coverage calculation for apps
	if s.Metrics.Total <= 0 {
		return 0
	}
	score := int((s.Metrics.Anchored * 100) / s.Metrics.Total)
	if score < 0 {
		return 0
	}
	if score > 100 {
		return 100
	}
	return score
}

// ScoreLabel returns the label for the score.
func (s *ConnectorState) ScoreLabel() string {
	if s.Definition.Role() == RoleIdP {
		return "Sync score"
	}
	return "Identity coverage"
}

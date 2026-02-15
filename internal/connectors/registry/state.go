package registry

import (
	"fmt"
	"strings"

	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
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

// StatusClass returns the CSS class for the status badge.
func (s *ConnectorState) StatusClass() string {
	if strings.TrimSpace(s.ConfigError) != "" {
		return "badge bg-rose-100 text-rose-800 dark:bg-rose-900/50 dark:text-rose-100"
	}
	if !s.Configured {
		return "badge bg-amber-100 text-amber-800 dark:bg-amber-900/50 dark:text-amber-100"
	}
	if !s.Enabled {
		return "badge bg-amber-100 text-amber-800 dark:bg-amber-900/50 dark:text-amber-100"
	}
	return "badge bg-emerald-100 text-emerald-800 dark:bg-emerald-900/50 dark:text-emerald-100"
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
	score := int((s.Metrics.Matched * 100) / s.Metrics.Total)
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

// Subtitle returns the appropriate subtitle based on configuration.
func (s *ConnectorState) Subtitle() string {
	if s.Configured {
		return s.Definition.ConfiguredSubtitle(s.Config)
	}
	return s.Definition.DefaultSubtitle()
}

// MetricsKV returns key-value pairs for the metrics section.
func (s *ConnectorState) MetricsKV() []viewmodels.GlobalViewKV {
	if s.Metrics == nil {
		return []viewmodels.GlobalViewKV{
			{Label: "Status", Value: s.StatusLabel()},
			{Label: "Accounts", Value: "—"},
			{Label: "Unmanaged", Value: "—"},
		}
	}

	if s.Definition.Role() == RoleIdP {
		return []viewmodels.GlobalViewKV{
			{Label: "Users", Value: views.FormatInt64(s.Metrics.Total)},
			{Label: "Apps", Value: views.FormatInt64(s.Metrics.Extras["apps"])},
			{Label: "Status", Value: s.StatusLabel()},
		}
	}

	return []viewmodels.GlobalViewKV{
		{Label: "Accounts", Value: views.FormatInt64(s.Metrics.Total)},
		{Label: "Managed", Value: views.FormatInt64(s.Metrics.Matched)},
		{Label: "Unmanaged", Value: views.FormatInt64(s.Metrics.Unmatched)},
	}
}

// HighlightsKV returns key-value pairs for the highlights section.
func (s *ConnectorState) HighlightsKV() []viewmodels.GlobalViewKV {
	if s.Metrics == nil {
		return []viewmodels.GlobalViewKV{
			{Label: "Connector", Value: s.StatusLabel()},
			{Label: "Explore", Value: "—"},
		}
	}

	if s.Definition.Role() == RoleIdP {
		domain := s.SourceName
		if domain == "" {
			domain = "—"
		}
		return []viewmodels.GlobalViewKV{
			{Label: "Connector", Value: s.StatusLabel()},
			{Label: "Domain", Value: domain},
			{Label: "Explore", Value: "Users + Apps"},
		}
	}

	return []viewmodels.GlobalViewKV{
		{Label: "Coverage", Value: fmt.Sprintf("%d%%", s.CoverageScore())},
		{Label: "Managed", Value: views.FormatInt64(s.Metrics.Matched)},
		{Label: "Unmanaged", Value: views.FormatInt64(s.Metrics.Unmatched)},
	}
}

// PrimaryHref returns the primary action link.
func (s *ConnectorState) PrimaryHref() string {
	if s.Configured && s.Enabled {
		if href := connectorBrowseUsersHref(s.Definition.Kind()); href != "" {
			return href
		}
	}
	return s.Definition.SettingsHref()
}

// PrimaryLabel returns the primary action label.
func (s *ConnectorState) PrimaryLabel() string {
	if s.Configured && s.Enabled {
		return "Browse users"
	}
	return "Configure"
}

// SecondaryHref returns the secondary action link.
func (s *ConnectorState) SecondaryHref() string {
	if s.Configured && s.Enabled {
		return connectorUnmanagedHref(s.Definition.Kind(), s.SourceName)
	}
	return ""
}

// SecondaryLabel returns the secondary action label.
func (s *ConnectorState) SecondaryLabel() string {
	if s.Configured && s.Enabled {
		return connectorSecondaryLabel(s.Definition.Kind())
	}
	return ""
}

func connectorBrowseUsersHref(kind string) string {
	switch strings.TrimSpace(kind) {
	case "okta":
		return "/idp-users"
	case "entra":
		return "/entra-users"
	case "github":
		return "/github-users"
	case "datadog":
		return "/datadog-users"
	case "aws_identity_center":
		return "/aws-users"
	default:
		return ""
	}
}

func connectorUnmanagedHref(kind, sourceName string) string {
	switch strings.TrimSpace(kind) {
	case "okta":
		return "/apps"
	case "entra":
		return "/unmatched/entra"
	case "github", "datadog":
		sourceName = strings.TrimSpace(sourceName)
		if sourceName != "" {
			return fmt.Sprintf("/unmatched/%s/%s", kind, sourceName)
		}
	case "aws_identity_center":
		return "/unmatched/aws"
	}
	return ""
}

func connectorSecondaryLabel(kind string) string {
	switch strings.TrimSpace(kind) {
	case "okta":
		return "Browse apps"
	case "entra", "github", "datadog", "aws_identity_center":
		return "Unmanaged"
	default:
		return ""
	}
}

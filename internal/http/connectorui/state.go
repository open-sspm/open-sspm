package connectorui

import (
	"fmt"
	"strings"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

type StatePresenter struct {
	state registry.ConnectorState
}

func NewStatePresenter(state registry.ConnectorState) StatePresenter {
	return StatePresenter{state: state}
}

func (p StatePresenter) Subtitle() string {
	if p.state.Configured {
		return p.state.Definition.ConfiguredSubtitle(p.state.Config)
	}
	return p.state.Definition.DefaultSubtitle()
}

func (p StatePresenter) StatusClass() string {
	if strings.TrimSpace(p.state.ConfigError) != "" {
		return "badge bg-rose-100 text-rose-800 dark:bg-rose-900/50 dark:text-rose-100"
	}
	if !p.state.Configured {
		return "badge bg-amber-100 text-amber-800 dark:bg-amber-900/50 dark:text-amber-100"
	}
	if !p.state.Enabled {
		return "badge bg-amber-100 text-amber-800 dark:bg-amber-900/50 dark:text-amber-100"
	}
	return "badge bg-emerald-100 text-emerald-800 dark:bg-emerald-900/50 dark:text-emerald-100"
}

func (p StatePresenter) MetricsKV() []viewmodels.GlobalViewKV {
	if p.state.Metrics == nil {
		return []viewmodels.GlobalViewKV{
			{Label: "Status", Value: p.state.StatusLabel()},
			{Label: "Accounts", Value: "—"},
			{Label: "Needs anchor", Value: "—"},
		}
	}

	if p.state.Definition.Role() == registry.RoleIdP {
		return []viewmodels.GlobalViewKV{
			{Label: "Users", Value: views.FormatInt64(p.state.Metrics.Total)},
			{Label: "Apps", Value: views.FormatInt64(p.state.Metrics.Extras["apps"])},
			{Label: "Status", Value: p.state.StatusLabel()},
		}
	}

	return []viewmodels.GlobalViewKV{
		{Label: "Accounts", Value: views.FormatInt64(p.state.Metrics.Total)},
		{Label: "Anchored", Value: views.FormatInt64(p.state.Metrics.Anchored)},
		{Label: "Needs anchor", Value: views.FormatInt64(p.state.Metrics.NeedsAnchor)},
	}
}

func (p StatePresenter) HighlightsKV() []viewmodels.GlobalViewKV {
	if p.state.Metrics == nil {
		return []viewmodels.GlobalViewKV{
			{Label: "Connector", Value: p.state.StatusLabel()},
			{Label: "Explore", Value: "—"},
		}
	}

	if p.state.Definition.Role() == registry.RoleIdP {
		domain := strings.TrimSpace(p.state.SourceName)
		if domain == "" {
			domain = "—"
		}
		return []viewmodels.GlobalViewKV{
			{Label: "Connector", Value: p.state.StatusLabel()},
			{Label: "Domain", Value: domain},
			{Label: "Explore", Value: "Users + Apps"},
		}
	}

	return []viewmodels.GlobalViewKV{
		{Label: "Coverage", Value: fmt.Sprintf("%d%%", p.state.CoverageScore())},
		{Label: "Anchored", Value: views.FormatInt64(p.state.Metrics.Anchored)},
		{Label: "Needs anchor", Value: views.FormatInt64(p.state.Metrics.NeedsAnchor)},
	}
}

func (p StatePresenter) PrimaryHref() string {
	if p.state.Configured && p.state.Enabled {
		if href := connectorBrowseUsersHref(p.state.Definition.Kind()); href != "" {
			return href
		}
	}
	return p.state.Definition.SettingsHref()
}

func (p StatePresenter) PrimaryLabel() string {
	if p.state.Configured && p.state.Enabled {
		return "Browse users"
	}
	return "Configure"
}

func (p StatePresenter) SecondaryHref() string {
	if p.state.Configured && p.state.Enabled {
		return connectorNeedsAnchorHref(p.state.Definition.Kind(), p.state.SourceName)
	}
	return ""
}

func (p StatePresenter) SecondaryLabel() string {
	if p.state.Configured && p.state.Enabled {
		return connectorSecondaryLabel(p.state.Definition.Kind())
	}
	return ""
}

func connectorBrowseUsersHref(kind string) string {
	switch strings.TrimSpace(kind) {
	case "okta":
		return "/accounts/okta"
	case "entra":
		return "/accounts/entra"
	case "google_workspace":
		return "/accounts/google-workspace"
	case "github":
		return "/accounts/github"
	case "datadog":
		return "/accounts/datadog"
	case "aws_identity_center":
		return "/accounts/aws"
	default:
		return ""
	}
}

func connectorNeedsAnchorHref(kind, sourceName string) string {
	switch strings.TrimSpace(kind) {
	case "okta":
		return "/assigned-apps"
	case "entra":
		return "/accounts/needs-anchor/entra"
	case "google_workspace":
		return "/accounts/needs-anchor/google-workspace"
	case "github", "datadog":
		sourceName = strings.TrimSpace(sourceName)
		if sourceName != "" {
			return fmt.Sprintf("/accounts/needs-anchor/%s/%s", kind, sourceName)
		}
	case "aws_identity_center":
		return "/accounts/needs-anchor/aws"
	}
	return ""
}

func connectorSecondaryLabel(kind string) string {
	switch strings.TrimSpace(kind) {
	case "okta":
		return "Browse apps"
	case "entra", "google_workspace", "github", "datadog", "aws_identity_center":
		return "Needs anchor"
	default:
		return ""
	}
}

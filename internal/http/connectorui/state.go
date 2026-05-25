package connectorui

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
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
	return connectorSubtitle(p.kind(), p.state.Config, p.state.Configured)
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

	if p.role() == registry.RoleIdP {
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

	if p.role() == registry.RoleIdP {
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
		if href := connectorBrowseUsersHref(p.kind()); href != "" {
			return href
		}
	}
	return SettingsHrefForKind(p.kind())
}

func (p StatePresenter) PrimaryLabel() string {
	if p.state.Configured && p.state.Enabled {
		return "Browse users"
	}
	return "Configure"
}

func (p StatePresenter) SecondaryHref() string {
	if p.state.Configured && p.state.Enabled {
		return connectorNeedsAnchorHref(p.kind(), p.state.SourceName)
	}
	return ""
}

func (p StatePresenter) SecondaryLabel() string {
	if p.state.Configured && p.state.Enabled {
		return connectorSecondaryLabel(p.kind())
	}
	return ""
}

func (p StatePresenter) kind() string {
	if p.state.Definition == nil {
		return ""
	}
	return strings.TrimSpace(p.state.Definition.Kind())
}

func (p StatePresenter) role() registry.IntegrationRole {
	if p.state.Definition == nil {
		return ""
	}
	return p.state.Definition.Role()
}

func SettingsHrefForKind(kind string) string {
	kind = strings.TrimSpace(kind)
	if kind == "" {
		return "/settings/connectors"
	}
	return "/settings/connectors?open=" + url.QueryEscape(kind)
}

func connectorSubtitle(kind string, cfg any, configured bool) string {
	if configured {
		if subtitle := configuredSubtitle(kind, cfg); subtitle != "" {
			return subtitle
		}
	}
	return defaultSubtitle(kind)
}

func configuredSubtitle(kind string, cfg any) string {
	switch strings.TrimSpace(kind) {
	case configstore.KindOkta:
		if c, ok := cfg.(configstore.OktaConfig); ok && strings.TrimSpace(c.Domain) != "" {
			return "Domain " + strings.TrimSpace(c.Domain)
		}
	case configstore.KindGoogleWorkspace:
		if c, ok := cfg.(configstore.GoogleWorkspaceConfig); ok {
			if strings.TrimSpace(c.PrimaryDomain) != "" {
				return "Domain " + strings.TrimSpace(c.PrimaryDomain)
			}
			if strings.TrimSpace(c.CustomerID) != "" {
				return "Customer " + strings.TrimSpace(c.CustomerID)
			}
		}
	case configstore.KindEntra:
		if c, ok := cfg.(configstore.EntraConfig); ok && strings.TrimSpace(c.TenantID) != "" {
			return "Tenant " + strings.TrimSpace(c.TenantID)
		}
	case configstore.KindGitHub:
		if c, ok := cfg.(configstore.GitHubConfig); ok && strings.TrimSpace(c.Org) != "" {
			return "Org " + strings.TrimSpace(c.Org)
		}
	case configstore.KindDatadog:
		if c, ok := cfg.(configstore.DatadogConfig); ok && strings.TrimSpace(c.Site) != "" {
			return "Site " + strings.TrimSpace(c.Site)
		}
	case configstore.KindAWSIdentityCenter:
		if c, ok := cfg.(configstore.AWSIdentityCenterConfig); ok {
			name := strings.TrimSpace(c.Name)
			if name == "" {
				name = strings.TrimSpace(c.Region)
			}
			if name != "" {
				return "Instance " + name
			}
		}
	case configstore.KindVault:
		if c, ok := cfg.(configstore.VaultConfig); ok {
			source := strings.TrimSpace(c.Normalized().SourceName())
			if source != "" {
				return "Source " + source
			}
		}
	}
	return ""
}

func defaultSubtitle(kind string) string {
	switch strings.TrimSpace(kind) {
	case configstore.KindOkta:
		return "Syncs Okta users and app assignments."
	case configstore.KindGoogleWorkspace:
		return "Users, groups, OAuth grants, and token audits from Google Workspace."
	case configstore.KindEntra:
		return "Users and access via Microsoft Graph."
	case configstore.KindGitHub:
		return "Organization members and permissions."
	case configstore.KindDatadog:
		return "Users and roles."
	case configstore.KindAWSIdentityCenter:
		return "Account assignments via permission sets."
	case configstore.KindVault:
		return "Identity entities, policies, mounts, and auth roles."
	default:
		return "Configure connector."
	}
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

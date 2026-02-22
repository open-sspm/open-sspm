// Package handlers contains HTTP handler logic split by domain.
package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/a-h/templ"
	"github.com/alexedwards/scs/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/authn"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

const (
	// ContextKeyRequestID stores the request id (X-Request-ID) for logging and client error references.
	ContextKeyRequestID = "request_id"

	// InternalErrorCode is a stable error code safe to return to clients.
	InternalErrorCode = "INTERNAL_ERROR"
)

// SyncRunner is the interface for triggering manual syncs.
type SyncRunner interface {
	RunOnce(context.Context) error
}

// Handlers groups all HTTP handlers and shared dependencies.
type Handlers struct {
	Cfg      config.Config
	Q        *gen.Queries
	Pool     *pgxpool.Pool
	Sessions *scs.SessionManager
	Syncer   SyncRunner
	Registry *registry.ConnectorRegistry
}

// ConnectorSnapshot holds the current connector configuration state.
type ConnectorSnapshot struct {
	Okta                        configstore.OktaConfig
	OktaEnabled                 bool
	OktaConfigured              bool
	GoogleWorkspace             configstore.GoogleWorkspaceConfig
	GoogleWorkspaceEnabled      bool
	GoogleWorkspaceConfigured   bool
	GitHub                      configstore.GitHubConfig
	GitHubEnabled               bool
	GitHubConfigured            bool
	Datadog                     configstore.DatadogConfig
	DatadogEnabled              bool
	DatadogConfigured           bool
	AWSIdentityCenter           configstore.AWSIdentityCenterConfig
	AWSIdentityCenterEnabled    bool
	AWSIdentityCenterConfigured bool
	Entra                       configstore.EntraConfig
	EntraEnabled                bool
	EntraConfigured             bool
	Vault                       configstore.VaultConfig
	VaultEnabled                bool
	VaultConfigured             bool
}

// LoadConnectorSnapshot retrieves the current connector configuration.
func (h *Handlers) LoadConnectorSnapshot(ctx context.Context) (ConnectorSnapshot, error) {
	states, err := h.Registry.LoadStates(ctx, h.Q)
	if err != nil {
		return ConnectorSnapshot{}, err
	}

	var snap ConnectorSnapshot
	for _, state := range states {
		switch state.Definition.Kind() {
		case configstore.KindOkta:
			if cfg, ok := state.Config.(configstore.OktaConfig); ok {
				snap.Okta = cfg
				snap.OktaEnabled = state.Enabled
				snap.OktaConfigured = state.Configured
			}
		case configstore.KindGoogleWorkspace:
			if cfg, ok := state.Config.(configstore.GoogleWorkspaceConfig); ok {
				snap.GoogleWorkspace = cfg
				snap.GoogleWorkspaceEnabled = state.Enabled
				snap.GoogleWorkspaceConfigured = state.Configured
			}
		case configstore.KindGitHub:
			if cfg, ok := state.Config.(configstore.GitHubConfig); ok {
				snap.GitHub = cfg
				snap.GitHubEnabled = state.Enabled
				snap.GitHubConfigured = state.Configured
			}
		case configstore.KindDatadog:
			if cfg, ok := state.Config.(configstore.DatadogConfig); ok {
				snap.Datadog = cfg
				snap.DatadogEnabled = state.Enabled
				snap.DatadogConfigured = state.Configured
			}
		case configstore.KindAWSIdentityCenter:
			if cfg, ok := state.Config.(configstore.AWSIdentityCenterConfig); ok {
				snap.AWSIdentityCenter = cfg
				snap.AWSIdentityCenterEnabled = state.Enabled
				snap.AWSIdentityCenterConfigured = state.Configured
			}
		case configstore.KindEntra:
			if cfg, ok := state.Config.(configstore.EntraConfig); ok {
				snap.Entra = cfg
				snap.EntraEnabled = state.Enabled
				snap.EntraConfigured = state.Configured
			}
		case configstore.KindVault:
			if cfg, ok := state.Config.(configstore.VaultConfig); ok {
				snap.Vault = cfg
				snap.VaultEnabled = state.Enabled
				snap.VaultConfigured = state.Configured
			}
		}
	}

	return snap, nil
}

// LayoutData builds the common layout data for page rendering.
func (h *Handlers) LayoutData(ctx context.Context, c *echo.Context, title string) (viewmodels.LayoutData, ConnectorSnapshot, error) {
	snap, err := h.LoadConnectorSnapshot(ctx)
	if err != nil {
		return viewmodels.LayoutData{}, snap, err
	}
	principal, ok := authn.PrincipalFromContext(c)
	csrfToken, _ := c.Get(middleware.DefaultCSRFConfig.ContextKey).(string)
	awsName := strings.TrimSpace(snap.AWSIdentityCenter.Name)
	if awsName == "" {
		awsName = strings.TrimSpace(snap.AWSIdentityCenter.Region)
	}
	rulesets, err := h.Q.ListRulesets(ctx)
	if err != nil {
		return viewmodels.LayoutData{}, snap, err
	}
	findingsRulesets := make([]viewmodels.FindingsRulesetItem, 0, len(rulesets))
	for _, ruleset := range rulesets {
		rulesetKey := strings.TrimSpace(ruleset.Key)
		if rulesetKey == "" {
			continue
		}
		rulesetName := strings.TrimSpace(ruleset.Name)
		if rulesetName == "" {
			rulesetName = rulesetKey
		}

		findingsRulesets = append(findingsRulesets, viewmodels.FindingsRulesetItem{
			Key:           rulesetKey,
			Name:          rulesetName,
			ConnectorKind: strings.TrimSpace(ruleset.ConnectorKind.String),
			Status:        strings.TrimSpace(ruleset.Status),
			Href:          "/findings/rulesets/" + rulesetKey,
		})
	}

	commandUsersRaw, err := h.Q.ListIdPUsersForCommand(ctx)
	if err != nil {
		return viewmodels.LayoutData{}, snap, err
	}
	commandUsers := make([]viewmodels.DashboardCommandUserItem, 0, len(commandUsersRaw))
	for _, u := range commandUsersRaw {
		status := strings.TrimSpace(u.Status)
		if status == "" {
			status = "—"
		}
		commandUsers = append(commandUsers, viewmodels.DashboardCommandUserItem{
			ID:          u.ID,
			Email:       strings.TrimSpace(u.Email),
			DisplayName: strings.TrimSpace(u.DisplayName),
			Status:      status,
		})
	}

	commandAppsRaw, err := h.Q.ListOktaAppsForCommand(ctx)
	if err != nil {
		return viewmodels.LayoutData{}, snap, err
	}
	commandApps := make([]viewmodels.DashboardCommandAppItem, 0, len(commandAppsRaw))
	for _, app := range commandAppsRaw {
		label := strings.TrimSpace(app.Label)
		if label == "" {
			label = strings.TrimSpace(app.ExternalID)
		}
		status := strings.TrimSpace(app.Status)
		if status == "" {
			status = "—"
		}
		commandApps = append(commandApps, viewmodels.DashboardCommandAppItem{
			ExternalID: strings.TrimSpace(app.ExternalID),
			Label:      label,
			Name:       strings.TrimSpace(app.Name),
			Status:     status,
		})
	}

	layout := viewmodels.LayoutData{
		Title:                       title,
		CSRFToken:                   csrfToken,
		UserEmail:                   principal.Email,
		UserRole:                    principal.Role,
		IsAdmin:                     ok && principal.IsAdmin(),
		FindingsRulesets:            findingsRulesets,
		GoogleWorkspaceCustomerID:   snap.GoogleWorkspace.CustomerID,
		GoogleWorkspaceEnabled:      snap.GoogleWorkspaceEnabled,
		GoogleWorkspaceConfigured:   snap.GoogleWorkspaceConfigured,
		GitHubOrg:                   snap.GitHub.Org,
		GitHubEnabled:               snap.GitHubEnabled,
		GitHubConfigured:            snap.GitHubConfigured,
		DatadogSite:                 snap.Datadog.Site,
		DatadogEnabled:              snap.DatadogEnabled,
		DatadogConfigured:           snap.DatadogConfigured,
		AWSIdentityCenterName:       awsName,
		AWSIdentityCenterEnabled:    snap.AWSIdentityCenterEnabled,
		AWSIdentityCenterConfigured: snap.AWSIdentityCenterConfigured,
		EntraTenantID:               snap.Entra.TenantID,
		EntraEnabled:                snap.EntraEnabled,
		EntraConfigured:             snap.EntraConfigured,
		Toast:                       popFlashToast(c),
		ActivePath:                  c.Request().URL.Path,
		CommandUsers:                commandUsers,
		CommandApps:                 commandApps,
	}
	return layout, snap, nil
}

// RenderComponent renders a templ component as the response.
func (h *Handlers) RenderComponent(c *echo.Context, component templ.Component) error {
	c.Response().Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := component.Render(c.Request().Context(), c.Response()); err != nil {
		return h.RenderError(c, err)
	}
	return nil
}

// RenderError returns a plain text error response.
func (h *Handlers) RenderError(c *echo.Context, err error) error {
	requestID, _ := c.Get(ContextKeyRequestID).(string)
	path := ""
	if req := c.Request(); req != nil && req.URL != nil {
		path = req.URL.Path
	}
	method := ""
	if req := c.Request(); req != nil {
		method = req.Method
	}
	c.Logger().Error("http error",
		"request_id", requestID,
		"method", method,
		"path", path,
		"ip", c.RealIP(),
		"error", err,
	)

	msg := "Internal server error."
	if requestID != "" {
		msg = fmt.Sprintf("%s Reference: %s.", msg, requestID)
	}
	msg = fmt.Sprintf("%s Code: %s.", msg, InternalErrorCode)
	c.Response().Header().Set(echo.HeaderContentType, echo.MIMETextPlainCharsetUTF8)
	return c.String(http.StatusInternalServerError, msg)
}

// RenderNotFound returns a 404 response.
func RenderNotFound(c *echo.Context) error {
	c.Response().Header().Set(echo.HeaderContentType, echo.MIMETextPlainCharsetUTF8)
	return c.String(http.StatusNotFound, "404 page not found")
}

// NormalizeConnectorKind normalizes connector kind strings.
func NormalizeConnectorKind(kind string) string {
	kind = strings.ToLower(strings.TrimSpace(kind))
	switch kind {
	case "aws":
		return configstore.KindAWSIdentityCenter
	default:
		return kind
	}
}

// ConnectorDisplayName returns the human-readable name for a connector kind.
func ConnectorDisplayName(kind string) string {
	switch NormalizeConnectorKind(kind) {
	case configstore.KindOkta:
		return "Okta"
	case configstore.KindGoogleWorkspace:
		return "Google Workspace"
	case configstore.KindGitHub:
		return "GitHub"
	case configstore.KindDatadog:
		return "Datadog"
	case configstore.KindAWSIdentityCenter:
		return "AWS Identity Center"
	case configstore.KindEntra:
		return "Microsoft Entra ID"
	case configstore.KindVault:
		return "Vault"
	default:
		return ""
	}
}

// IsKnownConnectorKind checks if the kind is a recognized connector.
func IsKnownConnectorKind(kind string) bool {
	switch NormalizeConnectorKind(kind) {
	case configstore.KindOkta, configstore.KindGoogleWorkspace, configstore.KindGitHub, configstore.KindDatadog, configstore.KindAWSIdentityCenter, configstore.KindEntra, configstore.KindVault:
		return true
	default:
		return false
	}
}

// IntegratedAppHref returns the navigation href for an integrated app.
func IntegratedAppHref(integrationKind string) string {
	switch NormalizeConnectorKind(integrationKind) {
	case configstore.KindGitHub:
		return "/github-users"
	case configstore.KindDatadog:
		return "/datadog-users"
	case configstore.KindAWSIdentityCenter:
		return "/aws-users"
	case configstore.KindEntra:
		return "/entra-users"
	case configstore.KindGoogleWorkspace:
		return "/google-workspace/users"
	default:
		return ""
	}
}

// ParseBoolForm parses a form value as a boolean.
func ParseBoolForm(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// SummarizeProfilePermissions extracts permission badges from a profile JSON.
func SummarizeProfilePermissions(profileJSON []byte) []viewmodels.PermissionBadge {
	if len(profileJSON) == 0 {
		return nil
	}
	var profile map[string]any
	if err := json.Unmarshal(profileJSON, &profile); err != nil {
		return nil
	}
	if len(profile) == 0 {
		return nil
	}
	keys := make([]string, 0, len(profile))
	for key, value := range profile {
		if key == "" {
			continue
		}
		if isSensitiveProfileKey(key) {
			continue
		}
		if !isScalarProfileValue(value) {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) > 5 {
		keys = keys[:5]
	}
	badges := make([]viewmodels.PermissionBadge, 0, len(keys))
	for _, key := range keys {
		value := profile[key]
		text := fmt.Sprintf("%s: %s", key, truncateProfileValue(fmt.Sprint(value), 80))
		badges = append(badges, viewmodels.PermissionBadge{Text: text})
	}
	return badges
}

func isScalarProfileValue(value any) bool {
	switch value.(type) {
	case string, bool, float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, json.Number:
		return true
	default:
		return false
	}
}

func isSensitiveProfileKey(key string) bool {
	lower := strings.ToLower(key)
	sensitive := []string{
		"token",
		"secret",
		"password",
		"passphrase",
		"private",
		"clientsecret",
		"refresh",
		"bearer",
		"credential",
		"accesskey",
	}
	for _, fragment := range sensitive {
		if strings.Contains(lower, fragment) {
			return true
		}
	}
	return false
}

func truncateProfileValue(value string, max int) string {
	if max <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= max {
		return value
	}
	return string(runes[:max]) + "..."
}

// DedupeStrings removes duplicate consecutive strings from a sorted slice.
func DedupeStrings(sorted []string) []string {
	if len(sorted) == 0 {
		return nil
	}
	out := make([]string, 0, len(sorted))
	for _, s := range sorted {
		if len(out) > 0 && out[len(out)-1] == s {
			continue
		}
		out = append(out, s)
	}
	return out
}

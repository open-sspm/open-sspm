package handlers

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
	"github.com/open-sspm/open-sspm/internal/sync"
)

// HandleSettings renders the settings page.
func (h *Handlers) HandleSettings(c echo.Context) error {
	layout, _, err := h.LayoutData(c.Request().Context(), c, "Settings", "Settings")
	if err != nil {
		return h.RenderError(c, err)
	}
	status := c.QueryParam("resync")
	var banner *viewmodels.ResyncBanner

	switch status {
	case "success":
		banner = &viewmodels.ResyncBanner{
			Class:   "alert-success",
			Title:   "Resync complete",
			Message: "The data sync finished successfully.",
		}
	case "error":
		banner = &viewmodels.ResyncBanner{
			Class:   "alert-error",
			Title:   "Resync failed",
			Message: "Check server logs for details.",
		}
	case "disabled":
		banner = &viewmodels.ResyncBanner{
			Class:   "alert-warning",
			Title:   "Resync unavailable",
			Message: "Sync is not configured on this server.",
		}
	}

	data := viewmodels.SettingsViewData{
		Layout:        layout,
		SyncInterval:  h.Cfg.SyncInterval.String(),
		ResyncEnabled: h.Syncer != nil,
		ResyncBanner:  banner,
	}

	return h.RenderComponent(c, views.SettingsPage(data))
}

// HandleConnectors renders the connectors page.
func (h *Handlers) HandleConnectors(c echo.Context) error {
	if c.Request().Method != http.MethodGet {
		return c.NoContent(http.StatusMethodNotAllowed)
	}
	openKind := NormalizeConnectorKind(c.QueryParam("open"))
	if !IsKnownConnectorKind(openKind) {
		openKind = ""
	}
	savedKind := NormalizeConnectorKind(c.QueryParam("saved"))
	return h.renderConnectorsPage(c, openKind, savedKind, nil)
}

// HandleConnectorAction routes connector save and toggle actions.
func (h *Handlers) HandleConnectorAction(c echo.Context) error {
	if c.Request().Method != http.MethodPost {
		return c.NoContent(http.StatusMethodNotAllowed)
	}
	suffix := strings.Trim(c.Param("*"), "/")
	if suffix == "" {
		return RenderNotFound(c)
	}
	parts := strings.Split(suffix, "/")
	kind := NormalizeConnectorKind(parts[0])
	if !IsKnownConnectorKind(kind) {
		return RenderNotFound(c)
	}
	if len(parts) == 1 {
		return h.handleConnectorSave(c, kind)
	}
	if len(parts) == 2 && parts[1] == "toggle" {
		return h.handleConnectorToggle(c, kind)
	}
	return RenderNotFound(c)
}

func (h *Handlers) handleConnectorToggle(c echo.Context, kind string) error {
	enabled := ParseBoolForm(c.FormValue("enabled"))
	ctx := c.Request().Context()
	cfg, err := h.Q.GetConnectorConfig(ctx, kind)
	if err != nil {
		return h.RenderError(c, err)
	}
	if enabled {
		if err := validateConnectorConfig(kind, cfg.Config); err != nil {
			alert := &viewmodels.ConnectorAlert{
				Class:   "alert-error",
				Title:   ConnectorDisplayName(kind) + " not enabled",
				Message: err.Error(),
			}
			return h.renderConnectorsPage(c, kind, "", alert)
		}
	}
	if _, err := h.Q.UpdateConnectorConfigEnabled(ctx, gen.UpdateConnectorConfigEnabledParams{Kind: kind, Enabled: enabled}); err != nil {
		return h.RenderError(c, err)
	}
	return c.Redirect(http.StatusSeeOther, "/settings/connectors?saved="+kind)
}

func (h *Handlers) handleConnectorSave(c echo.Context, kind string) error {
	ctx := c.Request().Context()
	cfgRow, err := h.Q.GetConnectorConfig(ctx, kind)
	if err != nil {
		return h.RenderError(c, err)
	}

	var raw []byte
	switch kind {
	case configstore.KindOkta:
		current, err := configstore.DecodeOktaConfig(cfgRow.Config)
		if err != nil {
			return h.RenderError(c, err)
		}
		update := configstore.OktaConfig{
			Domain: c.FormValue("domain"),
			Token:  c.FormValue("token"),
		}
		merged := configstore.MergeOktaConfig(current, update).Normalized()
		if cfgRow.Enabled {
			if err := merged.Validate(); err != nil {
				return h.renderConnectorsPage(c, kind, "", connectorAlert(err))
			}
		}
		raw, err = configstore.EncodeConfig(merged)
		if err != nil {
			return h.RenderError(c, err)
		}
	case configstore.KindGitHub:
		current, err := configstore.DecodeGitHubConfig(cfgRow.Config)
		if err != nil {
			return h.RenderError(c, err)
		}
		update := configstore.GitHubConfig{
			Org:         c.FormValue("org"),
			APIBase:     c.FormValue("api_base"),
			Enterprise:  c.FormValue("enterprise"),
			Token:       c.FormValue("token"),
			SCIMEnabled: ParseBoolForm(c.FormValue("scim_enabled")),
		}
		merged := configstore.MergeGitHubConfig(current, update).Normalized()
		if cfgRow.Enabled {
			if err := merged.Validate(); err != nil {
				return h.renderConnectorsPage(c, kind, "", connectorAlert(err))
			}
		}
		raw, err = configstore.EncodeConfig(merged)
		if err != nil {
			return h.RenderError(c, err)
		}
	case configstore.KindDatadog:
		current, err := configstore.DecodeDatadogConfig(cfgRow.Config)
		if err != nil {
			return h.RenderError(c, err)
		}
		update := configstore.DatadogConfig{
			Site:   c.FormValue("site"),
			APIKey: c.FormValue("api_key"),
			AppKey: c.FormValue("app_key"),
		}
		merged := configstore.MergeDatadogConfig(current, update).Normalized()
		if cfgRow.Enabled {
			if err := merged.Validate(); err != nil {
				return h.renderConnectorsPage(c, kind, "", connectorAlert(err))
			}
		}
		raw, err = configstore.EncodeConfig(merged)
		if err != nil {
			return h.RenderError(c, err)
		}
	case configstore.KindAWSIdentityCenter:
		current, err := configstore.DecodeAWSIdentityCenterConfig(cfgRow.Config)
		if err != nil {
			return h.RenderError(c, err)
		}
		update := configstore.AWSIdentityCenterConfig{
			Region:          c.FormValue("region"),
			Name:            c.FormValue("name"),
			InstanceARN:     c.FormValue("instance_arn"),
			IdentityStoreID: c.FormValue("identity_store_id"),
			AuthType:        c.FormValue("auth_type"),
			AccessKeyID:     c.FormValue("access_key_id"),
			SecretAccessKey: c.FormValue("secret_access_key"),
			SessionToken:    c.FormValue("session_token"),
		}
		merged := configstore.MergeAWSIdentityCenterConfig(current, update).Normalized()
		if cfgRow.Enabled {
			if err := merged.Validate(); err != nil {
				return h.renderConnectorsPage(c, kind, "", connectorAlert(err))
			}
		}
		raw, err = configstore.EncodeConfig(merged)
		if err != nil {
			return h.RenderError(c, err)
		}
	case configstore.KindEntra:
		current, err := configstore.DecodeEntraConfig(cfgRow.Config)
		if err != nil {
			return h.RenderError(c, err)
		}
		update := configstore.EntraConfig{
			TenantID:     c.FormValue("tenant_id"),
			ClientID:     c.FormValue("client_id"),
			ClientSecret: c.FormValue("client_secret"),
		}
		merged := configstore.MergeEntraConfig(current, update).Normalized()
		if cfgRow.Enabled {
			if err := merged.Validate(); err != nil {
				return h.renderConnectorsPage(c, kind, "", connectorAlert(err))
			}
		}
		raw, err = configstore.EncodeConfig(merged)
		if err != nil {
			return h.RenderError(c, err)
		}
	case configstore.KindVault:
		raw = cfgRow.Config
	default:
		return RenderNotFound(c)
	}

	if _, err := h.Q.UpdateConnectorConfig(ctx, gen.UpdateConnectorConfigParams{Kind: kind, Config: raw}); err != nil {
		return h.RenderError(c, err)
	}
	return c.Redirect(http.StatusSeeOther, "/settings/connectors?saved="+kind)
}

func (h *Handlers) renderConnectorsPage(c echo.Context, openKind, savedKind string, alert *viewmodels.ConnectorAlert) error {
	data, err := h.buildConnectorsViewData(c.Request().Context(), c, openKind, savedKind, alert)
	if err != nil {
		return h.RenderError(c, err)
	}
	return h.RenderComponent(c, views.ConnectorsPage(data))
}

func (h *Handlers) buildConnectorsViewData(ctx context.Context, c echo.Context, openKind, savedKind string, alert *viewmodels.ConnectorAlert) (viewmodels.ConnectorsViewData, error) {
	states, err := h.Registry.LoadStates(ctx, h.Q)
	if err != nil {
		return viewmodels.ConnectorsViewData{}, err
	}

	var data viewmodels.ConnectorsViewData

	// Populate connector-specific view data
	for _, state := range states {
		switch state.Definition.Kind() {
		case configstore.KindOkta:
			if cfg, ok := state.Config.(configstore.OktaConfig); ok {
				data.Okta = viewmodels.OktaConnectorViewData{
					Enabled:     state.Enabled,
					Configured:  state.Configured,
					Domain:      cfg.Domain,
					TokenMasked: configstore.MaskSecret(cfg.Token),
					HasToken:    cfg.Token != "",
				}
			}
		case configstore.KindGitHub:
			if cfg, ok := state.Config.(configstore.GitHubConfig); ok {
				data.GitHub = viewmodels.GitHubConnectorViewData{
					Enabled:     state.Enabled,
					Configured:  state.Configured,
					Org:         cfg.Org,
					APIBase:     cfg.APIBase,
					Enterprise:  cfg.Enterprise,
					SCIMEnabled: cfg.SCIMEnabled,
					TokenMasked: configstore.MaskSecret(cfg.Token),
					HasToken:    cfg.Token != "",
				}
			}
		case configstore.KindDatadog:
			if cfg, ok := state.Config.(configstore.DatadogConfig); ok {
				data.Datadog = viewmodels.DatadogConnectorViewData{
					Enabled:      state.Enabled,
					Configured:   state.Configured,
					Site:         cfg.Site,
					APIKeyMasked: configstore.MaskSecret(cfg.APIKey),
					AppKeyMasked: configstore.MaskSecret(cfg.AppKey),
					HasAPIKey:    cfg.APIKey != "",
					HasAppKey:    cfg.AppKey != "",
				}
			}
		case configstore.KindAWSIdentityCenter:
			if cfg, ok := state.Config.(configstore.AWSIdentityCenterConfig); ok {
				data.AWSIdentityCenter = viewmodels.AWSIdentityCenterConnectorViewData{
					Enabled:          state.Enabled,
					Configured:       state.Configured,
					Region:           cfg.Region,
					Name:             cfg.Name,
					InstanceARN:      cfg.InstanceARN,
					IdentityStoreID:  cfg.IdentityStoreID,
					AuthType:         cfg.AuthType,
					AccessKeyIDMask:  configstore.MaskSecret(cfg.AccessKeyID),
					HasAccessKeyID:   cfg.AccessKeyID != "",
					SecretKeyMask:    configstore.MaskSecret(cfg.SecretAccessKey),
					HasSecretKey:     cfg.SecretAccessKey != "",
					SessionTokenMask: configstore.MaskSecret(cfg.SessionToken),
					HasSessionToken:  cfg.SessionToken != "",
				}
			}
		case configstore.KindEntra:
			if cfg, ok := state.Config.(configstore.EntraConfig); ok {
				data.Entra = viewmodels.EntraConnectorViewData{
					Enabled:            state.Enabled,
					Configured:         state.Configured,
					TenantID:           cfg.TenantID,
					ClientID:           cfg.ClientID,
					ClientSecretMasked: configstore.MaskSecret(cfg.ClientSecret),
					HasClientSecret:    cfg.ClientSecret != "",
				}
			}
		case configstore.KindVault:
			data.Vault = viewmodels.VaultConnectorViewData{
				Enabled:    state.Enabled,
				Configured: state.Configured,
			}
		}
	}

	csrfToken, _ := c.Get(middleware.DefaultCSRFConfig.ContextKey).(string)
	awsName := strings.TrimSpace(data.AWSIdentityCenter.Name)
	if awsName == "" {
		awsName = strings.TrimSpace(data.AWSIdentityCenter.Region)
	}
	layout := viewmodels.LayoutData{
		Title:                       "Connectors",
		CSRFToken:                   csrfToken,
		GitHubOrg:                   data.GitHub.Org,
		GitHubEnabled:               data.GitHub.Enabled,
		GitHubConfigured:            data.GitHub.Configured,
		DatadogSite:                 data.Datadog.Site,
		DatadogEnabled:              data.Datadog.Enabled,
		DatadogConfigured:           data.Datadog.Configured,
		AWSIdentityCenterName:       awsName,
		AWSIdentityCenterEnabled:    data.AWSIdentityCenter.Enabled,
		AWSIdentityCenterConfigured: data.AWSIdentityCenter.Configured,
		EntraTenantID:               data.Entra.TenantID,
		EntraEnabled:                data.Entra.Enabled,
		EntraConfigured:             data.Entra.Configured,
		NavbarEndBadge:              "Settings",
		ActivePath:                  c.Request().URL.Path,
	}

	if !IsKnownConnectorKind(openKind) {
		openKind = ""
	}

	savedName := ConnectorDisplayName(savedKind)

	data.Layout = layout
	data.Alert = alert
	data.SavedName = savedName
	data.OpenKind = openKind

	return data, nil
}

func connectorAlert(err error) *viewmodels.ConnectorAlert {
	return &viewmodels.ConnectorAlert{
		Class:   "alert-error",
		Title:   "Validation error",
		Message: err.Error(),
	}
}

func validateConnectorConfig(kind string, raw []byte) error {
	switch NormalizeConnectorKind(kind) {
	case configstore.KindOkta:
		cfg, err := configstore.DecodeOktaConfig(raw)
		if err != nil {
			return err
		}
		return cfg.Normalized().Validate()
	case configstore.KindGitHub:
		cfg, err := configstore.DecodeGitHubConfig(raw)
		if err != nil {
			return err
		}
		return cfg.Normalized().Validate()
	case configstore.KindDatadog:
		cfg, err := configstore.DecodeDatadogConfig(raw)
		if err != nil {
			return err
		}
		return cfg.Normalized().Validate()
	case configstore.KindAWSIdentityCenter:
		cfg, err := configstore.DecodeAWSIdentityCenterConfig(raw)
		if err != nil {
			return err
		}
		return cfg.Normalized().Validate()
	case configstore.KindEntra:
		cfg, err := configstore.DecodeEntraConfig(raw)
		if err != nil {
			return err
		}
		return cfg.Normalized().Validate()
	case configstore.KindVault:
		return nil
	default:
		return errors.New("unknown connector")
	}
}

// HandleResync triggers a manual resync.
func (h *Handlers) HandleResync(c echo.Context) error {
	if c.Request().Method != http.MethodPost {
		return c.NoContent(http.StatusMethodNotAllowed)
	}
	if h.Syncer == nil {
		return c.Redirect(http.StatusSeeOther, "/settings?resync=disabled")
	}
	if err := h.Syncer.RunOnce(c.Request().Context()); err != nil {
		if errors.Is(err, sync.ErrNoEnabledConnectors) {
			return c.Redirect(http.StatusSeeOther, "/settings?resync=disabled")
		}
		return c.Redirect(http.StatusSeeOther, "/settings?resync=error")
	}
	return c.Redirect(http.StatusSeeOther, "/settings?resync=success")
}

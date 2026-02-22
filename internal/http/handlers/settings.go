package handlers

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
	"github.com/open-sspm/open-sspm/internal/identity"
	"github.com/open-sspm/open-sspm/internal/sync"
)

// HandleSettings renders the settings page.
func (h *Handlers) HandleSettings(c *echo.Context) error {
	layout, _, err := h.LayoutData(c.Request().Context(), c, "Settings")
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
	case "queued":
		banner = &viewmodels.ResyncBanner{
			Class:   "alert-success",
			Title:   "Resync queued",
			Message: "A worker will pick up the sync shortly.",
		}
	case "busy":
		banner = &viewmodels.ResyncBanner{
			Class:   "alert-warning",
			Title:   "Resync already running",
			Message: "A sync is already in progress. Try again shortly.",
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
func (h *Handlers) HandleConnectors(c *echo.Context) error {
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
func (h *Handlers) HandleConnectorAction(c *echo.Context) error {
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
	if len(parts) == 2 {
		switch parts[1] {
		case "toggle":
			return h.handleConnectorToggle(c, kind)
		case "authoritative":
			return h.handleConnectorAuthoritativeToggle(c, kind)
		}
	}
	return RenderNotFound(c)
}

func (h *Handlers) handleConnectorToggle(c *echo.Context, kind string) error {
	addVary(c, "HX-Request")

	enabled := ParseBoolForm(c.FormValue("enabled"))
	ctx := c.Request().Context()
	cfg, err := h.Q.GetConnectorConfig(ctx, kind)
	if err != nil {
		return h.RenderError(c, err)
	}
	if enabled {
		if err := validateConnectorConfig(kind, cfg.Config); err != nil {
			if isHX(c) {
				setFlashToast(c, viewmodels.ToastViewData{
					Category:    "error",
					Title:       ConnectorDisplayName(kind) + " not enabled",
					Description: err.Error(),
				})
				setHXRedirect(c, "/settings/connectors?open="+kind)
				return c.NoContent(http.StatusOK)
			}
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
	if isHX(c) {
		data, err := h.buildConnectorsViewData(ctx, c, "", "", nil)
		if err != nil {
			return h.RenderError(c, err)
		}
		return h.renderConnectorRow(c, kind, data)
	}
	return c.Redirect(http.StatusSeeOther, "/settings/connectors?saved="+kind)
}

func (h *Handlers) handleConnectorSave(c *echo.Context, kind string) error {
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
			Domain:           c.FormValue("domain"),
			Token:            c.FormValue("token"),
			DiscoveryEnabled: ParseBoolForm(c.FormValue("discovery_enabled")),
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
			TenantID:         c.FormValue("tenant_id"),
			ClientID:         c.FormValue("client_id"),
			ClientSecret:     c.FormValue("client_secret"),
			DiscoveryEnabled: ParseBoolForm(c.FormValue("discovery_enabled")),
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
		current, err := configstore.DecodeVaultConfig(cfgRow.Config)
		if err != nil {
			return h.RenderError(c, err)
		}
		update := configstore.VaultConfig{
			Address:          c.FormValue("address"),
			Namespace:        c.FormValue("namespace"),
			Name:             c.FormValue("name"),
			AuthType:         c.FormValue("auth_type"),
			Token:            c.FormValue("token"),
			AppRoleMountPath: c.FormValue("approle_mount_path"),
			AppRoleRoleID:    c.FormValue("approle_role_id"),
			AppRoleSecretID:  c.FormValue("approle_secret_id"),
			ScanAuthRoles:    ParseBoolForm(c.FormValue("scan_auth_roles")),
			TLSSkipVerify:    ParseBoolForm(c.FormValue("tls_skip_verify")),
			TLSCACertPEM:     c.FormValue("tls_ca_cert_pem"),
		}
		merged := configstore.MergeVaultConfig(current, update).Normalized()
		if cfgRow.Enabled {
			if err := merged.Validate(); err != nil {
				return h.renderConnectorsPage(c, kind, "", connectorAlert(err))
			}
		}
		raw, err = configstore.EncodeConfig(merged)
		if err != nil {
			return h.RenderError(c, err)
		}
	default:
		return RenderNotFound(c)
	}

	if _, err := h.Q.UpdateConnectorConfig(ctx, gen.UpdateConnectorConfigParams{Kind: kind, Config: raw}); err != nil {
		return h.RenderError(c, err)
	}
	return c.Redirect(http.StatusSeeOther, "/settings/connectors?saved="+kind)
}

func (h *Handlers) handleConnectorAuthoritativeToggle(c *echo.Context, kind string) error {
	addVary(c, "HX-Request")

	kind = NormalizeConnectorKind(kind)
	if kind != configstore.KindOkta && kind != configstore.KindEntra {
		return RenderNotFound(c)
	}

	ctx := c.Request().Context()
	sourceName, err := h.authoritativeSourceName(ctx, kind)
	if err != nil {
		if isHX(c) {
			setFlashToast(c, viewmodels.ToastViewData{
				Category:    "error",
				Title:       "Authoritative source unavailable",
				Description: err.Error(),
			})
			setHXRedirect(c, "/settings/connectors?open="+kind)
			return c.NoContent(http.StatusOK)
		}
		return h.renderConnectorsPage(c, kind, "", connectorAlert(err))
	}

	enabled := ParseBoolForm(c.FormValue("authoritative"))
	if _, err := h.Q.UpsertIdentitySourceSetting(ctx, gen.UpsertIdentitySourceSettingParams{
		SourceKind:      kind,
		SourceName:      sourceName,
		IsAuthoritative: enabled,
	}); err != nil {
		return h.RenderError(c, err)
	}

	if _, err := identity.Resolve(ctx, h.Q); err != nil {
		return h.RenderError(c, err)
	}

	if isHX(c) {
		data, err := h.buildConnectorsViewData(ctx, c, "", "", nil)
		if err != nil {
			return h.RenderError(c, err)
		}
		return h.renderConnectorRow(c, kind, data)
	}
	return c.Redirect(http.StatusSeeOther, "/settings/connectors?saved="+kind)
}

func (h *Handlers) renderConnectorsPage(c *echo.Context, openKind, savedKind string, alert *viewmodels.ConnectorAlert) error {
	data, err := h.buildConnectorsViewData(c.Request().Context(), c, openKind, savedKind, alert)
	if err != nil {
		return h.RenderError(c, err)
	}
	return h.RenderComponent(c, views.ConnectorsPage(data))
}

func (h *Handlers) renderConnectorRow(c *echo.Context, kind string, data viewmodels.ConnectorsViewData) error {
	switch NormalizeConnectorKind(kind) {
	case configstore.KindOkta:
		return h.RenderComponent(c, views.OktaConnectorRow(data))
	case configstore.KindEntra:
		return h.RenderComponent(c, views.EntraConnectorRow(data))
	case configstore.KindGitHub:
		return h.RenderComponent(c, views.GitHubConnectorRow(data))
	case configstore.KindDatadog:
		return h.RenderComponent(c, views.DatadogConnectorRow(data))
	case configstore.KindAWSIdentityCenter:
		return h.RenderComponent(c, views.AWSIdentityCenterConnectorRow(data))
	case configstore.KindVault:
		return h.RenderComponent(c, views.VaultConnectorRow(data))
	default:
		return RenderNotFound(c)
	}
}

func (h *Handlers) buildConnectorsViewData(ctx context.Context, c *echo.Context, openKind, savedKind string, alert *viewmodels.ConnectorAlert) (viewmodels.ConnectorsViewData, error) {
	states, err := h.Registry.LoadStates(ctx, h.Q)
	if err != nil {
		return viewmodels.ConnectorsViewData{}, err
	}
	sourceSettings, err := h.Q.ListIdentitySourceSettings(ctx)
	if err != nil {
		return viewmodels.ConnectorsViewData{}, err
	}
	authoritativeBySource := make(map[string]bool, len(sourceSettings))
	for _, source := range sourceSettings {
		authoritativeBySource[sourceKey(source.SourceKind, source.SourceName)] = source.IsAuthoritative
	}

	var data viewmodels.ConnectorsViewData

	// Populate connector-specific view data
	for _, state := range states {
		switch state.Definition.Kind() {
		case configstore.KindOkta:
			if cfg, ok := state.Config.(configstore.OktaConfig); ok {
				sourceName := strings.TrimSpace(cfg.Domain)
				if sourceName == "" {
					sourceName = "okta"
				}
				authoritative, exists := authoritativeBySource[sourceKey(configstore.KindOkta, sourceName)]
				if !exists && state.Configured {
					authoritative = true
				}
				data.Okta = viewmodels.OktaConnectorViewData{
					Enabled:          state.Enabled,
					Configured:       state.Configured,
					Domain:           cfg.Domain,
					TokenMasked:      configstore.MaskSecret(cfg.Token),
					HasToken:         cfg.Token != "",
					DiscoveryEnabled: cfg.DiscoveryEnabled,
					Authoritative:    authoritative,
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
				sourceName := strings.TrimSpace(cfg.TenantID)
				authoritative := authoritativeBySource[sourceKey(configstore.KindEntra, sourceName)]
				data.Entra = viewmodels.EntraConnectorViewData{
					Enabled:            state.Enabled,
					Configured:         state.Configured,
					TenantID:           cfg.TenantID,
					ClientID:           cfg.ClientID,
					ClientSecretMasked: configstore.MaskSecret(cfg.ClientSecret),
					HasClientSecret:    cfg.ClientSecret != "",
					DiscoveryEnabled:   cfg.DiscoveryEnabled,
					Authoritative:      authoritative,
				}
			}
		case configstore.KindVault:
			if cfg, ok := state.Config.(configstore.VaultConfig); ok {
				cfg = cfg.Normalized()
				data.Vault = viewmodels.VaultConnectorViewData{
					Enabled:             state.Enabled,
					Configured:          state.Configured,
					Address:             cfg.Address,
					Namespace:           cfg.Namespace,
					Name:                cfg.Name,
					AuthType:            cfg.AuthType,
					TokenMasked:         configstore.MaskSecret(cfg.Token),
					HasToken:            cfg.Token != "",
					AppRoleMountPath:    cfg.AppRoleMountPath,
					AppRoleRoleID:       cfg.AppRoleRoleID,
					HasAppRoleRoleID:    cfg.AppRoleRoleID != "",
					AppRoleSecretMasked: configstore.MaskSecret(cfg.AppRoleSecretID),
					HasAppRoleSecretID:  cfg.AppRoleSecretID != "",
					ScanAuthRoles:       cfg.ScanAuthRoles,
					TLSSkipVerify:       cfg.TLSSkipVerify,
					HasTLSCACert:        cfg.TLSCACertPEM != "",
				}
			}
		}
	}

	layout, _, err := h.LayoutData(ctx, c, "Connectors")
	if err != nil {
		return viewmodels.ConnectorsViewData{}, err
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
		cfg, err := configstore.DecodeVaultConfig(raw)
		if err != nil {
			return err
		}
		return cfg.Normalized().Validate()
	default:
		return errors.New("unknown connector")
	}
}

func (h *Handlers) authoritativeSourceName(ctx context.Context, kind string) (string, error) {
	cfg, err := h.Q.GetConnectorConfig(ctx, kind)
	if err != nil {
		return "", err
	}

	switch NormalizeConnectorKind(kind) {
	case configstore.KindOkta:
		oktaCfg, err := configstore.DecodeOktaConfig(cfg.Config)
		if err != nil {
			return "", err
		}
		sourceName := strings.TrimSpace(oktaCfg.Normalized().Domain)
		if sourceName == "" {
			sourceName = "okta"
		}
		return sourceName, nil
	case configstore.KindEntra:
		entraCfg, err := configstore.DecodeEntraConfig(cfg.Config)
		if err != nil {
			return "", err
		}
		sourceName := strings.TrimSpace(entraCfg.Normalized().TenantID)
		if sourceName == "" {
			return "", errors.New("configure the Entra tenant ID before enabling authoritative mode")
		}
		return sourceName, nil
	default:
		return "", errors.New("connector does not support authoritative identity settings")
	}
}

func sourceKey(kind, name string) string {
	return strings.ToLower(strings.TrimSpace(kind)) + "::" + strings.ToLower(strings.TrimSpace(name))
}

// HandleResync triggers a manual resync.
func (h *Handlers) HandleResync(c *echo.Context) error {
	if c.Request().Method != http.MethodPost {
		return c.NoContent(http.StatusMethodNotAllowed)
	}
	if h.Syncer == nil {
		return c.Redirect(http.StatusSeeOther, "/settings?resync=disabled")
	}
	if err := h.Syncer.RunOnce(c.Request().Context()); err != nil {
		if errors.Is(err, sync.ErrSyncQueued) {
			return c.Redirect(http.StatusSeeOther, "/settings?resync=queued")
		}
		if errors.Is(err, sync.ErrSyncAlreadyRunning) {
			return c.Redirect(http.StatusSeeOther, "/settings?resync=busy")
		}
		if errors.Is(err, sync.ErrNoEnabledConnectors) {
			return c.Redirect(http.StatusSeeOther, "/settings?resync=disabled")
		}
		return c.Redirect(http.StatusSeeOther, "/settings?resync=error")
	}
	return c.Redirect(http.StatusSeeOther, "/settings?resync=success")
}

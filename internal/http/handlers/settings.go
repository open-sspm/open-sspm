package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/a-h/templ"
	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/events"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
	"github.com/open-sspm/open-sspm/internal/identity"
	"github.com/open-sspm/open-sspm/internal/readmodels"
	"github.com/open-sspm/open-sspm/internal/riskpolicy"
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
		Layout:                layout,
		SyncInterval:          h.Cfg.SyncInterval.String(),
		SyncDiscoveryInterval: h.Cfg.SyncDiscoveryInterval.String(),
		SyncDiscoveryEnabled:  h.Cfg.SyncDiscoveryEnabled,
		ResyncEnabled:         h.Syncer != nil,
		ResyncBanner:          banner,
		RiskPolicyPacks:       riskPolicyPackSummaries(h.RiskPolicies),
		RiskPolicyExpressions: riskPolicyExpressionCount(h.RiskPolicies),
	}

	return h.RenderComponent(c, views.SettingsPage(data))
}

func riskPolicyPackSummaries(registry *riskpolicy.Registry) []viewmodels.RiskPolicyPackSummary {
	if registry == nil {
		return nil
	}
	metadatas := registry.PackMetadatas()
	summaries := make([]viewmodels.RiskPolicyPackSummary, 0, len(metadatas))
	for _, metadata := range metadatas {
		summaries = append(summaries, viewmodels.RiskPolicyPackSummary{
			Domain:  string(metadata.Domain),
			ID:      metadata.ID,
			Version: metadata.Version,
		})
	}
	sort.SliceStable(summaries, func(i, j int) bool {
		if summaries[i].Domain != summaries[j].Domain {
			return summaries[i].Domain < summaries[j].Domain
		}
		return summaries[i].ID < summaries[j].ID
	})
	return summaries
}

func riskPolicyExpressionCount(registry *riskpolicy.Registry) int {
	if registry == nil {
		return 0
	}
	return registry.CompiledExpressionCount()
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

// HandleConnectorDialog renders a lazy-loaded connector configuration dialog.
func (h *Handlers) HandleConnectorDialog(c *echo.Context) error {
	if c.Request().Method != http.MethodGet {
		return c.NoContent(http.StatusMethodNotAllowed)
	}
	kind := NormalizeConnectorKind(c.Param("kind"))
	if !IsKnownConnectorKind(kind) {
		return RenderNotFound(c)
	}
	return h.renderConnectorDialog(c, kind, nil, http.StatusOK)
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

type connectorSaveDefinition struct {
	buildMergedConfig func(c *echo.Context, raw []byte) (any, error)
	validateConfig    func(cfg any) error
}

func newConnectorSaveDefinition[T any](
	decode func([]byte) (T, error),
	readUpdate func(*echo.Context) T,
	merge func(T, T) T,
	normalize func(T) T,
	validate func(T) error,
) connectorSaveDefinition {
	return connectorSaveDefinition{
		buildMergedConfig: func(c *echo.Context, raw []byte) (any, error) {
			current, err := decode(raw)
			if err != nil {
				return nil, err
			}
			return normalize(merge(current, readUpdate(c))), nil
		},
		validateConfig: func(cfg any) error {
			return validate(cfg.(T))
		},
	}
}

var connectorSaveDefinitions = map[string]connectorSaveDefinition{
	configstore.KindOkta: newConnectorSaveDefinition(
		configstore.DecodeOktaConfig,
		readOktaConfigUpdate,
		configstore.MergeOktaConfig,
		configstore.OktaConfig.Normalized,
		configstore.OktaConfig.Validate,
	),
	configstore.KindGoogleWorkspace: newConnectorSaveDefinition(
		configstore.DecodeGoogleWorkspaceConfig,
		readGoogleWorkspaceConfigUpdate,
		configstore.MergeGoogleWorkspaceConfig,
		configstore.GoogleWorkspaceConfig.Normalized,
		configstore.GoogleWorkspaceConfig.Validate,
	),
	configstore.KindGitHub: newConnectorSaveDefinition(
		configstore.DecodeGitHubConfig,
		readGitHubConfigUpdate,
		configstore.MergeGitHubConfig,
		configstore.GitHubConfig.Normalized,
		configstore.GitHubConfig.Validate,
	),
	configstore.KindDatadog: newConnectorSaveDefinition(
		configstore.DecodeDatadogConfig,
		readDatadogConfigUpdate,
		configstore.MergeDatadogConfig,
		configstore.DatadogConfig.Normalized,
		configstore.DatadogConfig.Validate,
	),
	configstore.KindAWSIdentityCenter: newConnectorSaveDefinition(
		configstore.DecodeAWSIdentityCenterConfig,
		readAWSIdentityCenterConfigUpdate,
		configstore.MergeAWSIdentityCenterConfig,
		configstore.AWSIdentityCenterConfig.Normalized,
		configstore.AWSIdentityCenterConfig.Validate,
	),
	configstore.KindEntra: newConnectorSaveDefinition(
		configstore.DecodeEntraConfig,
		readEntraConfigUpdate,
		configstore.MergeEntraConfig,
		configstore.EntraConfig.Normalized,
		configstore.EntraConfig.Validate,
	),
	configstore.KindVault: newConnectorSaveDefinition(
		configstore.DecodeVaultConfig,
		readVaultConfigUpdate,
		configstore.MergeVaultConfig,
		configstore.VaultConfig.Normalized,
		configstore.VaultConfig.Validate,
	),
}

func (h *Handlers) handleConnectorToggle(c *echo.Context, kind string) error {
	addVary(c, "HX-Request")

	enabled := ParseBoolForm(c.FormValue("enabled"))
	ctx := c.Request().Context()
	cfg, err := h.connectorConfigStore().GetResolvedConnectorConfig(ctx, kind)
	if err != nil {
		return h.RenderError(c, err)
	}
	if enabled {
		if err := validateConnectorConfig(kind, cfg.ResolvedConfig); err != nil {
			if isHX(c) {
				setHXToast(c, viewmodels.ToastViewData{
					Category:    "error",
					Title:       ConnectorDisplayName(kind) + " not enabled",
					Description: err.Error(),
				})
				data, dataErr := h.buildConnectorsViewData(ctx, c, "", "", nil)
				if dataErr != nil {
					return h.RenderError(c, dataErr)
				}
				return h.renderConnectorRowStatus(c, kind, data, http.StatusUnprocessableEntity)
			}
			alert := &viewmodels.ConnectorAlert{
				Class:   "alert-error",
				Title:   ConnectorDisplayName(kind) + " not enabled",
				Message: err.Error(),
			}
			return h.renderConnectorsPage(c, kind, "", alert)
		}
	}
	if err := h.WithTx(ctx, func(qtx *gen.Queries) error {
		if _, err := qtx.UpdateConnectorConfigEnabled(ctx, gen.UpdateConnectorConfigEnabledParams{Kind: kind, Enabled: enabled}); err != nil {
			return err
		}
		projector := readmodels.NewProjector(nil, qtx, readmodels.RefreshConfigFromConfig(h.Cfg))
		if err := projector.RefreshConnectorSourceState(ctx); err != nil {
			return err
		}
		return projector.RefreshAllSaaSAppRiskReadModels(ctx)
	}); err != nil {
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
	configStore := h.connectorConfigStore()
	cfgRow, err := configStore.GetResolvedConnectorConfig(ctx, kind)
	if err != nil {
		return h.RenderError(c, err)
	}

	definition, ok := connectorSaveDefinitions[kind]
	if !ok {
		return RenderNotFound(c)
	}
	mergedConfig, err := definition.buildMergedConfig(c, cfgRow.ResolvedConfig)
	if err != nil {
		return h.RenderError(c, err)
	}
	if cfgRow.Row.Enabled {
		if err := definition.validateConfig(mergedConfig); err != nil {
			if isHX(c) {
				return h.renderConnectorDialog(c, kind, connectorAlert(err), http.StatusUnprocessableEntity)
			}
			return h.renderConnectorsPage(c, kind, "", connectorAlert(err))
		}
	}

	if err := h.WithTx(ctx, func(qtx *gen.Queries) error {
		if err := configStore.SaveConnectorConfigTx(ctx, qtx, kind, mergedConfig); err != nil {
			return err
		}
		projector := readmodels.NewProjector(nil, qtx, readmodels.RefreshConfigFromConfig(h.Cfg))
		if err := projector.RefreshConnectorSourceState(ctx); err != nil {
			return err
		}
		return projector.RefreshAllSaaSAppRiskReadModels(ctx)
	}); err != nil {
		if errors.Is(err, configstore.ErrConnectorSecretKeyRequired) {
			if isHX(c) {
				return h.renderConnectorDialog(c, kind, connectorAlert(err), http.StatusUnprocessableEntity)
			}
			return h.renderConnectorsPage(c, kind, "", connectorAlert(err))
		}
		return h.RenderError(c, err)
	}
	if isHX(c) {
		return h.connectorMutationSuccess(c, kind)
	}
	return c.Redirect(http.StatusSeeOther, "/settings/connectors?saved="+kind)
}

func readOktaConfigUpdate(c *echo.Context) configstore.OktaConfig {
	return configstore.OktaConfig{
		Domain:              c.FormValue("domain"),
		Token:               c.FormValue("token"),
		DiscoveryEnabled:    ParseBoolForm(c.FormValue("discovery_enabled")),
		DiscoveryIngestMode: c.FormValue("discovery_ingest_mode"),
		EventHookEnabled:    ParseBoolForm(c.FormValue("event_hook_enabled")),
		EventHookSecret:     c.FormValue("event_hook_secret"),
		EventBridgeEnabled:  ParseBoolForm(c.FormValue("eventbridge_enabled")),
		EventBridgeSecret:   c.FormValue("eventbridge_secret"),
	}
}

func readGoogleWorkspaceConfigUpdate(c *echo.Context) configstore.GoogleWorkspaceConfig {
	return configstore.GoogleWorkspaceConfig{
		CustomerID:          c.FormValue("customer_id"),
		PrimaryDomain:       c.FormValue("primary_domain"),
		DelegatedAdminEmail: c.FormValue("delegated_admin_email"),
		AuthType:            c.FormValue("auth_type"),
		ServiceAccountJSON:  c.FormValue("service_account_json"),
		ServiceAccountEmail: c.FormValue("service_account_email"),
		DiscoveryEnabled:    ParseBoolForm(c.FormValue("discovery_enabled")),
	}
}

func readGitHubConfigUpdate(c *echo.Context) configstore.GitHubConfig {
	return configstore.GitHubConfig{
		Org:         c.FormValue("org"),
		APIBase:     c.FormValue("api_base"),
		Enterprise:  c.FormValue("enterprise"),
		Token:       c.FormValue("token"),
		SCIMEnabled: ParseBoolForm(c.FormValue("scim_enabled")),
	}
}

func readDatadogConfigUpdate(c *echo.Context) configstore.DatadogConfig {
	return configstore.DatadogConfig{
		Site:   c.FormValue("site"),
		APIKey: c.FormValue("api_key"),
		AppKey: c.FormValue("app_key"),
	}
}

func readAWSIdentityCenterConfigUpdate(c *echo.Context) configstore.AWSIdentityCenterConfig {
	return configstore.AWSIdentityCenterConfig{
		Region:          c.FormValue("region"),
		Name:            c.FormValue("name"),
		InstanceARN:     c.FormValue("instance_arn"),
		IdentityStoreID: c.FormValue("identity_store_id"),
		AuthType:        c.FormValue("auth_type"),
		AccessKeyID:     c.FormValue("access_key_id"),
		SecretAccessKey: c.FormValue("secret_access_key"),
		SessionToken:    c.FormValue("session_token"),
	}
}

func readEntraConfigUpdate(c *echo.Context) configstore.EntraConfig {
	return configstore.EntraConfig{
		TenantID:         c.FormValue("tenant_id"),
		ClientID:         c.FormValue("client_id"),
		ClientSecret:     c.FormValue("client_secret"),
		DiscoveryEnabled: ParseBoolForm(c.FormValue("discovery_enabled")),
	}
}

func readVaultConfigUpdate(c *echo.Context) configstore.VaultConfig {
	return configstore.VaultConfig{
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
			setHXToast(c, viewmodels.ToastViewData{
				Category:    "error",
				Title:       "Authoritative source unavailable",
				Description: "Check the connector configuration and try again.",
			})
			data, dataErr := h.buildConnectorsViewData(ctx, c, "", "", nil)
			if dataErr != nil {
				return h.RenderError(c, dataErr)
			}
			return h.renderConnectorRowStatus(c, kind, data, http.StatusUnprocessableEntity)
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

	configuredSourceKinds, configuredSourceNames, err := h.loadConfiguredIdentitySourcePairs(ctx)
	if err != nil {
		return h.RenderError(c, err)
	}
	if _, err := identity.ResolveWithConfiguredSourcesTx(ctx, h.Pool, configuredSourceKinds, configuredSourceNames); err != nil {
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
	addVary(c, "HX-Request", "HX-Target")
	data, err := h.buildConnectorsViewData(c.Request().Context(), c, openKind, savedKind, alert)
	if err != nil {
		return h.RenderError(c, err)
	}
	if isHX(c) && !isHXBoosted(c) && isHXTarget(c, "connectors-panel") {
		return h.RenderComponent(c, views.ConnectorsPanel(data))
	}
	return h.RenderComponent(c, views.ConnectorsPage(data))
}

func (h *Handlers) renderConnectorDialog(c *echo.Context, kind string, alert *viewmodels.ConnectorAlert, status int) error {
	data, err := h.buildConnectorsViewData(c.Request().Context(), c, kind, "", alert)
	if err != nil {
		return h.RenderError(c, err)
	}
	addVary(c, "HX-Request")
	if status != 0 && status != http.StatusOK {
		return h.RenderComponentStatus(c, status, views.ConnectorDialog(data, kind, true, alert))
	}
	return h.RenderComponent(c, views.ConnectorDialog(data, kind, true, alert))
}

func (h *Handlers) renderConnectorDialogAppend(c *echo.Context, kind string, alert *viewmodels.ConnectorAlert, status int) error {
	c.Response().Header().Set("HX-Retarget", "body")
	c.Response().Header().Set("HX-Reswap", "beforeend")
	return h.renderConnectorDialog(c, kind, alert, status)
}

func (h *Handlers) connectorMutationSuccess(c *echo.Context, kind string) error {
	data, err := h.buildConnectorsViewData(c.Request().Context(), c, "", "", nil)
	if err != nil {
		return h.RenderError(c, err)
	}
	setHXToast(c, viewmodels.ToastViewData{
		Category:    "success",
		Title:       ConnectorDisplayName(kind) + " saved",
		Description: "Connector settings updated.",
	})
	addHXTrigger(c, events.ConnectorsChanged, map[string]string{"kind": kind})
	return h.RenderComponent(c, views.ConnectorsPanelOOB(data))
}

func (h *Handlers) renderConnectorRow(c *echo.Context, kind string, data viewmodels.ConnectorsViewData) error {
	return h.renderConnectorRowStatus(c, kind, data, http.StatusOK)
}

func (h *Handlers) renderConnectorRowStatus(c *echo.Context, kind string, data viewmodels.ConnectorsViewData, status int) error {
	render := h.RenderComponent
	if status != 0 && status != http.StatusOK {
		render = func(c *echo.Context, component templ.Component) error {
			return h.RenderComponentStatus(c, status, component)
		}
	}
	switch NormalizeConnectorKind(kind) {
	case configstore.KindOkta:
		return render(c, views.OktaConnectorRow(data))
	case configstore.KindGoogleWorkspace:
		return render(c, views.GoogleWorkspaceConnectorRow(data))
	case configstore.KindEntra:
		return render(c, views.EntraConnectorRow(data))
	case configstore.KindGitHub:
		return render(c, views.GitHubConnectorRow(data))
	case configstore.KindDatadog:
		return render(c, views.DatadogConnectorRow(data))
	case configstore.KindAWSIdentityCenter:
		return render(c, views.AWSIdentityCenterConnectorRow(data))
	case configstore.KindVault:
		return render(c, views.VaultConnectorRow(data))
	default:
		return RenderNotFound(c)
	}
}

func (h *Handlers) buildConnectorsViewData(ctx context.Context, c *echo.Context, openKind, savedKind string, alert *viewmodels.ConnectorAlert) (viewmodels.ConnectorsViewData, error) {
	now := time.Now()
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
				oktaData := viewmodels.OktaConnectorViewData{
					Enabled:              state.Enabled,
					Configured:           state.Configured,
					Domain:               cfg.Domain,
					DiscoveryIngestMode:  cfg.DiscoveryIngestMode,
					TokenMasked:          configstore.MaskSecret(cfg.Token),
					HasToken:             cfg.Token != "",
					DiscoveryEnabled:     cfg.DiscoveryEnabled,
					EventHookEnabled:     cfg.EventHookEnabled,
					EventHookMasked:      configstore.MaskSecret(cfg.EventHookSecret),
					HasEventHookSecret:   cfg.EventHookSecret != "",
					EventBridgeEnabled:   cfg.EventBridgeEnabled,
					EventBridgeMasked:    configstore.MaskSecret(cfg.EventBridgeSecret),
					HasEventBridgeSecret: cfg.EventBridgeSecret != "",
					Authoritative:        authoritative,
				}
				switch cfg.DiscoveryIngestMode {
				case configstore.OktaDiscoveryIngestModeEventHook, configstore.OktaDiscoveryIngestModeEventBridge:
					if cfg.Token == "" {
						oktaData.PushCompletenessNote = "Push-only discovery is degraded for completeness and backfill until an Okta API token is configured."
					}
				}
				if sourceName != "" && (cfg.EventHookEnabled || cfg.EventBridgeEnabled) {
					status, err := h.Q.GetOktaPushIngestStatusBySource(ctx, sourceName)
					if err != nil && !errors.Is(err, pgx.ErrNoRows) {
						return viewmodels.ConnectorsViewData{}, err
					}
					oktaData.PushStatusVisible = true
					oktaData.PushStatusLabel = "Waiting for events"
					oktaData.PushLastReceived = "—"
					oktaData.PushLastProcessed = "—"
					oktaData.PushQueueLabel = "0 queued"
					oktaData.PushDeadLetterLabel = "0 dead-letter"
					if err == nil {
						oktaData.PushLastReceived = relativeWithTitleDisplay(now, status.LastReceivedAt, "—", "").Label
						oktaData.PushLastProcessed = relativeWithTitleDisplay(now, status.LastProcessedAt, "—", "").Label
						oktaData.PushQueueLabel = formatCountLabel(status.QueuedCount+status.ProcessingCount, "pending")
						oktaData.PushDeadLetterLabel = formatCountLabel(status.DeadLetterCount, "dead-letter")
						switch {
						case status.DeadLetterCount > 0:
							oktaData.PushStatusLabel = "Needs attention"
						case status.QueuedCount+status.ProcessingCount > 0:
							oktaData.PushStatusLabel = "Processing"
						case status.LastProcessedAt.Valid:
							oktaData.PushStatusLabel = "Processing normally"
						case status.LastReceivedAt.Valid:
							oktaData.PushStatusLabel = "Received"
						}
						oktaData.PushLastError = strings.TrimSpace(status.LastDeadLetterError)
					}
				}
				data.Okta = oktaData
			}
		case configstore.KindGoogleWorkspace:
			if cfg, ok := state.Config.(configstore.GoogleWorkspaceConfig); ok {
				cfg = cfg.Normalized()
				data.GoogleWorkspace = viewmodels.GoogleWorkspaceConnectorViewData{
					Enabled:               state.Enabled,
					Configured:            state.Configured,
					CustomerID:            cfg.CustomerID,
					PrimaryDomain:         cfg.PrimaryDomain,
					DelegatedAdminEmail:   cfg.DelegatedAdminEmail,
					AuthType:              cfg.AuthType,
					ServiceAccountMask:    configstore.MaskSecret(cfg.ServiceAccountJSON),
					HasServiceAccountJSON: cfg.ServiceAccountJSON != "",
					ServiceAccountEmail:   cfg.ServiceAccountEmail,
					DiscoveryEnabled:      cfg.DiscoveryEnabled,
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
	case configstore.KindGoogleWorkspace:
		cfg, err := configstore.DecodeGoogleWorkspaceConfig(raw)
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
	cfg, err := h.connectorConfigStore().GetResolvedConnectorConfig(ctx, kind)
	if err != nil {
		return "", err
	}

	switch NormalizeConnectorKind(kind) {
	case configstore.KindOkta:
		oktaCfg, err := configstore.DecodeOktaConfig(cfg.ResolvedConfig)
		if err != nil {
			return "", err
		}
		sourceName := strings.TrimSpace(oktaCfg.Normalized().Domain)
		if sourceName == "" {
			sourceName = "okta"
		}
		return sourceName, nil
	case configstore.KindEntra:
		entraCfg, err := configstore.DecodeEntraConfig(cfg.ResolvedConfig)
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

func formatCountLabel(count int64, noun string) string {
	noun = strings.TrimSpace(noun)
	if noun == "" {
		noun = "row"
	}
	return strconv.FormatInt(max(count, 0), 10) + " " + noun
}

// HandleResync triggers a manual resync.
func (h *Handlers) HandleResync(c *echo.Context) error {
	if c.Request().Method != http.MethodPost {
		return c.NoContent(http.StatusMethodNotAllowed)
	}
	if h.Syncer == nil {
		if isHX(c) {
			setHXToast(c, viewmodels.ToastViewData{Category: "warning", Title: "Resync unavailable", Description: "Sync is not configured on this server."})
			return h.RenderComponent(c, views.SettingsSyncStatus("disabled", "Sync unavailable", false, h.csrfToken(c), false))
		}
		return c.Redirect(http.StatusSeeOther, "/settings?resync=disabled")
	}
	triggerCtx := sync.WithForcedSync(c.Request().Context())
	if err := h.Syncer.RunOnce(triggerCtx); err != nil {
		if errors.Is(err, sync.ErrSyncQueued) {
			if isHX(c) {
				setHXToast(c, viewmodels.ToastViewData{Category: "success", Title: "Resync queued", Description: "A worker will pick up the sync shortly."})
				addHXTrigger(c, events.DataSyncChanged, map[string]string{"status": "queued"})
				return h.RenderComponent(c, views.SettingsSyncStatus("queued", "Sync queued", true, h.csrfToken(c), true))
			}
			return c.Redirect(http.StatusSeeOther, "/settings?resync=queued")
		}
		if errors.Is(err, sync.ErrSyncAlreadyRunning) {
			if isHX(c) {
				setHXToast(c, viewmodels.ToastViewData{Category: "warning", Title: "Resync already running", Description: "Status will refresh automatically."})
				addHXTrigger(c, events.DataSyncChanged, map[string]string{"status": "running"})
				return h.RenderComponent(c, views.SettingsSyncStatus("running", "Sync running", true, h.csrfToken(c), true))
			}
			return c.Redirect(http.StatusSeeOther, "/settings?resync=busy")
		}
		if errors.Is(err, sync.ErrNoEnabledConnectors) {
			if isHX(c) {
				setHXToast(c, viewmodels.ToastViewData{Category: "warning", Title: "Resync unavailable", Description: "No connectors are enabled."})
				return h.RenderComponent(c, views.SettingsSyncStatus("disabled", "No connectors enabled", false, h.csrfToken(c), true))
			}
			return c.Redirect(http.StatusSeeOther, "/settings?resync=disabled")
		}
		if isHX(c) {
			setHXToast(c, viewmodels.ToastViewData{Category: "error", Title: "Resync failed", Description: "Check server logs for details."})
			return h.RenderComponent(c, views.SettingsSyncStatus("error", "Sync failed", false, h.csrfToken(c), true))
		}
		return c.Redirect(http.StatusSeeOther, "/settings?resync=error")
	}
	if isHX(c) {
		setHXToast(c, viewmodels.ToastViewData{Category: "success", Title: "Resync complete", Description: "The data sync finished successfully."})
		addHXTrigger(c, events.DataSyncChanged, map[string]string{"status": "success"})
		return h.RenderComponent(c, views.SettingsSyncStatus("success", "Sync complete", false, h.csrfToken(c), true))
	}
	return c.Redirect(http.StatusSeeOther, "/settings?resync=success")
}

func (h *Handlers) HandleResyncStatus(c *echo.Context) error {
	if c.Request().Method != http.MethodGet {
		return c.NoContent(http.StatusMethodNotAllowed)
	}
	active, err := h.activeManualSyncJobs(c.Request().Context())
	if err != nil {
		return h.RenderError(c, err)
	}
	if active > 0 {
		return h.RenderComponent(c, views.SettingsSyncStatus("running", "Sync running", true, h.csrfToken(c), h.Syncer != nil))
	}
	status, label, err := h.latestManualSyncTerminalStatus(c.Request().Context())
	if err != nil {
		return h.RenderError(c, err)
	}
	if isHX(c) {
		addHXTrigger(c, events.DataSyncChanged, map[string]string{"status": status})
	}
	return h.RenderComponent(c, views.SettingsSyncStatus(status, label, false, h.csrfToken(c), h.Syncer != nil))
}

// HandleResyncStream is an SSE endpoint that streams sync-status fragments to
// the settings page. The connection stays open while a manual sync is active
// and emits one `status` event per state change. When sync completes we send
// the final fragment and close.
//
// The backend still polls the DB internally (every two seconds) because the
// sync workers don't publish change notifications today. Compared to the
// previous client-side hx-trigger="every 2s", this:
//   - keeps a single long-lived TCP connection per active sync,
//   - sends only the diff'd fragment (no cookie/CSRF roundtrip per tick),
//   - converges immediately on terminal state without one extra cycle.
func (h *Handlers) HandleResyncStream(c *echo.Context) error {
	w := c.Response()
	resp, err := echo.UnwrapResponse(w)
	if err != nil || resp == nil {
		return h.HandleResyncStatus(c)
	}
	header := w.Header()
	header.Set("Content-Type", "text/event-stream")
	header.Set("Cache-Control", "no-cache")
	header.Set("Connection", "keep-alive")
	header.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	resp.Flush()

	ctx := c.Request().Context()
	const tickInterval = 2 * time.Second

	emit := func(status, label string, poll bool) error {
		buf := &strings.Builder{}
		if err := views.SettingsSyncStatusContent(status, label, poll, h.csrfToken(c), h.Syncer != nil).Render(ctx, buf); err != nil {
			return err
		}
		payload := strings.ReplaceAll(buf.String(), "\n", "")
		if _, err := fmt.Fprintf(w, "event: status\ndata: %s\n\n", payload); err != nil {
			return err
		}
		resp.Flush()
		return nil
	}

	emitDone := func() error {
		if _, err := fmt.Fprint(w, "event: done\ndata: done\n\n"); err != nil {
			return err
		}
		resp.Flush()
		return nil
	}

	lastActive := int64(-1)
	tick := func() (terminal bool, _ error) {
		active, err := h.activeManualSyncJobs(ctx)
		if err != nil {
			return true, err
		}
		if active == lastActive {
			return false, nil
		}
		lastActive = active
		if active > 0 {
			return false, emit("running", "Sync running", true)
		}
		status, label, err := h.latestManualSyncTerminalStatus(ctx)
		if err != nil {
			return true, err
		}
		if err := emit(status, label, false); err != nil {
			return true, err
		}
		return true, emitDone()
	}

	if done, err := tick(); err != nil || done {
		return err
	}

	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			done, err := tick()
			if err != nil {
				return err
			}
			if done {
				return nil
			}
		}
	}
}

func (h *Handlers) activeManualSyncJobs(ctx context.Context) (int64, error) {
	if h.Pool == nil {
		return 0, nil
	}
	var count int64
	err := h.Pool.QueryRow(ctx, `
		SELECT count(*)
		FROM sync_jobs
		WHERE trigger_kind = 'manual'
		  AND status IN ('pending', 'claimed', 'running')
	`).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (h *Handlers) latestManualSyncTerminalStatus(ctx context.Context) (string, string, error) {
	if h.Pool == nil {
		return "success", "Sync complete", nil
	}
	var jobStatus string
	err := h.Pool.QueryRow(ctx, `
		SELECT status
		FROM sync_jobs
		WHERE trigger_kind = 'manual'
		  AND status IN ('succeeded', 'failed')
		ORDER BY COALESCE(finished_at, updated_at, created_at) DESC, created_at DESC, id DESC
		LIMIT 1
	`).Scan(&jobStatus)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "success", "Sync complete", nil
		}
		return "", "", err
	}
	switch strings.ToLower(strings.TrimSpace(jobStatus)) {
	case "failed":
		return "error", "Sync failed", nil
	default:
		return "success", "Sync complete", nil
	}
}

func (h *Handlers) csrfToken(c *echo.Context) string {
	if c == nil {
		return ""
	}
	token, _ := c.Get(middleware.DefaultCSRFConfig.ContextKey).(string)
	return token
}

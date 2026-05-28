package httpapp

import (
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/auth"
	"github.com/open-sspm/open-sspm/internal/http/authn"
)

func (es *EchoServer) registerWebRoutes() {
	authed := es.e.Group("")
	authed.Use(es.browserMiddleware()...)
	authed.Use(authn.RequireAuth(es.h.Sessions, es.h.Q))

	es.registerOverviewRoutes(authed)
	es.registerAccessRoutes(authed)
	es.registerDiscoveryRoutes(authed)
	es.registerIdentityRoutes(authed)
	es.registerFindingsRoutes(authed)
	es.registerAccountRoutes(authed)
	authed.POST("/logout", es.h.HandleLogoutPost)

	admin := authed.Group("")
	admin.Use(authn.RequireRole(auth.RoleAdmin))
	es.registerAdminRoutes(admin)
}

func (es *EchoServer) registerOverviewRoutes(authed *echo.Group) {
	authed.GET("/", es.h.HandleDashboard)
	authed.GET("/global-view", es.h.HandleGlobalView)
	authed.GET("/askbar/suggestions", es.h.HandleAskBarSuggestions)
	authed.GET("/command/search", es.h.HandleCommandSearch)
}

func (es *EchoServer) registerAccessRoutes(authed *echo.Group) {
	authed.GET("/assigned-apps", es.h.HandleApps)
	authed.GET("/assigned-apps/:sourceName/:externalID", es.h.HandleOktaAppShow)
	authed.GET("/oauth-apps", es.h.HandleConnectedApps)
	authed.GET("/oauth-apps/:id", es.h.HandleConnectedAppShow)
	authed.GET("/oauth-apps/:id/export", es.h.HandleConnectedAppExport)
	authed.GET("/app-assets", es.h.HandleAppAssets)
	authed.GET("/app-assets/:id", es.h.HandleAppAssetShow)
	authed.GET("/app-assets/:id/export", es.h.HandleAppAssetExport)
	authed.GET("/credentials", es.h.HandleCredentials)
	authed.GET("/credentials/export", es.h.HandleCredentialsExport)
	authed.GET("/credentials/:id", es.h.HandleCredentialShow)
	authed.GET("/resources/:sourceKind/:sourceName/:resourceKind/*", es.h.HandleResourceShow)
}

func (es *EchoServer) registerDiscoveryRoutes(authed *echo.Group) {
	authed.GET("/discovery/apps", es.h.HandleDiscoveryApps)
	authed.GET("/discovery/apps/replacement-candidates", es.h.HandleDiscoveryReplacementCandidates)
	authed.GET("/discovery/apps/:id", es.h.HandleDiscoveryAppShow)
	authed.GET("/discovery/hotspots", es.h.HandleDiscoveryHotspots)
}

func (es *EchoServer) registerIdentityRoutes(authed *echo.Group) {
	authed.GET("/identities", es.h.HandleIdentities)
	authed.GET("/identities/:id", es.h.HandleIdentityShow)
	authed.GET("/non-human-identities", es.h.HandleNonHumanIdentities)
	authed.GET("/non-human-identities/:ref", es.h.HandleNonHumanIdentityShow)
}

func (es *EchoServer) registerFindingsRoutes(authed *echo.Group) {
	authed.GET("/findings", es.h.HandleFindings)
	authed.GET("/findings/rulesets/:rulesetKey", es.h.HandleFindingsRuleset)
	authed.GET("/findings/rulesets/:rulesetKey/rules/:ruleKey", es.h.HandleFindingsRule)
}

func (es *EchoServer) registerAccountRoutes(authed *echo.Group) {
	authed.GET("/accounts/okta", es.h.HandleOktaAccounts)
	authed.GET("/accounts/okta/:id", es.h.HandleOktaAccountShow)
	authed.GET("/accounts/github", es.h.HandleGitHubUsers)
	authed.GET("/accounts/entra", es.h.HandleEntraUsers)
	authed.GET("/accounts/google-workspace", es.h.HandleGoogleWorkspaceUsers)
	authed.GET("/accounts/google-workspace/groups", es.h.HandleGoogleWorkspaceGroups)
	authed.GET("/accounts/aws", es.h.HandleAWSUsers)
	authed.GET("/accounts/datadog", es.h.HandleDatadogUsers)
	authed.GET("/accounts/needs-anchor/github/:org", es.h.HandleGitHubAccountsNeedingAnchor)
	authed.GET("/accounts/needs-anchor/entra", es.h.HandleEntraAccountsNeedingAnchor)
	authed.GET("/accounts/needs-anchor/google-workspace", es.h.HandleGoogleWorkspaceAccountsNeedingAnchor)
	authed.GET("/accounts/needs-anchor/aws", es.h.HandleAWSAccountsNeedingAnchor)
	authed.GET("/accounts/needs-anchor/datadog/:site", es.h.HandleDatadogAccountsNeedingAnchor)
}

func (es *EchoServer) registerAdminRoutes(admin *echo.Group) {
	admin.GET("/identity-resolution", es.h.HandleIdentityResolutionReview)
	admin.POST("/assigned-apps/map", es.h.HandleAppsMap)
	admin.POST("/app-assets/:id/governance", es.h.HandleAppAssetGovernanceUpdate)
	admin.POST("/discovery/apps/:id/governance", es.h.HandleDiscoveryAppGovernanceUpdate)
	admin.POST("/non-human-identities/:ref/relationships", es.h.HandleNonHumanIdentityRelationshipCreate)
	admin.POST("/identity-resolution/candidates/:id/accept", es.h.HandleIdentityResolutionCandidateAccept)
	admin.POST("/identity-resolution/candidates/:id/reject", es.h.HandleIdentityResolutionCandidateReject)
	admin.POST("/identity-resolution/candidates/:id/mark-service", es.h.HandleIdentityResolutionCandidateMarkService)
	admin.POST("/identity-resolution/candidates/:id/mark-shared", es.h.HandleIdentityResolutionCandidateMarkShared)
	admin.POST("/app-assets/:id/grants/:credentialID/revoke", es.h.HandleAppAssetGrantRevoke)
	admin.POST("/oauth-apps/:id/grants/:credentialID/revoke", es.h.HandleConnectedAppGrantRevoke)
	admin.POST("/links", es.h.HandleCreateLink)
	admin.POST("/findings/rulesets/:rulesetKey/override", es.h.HandleFindingsRulesetOverride)
	admin.POST("/findings/rulesets/:rulesetKey/rules/:ruleKey/override", es.h.HandleFindingsRuleOverride)
	admin.POST("/findings/rulesets/:rulesetKey/rules/:ruleKey/attestation", es.h.HandleFindingsRuleAttestation)
	admin.GET("/settings", es.h.HandleSettings)
	admin.GET("/settings/connectors", es.h.HandleConnectors)
	admin.GET("/settings/connectors/:kind/dialog", es.h.HandleConnectorDialog)
	admin.GET("/settings/connector-health", es.h.HandleConnectorHealth)
	admin.GET("/settings/connector-health/errors", es.h.HandleConnectorHealthErrorDetails)
	admin.POST("/settings/connector-health/sync", es.h.HandleConnectorHealthSync)
	admin.POST("/settings/connectors/*", es.h.HandleConnectorAction)
	admin.GET("/settings/users", es.h.HandleSettingsUsers)
	admin.GET("/settings/users/new", es.h.HandleSettingsUsersNewDialog)
	admin.GET("/settings/users/:id/edit", es.h.HandleSettingsUserEditDialog)
	admin.GET("/settings/users/:id/delete", es.h.HandleSettingsUserDeleteDialog)
	admin.POST("/settings/users", es.h.HandleSettingsUsersCreate)
	admin.POST("/settings/users/:id", es.h.HandleSettingsUserUpdate)
	admin.POST("/settings/users/:id/delete", es.h.HandleSettingsUserDelete)
	admin.GET("/settings/resync/status", es.h.HandleResyncStatus)
	admin.GET("/settings/resync/stream", es.h.HandleResyncStream)
	admin.POST("/settings/resync", es.h.HandleResync)
}

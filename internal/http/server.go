package httpapp

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/handlers"
)

// EchoServer is the HTTP server wrapper.
type EchoServer struct {
	h *handlers.Handlers
	e *echo.Echo
}

// NewEchoServer creates a new HTTP server.
func NewEchoServer(cfg config.Config, q *gen.Queries, syncer handlers.SyncRunner, reg *registry.ConnectorRegistry) (*EchoServer, error) {
	h := &handlers.Handlers{Cfg: cfg, Q: q, Syncer: syncer, Registry: reg}
	es := &EchoServer{h: h, e: echo.New()}
	es.registerRoutes()
	return es, nil
}

func (es *EchoServer) registerRoutes() {
	es.e.GET("/healthz", es.h.HandleHealthz)

	authed := es.e.Group("")
	authed.Use(middleware.CSRFWithConfig(middleware.CSRFConfig{
		TokenLookup:    "header:" + echo.HeaderXCSRFToken + ",form:csrf",
		CookiePath:     "/",
		CookieHTTPOnly: true,
		CookieSameSite: http.SameSiteLaxMode,
	}))
	authed.GET("/", es.h.HandleDashboard)
	authed.GET("/global-view", es.h.HandleGlobalView)
	authed.GET("/apps", es.h.HandleApps)
	authed.POST("/apps/map", es.h.HandleAppsMap)
	authed.GET("/apps/*", es.h.HandleOktaAppShow)
	authed.GET("/idp-users", es.h.HandleIdpUsers)
	authed.GET("/idp-users/*", es.h.HandleIdpUserShow)
	authed.GET("/api/idp-users/:id/access-tree", es.h.HandleIdpUserAccessTree)
	authed.GET("/resources/:sourceKind/:sourceName/:resourceKind/*", es.h.HandleResourceShow)
	authed.GET("/findings", es.h.HandleFindings)
	authed.GET("/findings/okta-benchmark", es.h.HandleFindingsOktaBenchmark) // legacy redirect
	authed.GET("/findings/rulesets/:rulesetKey", es.h.HandleFindingsRuleset)
	authed.POST("/findings/rulesets/:rulesetKey/override", es.h.HandleFindingsRulesetOverride)
	authed.GET("/findings/rulesets/:rulesetKey/rules/:ruleKey", es.h.HandleFindingsRule)
	authed.POST("/findings/rulesets/:rulesetKey/rules/:ruleKey/override", es.h.HandleFindingsRuleOverride)
	authed.POST("/findings/rulesets/:rulesetKey/rules/:ruleKey/attestation", es.h.HandleFindingsRuleAttestation)
	authed.GET("/github-users", es.h.HandleGitHubUsers)
	authed.GET("/entra-users", es.h.HandleEntraUsers)
	authed.GET("/aws-users", es.h.HandleAWSUsers)
	authed.GET("/datadog-users", es.h.HandleDatadogUsers)
	authed.GET("/unmatched/github/*", es.h.HandleUnmatchedGitHub)
	authed.GET("/unmatched/entra", es.h.HandleUnmatchedEntra)
	authed.GET("/unmatched/aws", es.h.HandleUnmatchedAWS)
	authed.GET("/unmatched/datadog/*", es.h.HandleUnmatchedDatadog)
	authed.POST("/links", es.h.HandleCreateLink)
	authed.GET("/settings", es.h.HandleSettings)
	authed.GET("/settings/connectors", es.h.HandleConnectors)
	authed.POST("/settings/connectors/*", es.h.HandleConnectorAction)
	authed.POST("/settings/resync", es.h.HandleResync)

	es.e.Static("/static", "web/static")
}

// Start starts the HTTP server.
func (es *EchoServer) Start(addr string) error {
	return es.e.Start(addr)
}

// StartServer starts the HTTP server with a custom http.Server.
func (es *EchoServer) StartServer(server *http.Server) error {
	return es.e.StartServer(server)
}

// Shutdown gracefully shuts down the HTTP server.
func (es *EchoServer) Shutdown(ctx context.Context) error {
	return es.e.Shutdown(ctx)
}

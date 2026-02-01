package httpapp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/alexedwards/scs/pgxstore"
	"github.com/alexedwards/scs/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
	"github.com/open-sspm/open-sspm/internal/auth"
	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/authn"
	"github.com/open-sspm/open-sspm/internal/http/handlers"
)

// EchoServer is the HTTP server wrapper.
type EchoServer struct {
	h *handlers.Handlers
	e *echo.Echo

	mu     sync.Mutex
	server *http.Server
}

// NewEchoServer creates a new HTTP server.
func NewEchoServer(cfg config.Config, pool *pgxpool.Pool, q *gen.Queries, syncer handlers.SyncRunner, reg *registry.ConnectorRegistry) (*EchoServer, error) {
	sessions := scs.New()
	sessions.Store = pgxstore.New(pool)
	sessions.HashTokenInStore = true
	sessions.IdleTimeout = 12 * time.Hour
	sessions.Lifetime = 14 * 24 * time.Hour
	sessions.Cookie.Name = "oss_session"
	sessions.Cookie.HttpOnly = true
	sessions.Cookie.Path = "/"
	sessions.Cookie.SameSite = http.SameSiteLaxMode
	sessions.Cookie.Secure = cfg.AuthCookieSecure

	h := &handlers.Handlers{Cfg: cfg, Q: q, Pool: pool, Syncer: syncer, Registry: reg, Sessions: sessions}
	es := &EchoServer{h: h, e: echo.New()}
	es.e.Use(middleware.RequestIDWithConfig(middleware.RequestIDConfig{
		RequestIDHandler: func(c *echo.Context, id string) {
			id = normalizeRequestID(id)
			if id == "" {
				id = generateRequestID()
				c.Response().Header().Set(echo.HeaderXRequestID, id)
			}
			c.Set(handlers.ContextKeyRequestID, id)
		},
	}))
	es.e.Use(echo.WrapMiddleware(sessions.LoadAndSave))
	es.e.Use(middleware.CSRFWithConfig(middleware.CSRFConfig{
		TokenLookup:    "header:" + echo.HeaderXCSRFToken + ",form:csrf",
		CookiePath:     "/",
		CookieHTTPOnly: true,
		CookieSameSite: http.SameSiteLaxMode,
		CookieSecure:   cfg.AuthCookieSecure,
	}))
	es.e.HTTPErrorHandler = es.httpErrorHandler
	es.registerRoutes()
	return es, nil
}

func normalizeRequestID(id string) string {
	id = strings.TrimSpace(id)
	if id == "" || len(id) > 128 {
		return ""
	}
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '_' || r == '.':
		default:
			return ""
		}
	}
	return id
}

func generateRequestID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err == nil {
		return hex.EncodeToString(buf[:])
	}
	// Best-effort fallback; request ids are for debugging, not security.
	return "unknown"
}

func resolveStaticDir(preferred string) (resolved string, ok bool) {
	preferred = strings.TrimSpace(preferred)
	candidates := make([]string, 0, 6)
	if preferred != "" {
		candidates = append(candidates, preferred)
	}

	candidates = append(candidates, "web/static")

	if exe, err := os.Executable(); err == nil && strings.TrimSpace(exe) != "" {
		exeDir := filepath.Dir(exe)
		if exeDir != "" && exeDir != "." {
			candidates = append(candidates,
				filepath.Join(exeDir, "web/static"),
				filepath.Join(filepath.Dir(exeDir), "web/static"),
			)
		}
	}

	// Common demo/packaging location.
	candidates = append(candidates, "/opt/open-sspm/web/static")

	seen := map[string]struct{}{}
	unique := make([]string, 0, len(candidates))
	for _, c := range candidates {
		if c == "" {
			continue
		}
		c = filepath.Clean(c)
		if _, exists := seen[c]; exists {
			continue
		}
		seen[c] = struct{}{}
		unique = append(unique, c)
	}

	for _, c := range unique {
		if info, err := os.Stat(c); err == nil && info.IsDir() {
			return c, true
		}
	}

	// Best-effort fallback; static handler will return 404s.
	if preferred != "" {
		return filepath.Clean(preferred), false
	}
	return "web/static", false
}

func (es *EchoServer) httpErrorHandler(c *echo.Context, err error) {
	resp, _ := echo.UnwrapResponse(c.Response())
	if resp != nil && resp.Committed {
		return
	}

	status := http.StatusInternalServerError
	if httpErr, ok := err.(*echo.HTTPError); ok && httpErr != nil {
		status = httpErr.Code
	}

	if status >= 500 {
		_ = es.h.RenderError(c, err)
		return
	}
	if status == http.StatusNotFound {
		_ = handlers.RenderNotFound(c)
		return
	}
	if status == http.StatusForbidden {
		_ = es.h.RenderForbidden(c)
		return
	}
	_ = c.String(status, http.StatusText(status))
}

func (es *EchoServer) registerRoutes() {
	es.e.GET("/healthz", es.h.HandleHealthz)

	authed := es.e.Group("")

	es.e.GET("/login", es.h.HandleLoginGet)
	es.e.POST("/login", es.h.HandleLoginPost, middleware.RateLimiterWithConfig(middleware.RateLimiterConfig{
		Store: middleware.NewRateLimiterMemoryStoreWithConfig(middleware.RateLimiterMemoryStoreConfig{
			Rate:      0.5,
			Burst:     10,
			ExpiresIn: 10 * time.Minute,
		}),
		IdentifierExtractor: func(c *echo.Context) (string, error) {
			return c.RealIP(), nil
		},
		DenyHandler: func(c *echo.Context, identifier string, err error) error {
			return c.String(http.StatusTooManyRequests, "too many login attempts")
		},
	}))

	authed.Use(authn.RequireAuth(es.h.Sessions, es.h.Q))
	authed.GET("/", es.h.HandleDashboard)
	authed.GET("/global-view", es.h.HandleGlobalView)
	authed.GET("/apps", es.h.HandleApps)
	authed.GET("/apps/*", es.h.HandleOktaAppShow)
	authed.GET("/idp-users", es.h.HandleIdpUsers)
	authed.GET("/idp-users/*", es.h.HandleIdpUserShow)
	authed.GET("/api/idp-users/:id/access-tree", es.h.HandleIdpUserAccessTree)
	authed.GET("/resources/:sourceKind/:sourceName/:resourceKind/*", es.h.HandleResourceShow)
	authed.GET("/findings", es.h.HandleFindings)
	authed.GET("/findings/rulesets/:rulesetKey", es.h.HandleFindingsRuleset)
	authed.GET("/findings/rulesets/:rulesetKey/rules/:ruleKey", es.h.HandleFindingsRule)
	authed.GET("/github-users", es.h.HandleGitHubUsers)
	authed.GET("/entra-users", es.h.HandleEntraUsers)
	authed.GET("/aws-users", es.h.HandleAWSUsers)
	authed.GET("/datadog-users", es.h.HandleDatadogUsers)
	authed.GET("/unmatched/github/*", es.h.HandleUnmatchedGitHub)
	authed.GET("/unmatched/entra", es.h.HandleUnmatchedEntra)
	authed.GET("/unmatched/aws", es.h.HandleUnmatchedAWS)
	authed.GET("/unmatched/datadog/*", es.h.HandleUnmatchedDatadog)
	authed.POST("/logout", es.h.HandleLogoutPost)

	admin := authed.Group("")
	admin.Use(authn.RequireRole(auth.RoleAdmin))
	admin.POST("/apps/map", es.h.HandleAppsMap)
	admin.POST("/links", es.h.HandleCreateLink)
	admin.POST("/findings/rulesets/:rulesetKey/override", es.h.HandleFindingsRulesetOverride)
	admin.POST("/findings/rulesets/:rulesetKey/rules/:ruleKey/override", es.h.HandleFindingsRuleOverride)
	admin.POST("/findings/rulesets/:rulesetKey/rules/:ruleKey/attestation", es.h.HandleFindingsRuleAttestation)
	admin.GET("/settings", es.h.HandleSettings)
	admin.GET("/settings/connectors", es.h.HandleConnectors)
	admin.GET("/settings/connector-health", es.h.HandleConnectorHealth)
	admin.POST("/settings/connectors/*", es.h.HandleConnectorAction)
	admin.GET("/settings/users", es.h.HandleSettingsUsers)
	admin.POST("/settings/users", es.h.HandleSettingsUsersCreate)
	admin.POST("/settings/users/:id", es.h.HandleSettingsUserUpdate)
	admin.POST("/settings/users/:id/delete", es.h.HandleSettingsUserDelete)
	admin.POST("/settings/resync", es.h.HandleResync)

	staticDir, ok := resolveStaticDir(es.h.Cfg.StaticDir)
	if ok {
		slog.Info("serving static assets", "dir", staticDir)
	} else {
		wd, _ := os.Getwd()
		slog.Warn("static assets directory not found; /static may return 404s", "dir", staticDir, "cwd", wd)
	}
	es.e.Static("/static", staticDir)
}

// Start starts the HTTP server.
func (es *EchoServer) Start(addr string) error {
	return es.StartServer(&http.Server{Addr: addr})
}

// StartServer starts the HTTP server with a custom http.Server.
func (es *EchoServer) StartServer(server *http.Server) error {
	if server == nil {
		return errors.New("http server is nil")
	}

	server.Handler = es.e

	es.mu.Lock()
	es.server = server
	es.mu.Unlock()

	return server.ListenAndServe()
}

// Shutdown gracefully shuts down the HTTP server.
func (es *EchoServer) Shutdown(ctx context.Context) error {
	es.mu.Lock()
	server := es.server
	es.mu.Unlock()

	if server == nil {
		return nil
	}
	return server.Shutdown(ctx)
}

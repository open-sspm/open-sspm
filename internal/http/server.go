package httpapp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net"
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
	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/evaluator"
	"github.com/open-sspm/open-sspm/internal/http/handlers"
	"github.com/open-sspm/open-sspm/internal/mailer"
)

// EchoServer is the HTTP server wrapper.
type EchoServer struct {
	h *handlers.Handlers
	e *echo.Echo

	mu     sync.Mutex
	server *http.Server
}

// NewEchoServer creates a new HTTP server.
func NewEchoServer(
	cfg config.Config,
	pool *pgxpool.Pool,
	q *gen.Queries,
	syncer handlers.SyncRunner,
	reg *registry.ConnectorRegistry,
	mailAdapter mailer.Mailer,
) (*EchoServer, error) {
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

	policyRegistry, err := evaluator.BuiltinRegistry()
	if err != nil {
		return nil, err
	}

	h := &handlers.Handlers{
		Cfg:            cfg,
		Q:              q,
		Pool:           pool,
		Sessions:       sessions,
		Syncer:         syncer,
		Registry:       reg,
		Mailer:         mailAdapter,
		PolicyRegistry: policyRegistry,
	}
	es := &EchoServer{h: h, e: newEcho(cfg)}
	es.e.Use(securityHeadersMiddleware)
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
	es.e.HTTPErrorHandler = es.httpErrorHandler
	es.registerRoutes()
	return es, nil
}

// securityHeadersMiddleware emits the security-related response headers that
// every page should carry. The CSP is tuned to match the htmx-config settings
// in views/layout.templ (allowEval:false, allowScriptTags:false, selfRequestsOnly:true)
// and our local-only asset pipeline.
func securityHeadersMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c *echo.Context) error {
		h := c.Response().Header()
		if h.Get("Content-Security-Policy") == "" {
			h.Set("Content-Security-Policy",
				"default-src 'self'; "+
					"script-src 'self' 'sha256-qgfGDKq/rijkXxRFr/5N/gmWkvAuA8vY4XTEtgdMC9w='; "+
					"style-src 'self' 'unsafe-inline'; "+
					"img-src 'self' data:; "+
					"font-src 'self' data:; "+
					"connect-src 'self'; "+
					"frame-ancestors 'none'; "+
					"base-uri 'self'; "+
					"form-action 'self'")
		}
		if h.Get("Referrer-Policy") == "" {
			h.Set("Referrer-Policy", "strict-origin-when-cross-origin")
		}
		if h.Get("X-Content-Type-Options") == "" {
			h.Set("X-Content-Type-Options", "nosniff")
		}
		if h.Get("Permissions-Policy") == "" {
			h.Set("Permissions-Policy", "camera=(), microphone=(), geolocation=(), payment=()")
		}
		return next(c)
	}
}

func newEcho(cfg config.Config) *echo.Echo {
	e := echo.New()
	e.Logger = slog.Default().With("component", "http")
	// Preserve client IPs behind ingress while rejecting spoofed forwarded headers on direct traffic.
	e.IPExtractor = newXFFIPExtractor(cfg.TrustedProxyCIDRs)
	return e
}

func newXFFIPExtractor(trustedProxyCIDRs []string) echo.IPExtractor {
	opts := trustedProxyTrustOptions(trustedProxyCIDRs)
	return echo.ExtractIPFromXFFHeader(opts...)
}

func trustedProxyTrustOptions(trustedProxyCIDRs []string) []echo.TrustOption {
	opts := make([]echo.TrustOption, 0, len(trustedProxyCIDRs))
	for _, cidr := range trustedProxyCIDRs {
		_, network, err := net.ParseCIDR(cidr)
		if err != nil {
			continue
		}
		opts = append(opts, echo.TrustIPRange(network))
	}
	return opts
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
	if handlers.IsClientCanceled(c, err) {
		return
	}

	resp, _ := echo.UnwrapResponse(c.Response())
	if resp != nil && resp.Committed {
		return
	}

	status := httpStatusFromError(err)

	if status >= 500 {
		_ = es.h.RenderError(c, err)
		return
	}
	if status == http.StatusNotFound {
		_ = es.h.RenderNotFoundPage(c)
		return
	}
	if status == http.StatusForbidden {
		_ = es.h.RenderForbidden(c)
		return
	}
	_ = c.String(status, http.StatusText(status))
}

func httpStatusFromError(err error) int {
	status := http.StatusInternalServerError
	if err == nil {
		return status
	}

	var statusCoder interface{ StatusCode() int }
	if errors.As(err, &statusCoder) {
		code := statusCoder.StatusCode()
		if code >= 100 && code <= 999 {
			return code
		}
	}
	return status
}

func (es *EchoServer) registerRoutes() {
	es.registerPlatformRoutes()
	es.registerPublicRoutes()
	es.registerIngestRoutes()
	es.registerWebRoutes()
}

func (es *EchoServer) browserMiddleware() []echo.MiddlewareFunc {
	return []echo.MiddlewareFunc{
		echo.WrapMiddleware(es.h.Sessions.LoadAndSave),
		es.browserCSRFMiddleware(),
	}
}

func (es *EchoServer) sessionMiddleware() []echo.MiddlewareFunc {
	return []echo.MiddlewareFunc{
		echo.WrapMiddleware(es.h.Sessions.LoadAndSave),
	}
}

func (es *EchoServer) browserCSRFMiddleware() echo.MiddlewareFunc {
	return middleware.CSRFWithConfig(middleware.CSRFConfig{
		TokenLookup:    "header:" + echo.HeaderXCSRFToken + ",form:csrf",
		CookiePath:     "/",
		CookieHTTPOnly: true,
		CookieSameSite: http.SameSiteLaxMode,
		CookieSecure:   es.h.Cfg.AuthCookieSecure,
	})
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

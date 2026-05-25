package httpapp

import (
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
)

func (es *EchoServer) registerPlatformRoutes() {
	es.e.GET("/healthz", es.h.HandleHealthz)
	es.e.GET("/favicon.ico", func(c *echo.Context) error {
		c.Response().Header().Set(echo.HeaderContentType, echo.MIMETextHTMLCharsetUTF8)
		return c.Redirect(http.StatusMovedPermanently, "/static/favicon.ico")
	})

	staticDir, ok := resolveStaticDir(es.h.Cfg.StaticDir)
	if ok {
		slog.Info("serving static assets", "dir", staticDir)
	} else {
		wd, _ := os.Getwd()
		slog.Warn("static assets directory not found; /static may return 404s", "dir", staticDir, "cwd", wd)
	}
	es.e.Static("/static", staticDir)
}

func (es *EchoServer) registerPublicRoutes() {
	public := es.e.Group("")
	public.Use(es.browserMiddleware()...)

	public.GET("/login", es.h.HandleLoginGet)
	public.POST("/login", es.h.HandleLoginPost, loginRateLimiter())
}

func loginRateLimiter() echo.MiddlewareFunc {
	return middleware.RateLimiterWithConfig(middleware.RateLimiterConfig{
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
	})
}

package authn

import (
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/alexedwards/scs/v2"
	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v4"
	"github.com/open-sspm/open-sspm/internal/auth"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

const (
	ContextKeyPrincipal = "auth_principal"

	SessionKeyUserID = "auth_user_id"
)

func PrincipalFromContext(c echo.Context) (auth.Principal, bool) {
	p, ok := c.Get(ContextKeyPrincipal).(auth.Principal)
	return p, ok
}

func LoadPrincipal(ctx echo.Context, sessions *scs.SessionManager, q *gen.Queries) (auth.Principal, bool, error) {
	userID := sessions.GetInt64(ctx.Request().Context(), SessionKeyUserID)
	if userID <= 0 {
		return auth.Principal{}, false, nil
	}

	user, err := q.GetAuthUser(ctx.Request().Context(), userID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			_ = sessions.Destroy(ctx.Request().Context())
			return auth.Principal{}, false, nil
		}
		return auth.Principal{}, false, err
	}
	if !user.IsActive {
		_ = sessions.Destroy(ctx.Request().Context())
		return auth.Principal{}, false, nil
	}

	return auth.Principal{
		UserID: user.ID,
		Email:  user.Email,
		Role:   user.Role,
		Method: auth.MethodPassword,
	}, true, nil
}

func RequireAuth(sessions *scs.SessionManager, q *gen.Queries) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			principal, ok, err := LoadPrincipal(c, sessions, q)
			if err != nil {
				return err
			}
			if !ok {
				return handleUnauth(c)
			}
			c.Set(ContextKeyPrincipal, principal)
			return next(c)
		}
	}
}

func RequireRole(role string) echo.MiddlewareFunc {
	role = strings.ToLower(strings.TrimSpace(role))
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			p, ok := PrincipalFromContext(c)
			if !ok {
				return handleUnauth(c)
			}
			if strings.ToLower(strings.TrimSpace(p.Role)) != role {
				if isAPIRequest(c) {
					return c.JSON(http.StatusForbidden, map[string]string{"error": "forbidden"})
				}
				return echo.NewHTTPError(http.StatusForbidden)
			}
			return next(c)
		}
	}
}

func isAPIRequest(c echo.Context) bool {
	return strings.HasPrefix(c.Path(), "/api/") || strings.HasPrefix(c.Request().URL.Path, "/api/")
}

func handleUnauth(c echo.Context) error {
	if isAPIRequest(c) {
		return c.JSON(http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
	}

	location := "/login"
	if c.Request().Method == http.MethodGet {
		if next := SanitizeNext(c.Request().URL.RequestURI()); next != "" {
			location = "/login?next=" + url.QueryEscape(next)
		}
	}
	return c.Redirect(http.StatusSeeOther, location)
}

func SanitizeNext(next string) string {
	next = strings.TrimSpace(next)
	if next == "" || len(next) > 2048 {
		return ""
	}
	if !strings.HasPrefix(next, "/") || strings.HasPrefix(next, "//") {
		return ""
	}

	u, err := url.Parse(next)
	if err != nil || u.IsAbs() || u.Host != "" || u.Scheme != "" {
		return ""
	}
	if u.Path == "/login" || strings.HasPrefix(u.Path, "/login/") {
		return ""
	}
	if strings.Contains(next, "\\") {
		return ""
	}
	return next
}

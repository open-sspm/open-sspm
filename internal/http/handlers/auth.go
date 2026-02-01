package handlers

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
	"github.com/open-sspm/open-sspm/internal/auth"
	"github.com/open-sspm/open-sspm/internal/auth/providers"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/authn"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

func (h *Handlers) HandleLoginGet(c *echo.Context) error {
	if h.Sessions == nil {
		return errors.New("auth sessions not configured")
	}

	if _, ok, err := authn.LoadPrincipal(c, h.Sessions, h.Q); err != nil {
		return err
	} else if ok {
		return c.Redirect(http.StatusSeeOther, "/")
	}

	count, err := h.Q.CountAuthUsers(c.Request().Context())
	if err != nil {
		return err
	}

	csrfToken, _ := c.Get(middleware.DefaultCSRFConfig.ContextKey).(string)
	data := viewmodels.LoginViewData{
		CSRFToken:     csrfToken,
		Next:          authn.SanitizeNext(c.QueryParam("next")),
		SetupRequired: count == 0,
		Toast:         popFlashToast(c),
	}
	return h.RenderComponent(c, views.LoginPage(data))
}

func (h *Handlers) HandleLoginPost(c *echo.Context) error {
	if h.Sessions == nil {
		return errors.New("auth sessions not configured")
	}

	ctx := c.Request().Context()

	count, err := h.Q.CountAuthUsers(ctx)
	if err != nil {
		return err
	}

	email := auth.NormalizeEmail(c.FormValue("email"))
	password := c.FormValue("password")
	next := authn.SanitizeNext(c.FormValue("next"))

	csrfToken, _ := c.Get(middleware.DefaultCSRFConfig.ContextKey).(string)
	data := viewmodels.LoginViewData{
		CSRFToken: csrfToken,
		Email:     email,
		Next:      next,
	}

	if count == 0 {
		data.SetupRequired = true
		return h.RenderComponent(c, views.LoginPage(data))
	}

	if email == "" || strings.TrimSpace(password) == "" {
		data.ErrorMessage = "Invalid email or password."
		return h.RenderComponent(c, views.LoginPage(data))
	}

	passwordProvider := providers.NewPasswordProvider(h.Q)
	principal, err := passwordProvider.Authenticate(ctx, email, password)
	if err != nil {
		if errors.Is(err, auth.ErrInvalidCredentials) {
			data.ErrorMessage = "Invalid email or password."
			return h.RenderComponent(c, views.LoginPage(data))
		}
		return err
	}

	if err := h.Sessions.RenewToken(ctx); err != nil {
		return err
	}
	h.Sessions.Put(ctx, authn.SessionKeyUserID, principal.UserID)

	_ = h.Q.UpdateAuthUserLoginMeta(ctx, gen.UpdateAuthUserLoginMetaParams{
		ID:          principal.UserID,
		LastLoginAt: pgtype.Timestamptz{Time: time.Now(), Valid: true},
		LastLoginIp: strings.TrimSpace(c.RealIP()),
	})

	if next != "" {
		return c.Redirect(http.StatusSeeOther, next)
	}
	return c.Redirect(http.StatusSeeOther, "/")
}

func (h *Handlers) HandleLogoutPost(c *echo.Context) error {
	if h.Sessions == nil {
		return errors.New("auth sessions not configured")
	}

	if err := h.Sessions.Destroy(c.Request().Context()); err != nil {
		return err
	}
	setFlashToast(c, viewmodels.ToastViewData{
		Category: "success",
		Title:    "Signed out",
	})
	return c.Redirect(http.StatusSeeOther, "/login")
}

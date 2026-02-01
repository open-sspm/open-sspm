package handlers

import (
	"net/http"

	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

func (h *Handlers) RenderForbidden(c *echo.Context) error {
	layout, _, err := h.LayoutData(c.Request().Context(), c, "Forbidden")
	if err != nil {
		return h.RenderError(c, err)
	}

	c.Response().Header().Set("Content-Type", "text/html; charset=utf-8")
	c.Response().WriteHeader(http.StatusForbidden)
	if err := views.ForbiddenPage(layout).Render(c.Request().Context(), c.Response()); err != nil {
		return h.RenderError(c, err)
	}
	return nil
}

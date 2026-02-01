package handlers

import (
	"strings"

	"github.com/labstack/echo/v5"
)

func isHX(c *echo.Context) bool {
	if c == nil || c.Request() == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(c.Request().Header.Get("HX-Request")), "true")
}

func isHXTarget(c *echo.Context, target string) bool {
	if c == nil || c.Request() == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(c.Request().Header.Get("HX-Target")), strings.TrimSpace(target))
}

func setHXRedirect(c *echo.Context, url string) {
	if c == nil {
		return
	}
	c.Response().Header().Set("HX-Redirect", url)
}

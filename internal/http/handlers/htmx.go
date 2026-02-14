package handlers

import (
	"net/http"
	"slices"
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

func addVary(c *echo.Context, values ...string) {
	if c == nil || len(values) == 0 {
		return
	}

	header := c.Response().Header()
	existing := header.Values(echo.HeaderVary)

	seen := make(map[string]struct{})
	combined := make([]string, 0, len(existing)+len(values))

	addToken := func(token string) bool {
		token = strings.TrimSpace(token)
		if token == "" {
			return false
		}
		if token == "*" {
			header.Set(echo.HeaderVary, "*")
			return true
		}

		canonical := http.CanonicalHeaderKey(token)
		key := strings.ToLower(canonical)
		if _, ok := seen[key]; ok {
			return false
		}
		seen[key] = struct{}{}
		combined = append(combined, canonical)
		return false
	}

	for _, line := range existing {
		if slices.ContainsFunc(strings.Split(line, ","), addToken) {
			return
		}
	}

	if slices.ContainsFunc(values, addToken) {
		return
	}

	if len(combined) == 0 {
		return
	}
	header.Set(echo.HeaderVary, strings.Join(combined, ", "))
}

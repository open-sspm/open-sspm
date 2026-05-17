package handlers

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func (h *Handlers) HandleGenericIngestGet(c *echo.Context) error {
	connector, channel := genericIngestRoute(c)
	switch connector {
	case "okta":
		if channel == oktaPushChannelEventHook {
			return h.HandleOktaEventHookVerify(c)
		}
	}
	return echo.NewHTTPError(http.StatusNotFound, "unknown ingest channel")
}

func (h *Handlers) HandleGenericIngestPost(c *echo.Context) error {
	connector, channel := genericIngestRoute(c)
	switch connector {
	case "okta":
		switch channel {
		case oktaPushChannelEventHook:
			return h.HandleOktaEventHookPost(c)
		case oktaPushChannelEventBridge:
			return h.HandleOktaEventBridgePost(c)
		}
	}
	return echo.NewHTTPError(http.StatusNotFound, "unknown ingest channel")
}

func genericIngestRoute(c *echo.Context) (connector, channel string) {
	if c == nil {
		return "", ""
	}
	return strings.ToLower(strings.TrimSpace(c.Param("connector"))), strings.ToLower(strings.TrimSpace(c.Param("channel")))
}

package handlers

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

const flashToastCookieName = "oss_toast"

func setFlashToast(c *echo.Context, toast viewmodels.ToastViewData) {
	toast.Category = normalizeToastCategory(toast.Category)
	toast.Title = strings.TrimSpace(toast.Title)
	toast.Description = strings.TrimSpace(toast.Description)
	if toast.Title == "" && toast.Description == "" {
		return
	}

	payload, err := json.Marshal(toast)
	if err != nil {
		return
	}

	c.SetCookie(&http.Cookie{
		Name:     flashToastCookieName,
		Value:    base64.RawURLEncoding.EncodeToString(payload),
		Path:     "/",
		MaxAge:   30,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
}

func popFlashToast(c *echo.Context) *viewmodels.ToastViewData {
	cookie, err := c.Cookie(flashToastCookieName)
	if err != nil || cookie == nil {
		return nil
	}

	c.SetCookie(&http.Cookie{
		Name:     flashToastCookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		Expires:  time.Unix(0, 0),
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})

	raw, err := base64.RawURLEncoding.DecodeString(cookie.Value)
	if err != nil {
		return nil
	}

	var toast viewmodels.ToastViewData
	if err := json.Unmarshal(raw, &toast); err != nil {
		return nil
	}

	toast.Category = normalizeToastCategory(toast.Category)
	toast.Title = strings.TrimSpace(toast.Title)
	toast.Description = strings.TrimSpace(toast.Description)
	if toast.Title == "" && toast.Description == "" {
		return nil
	}

	return &toast
}

func normalizeToastCategory(category string) string {
	switch strings.ToLower(strings.TrimSpace(category)) {
	case "success", "error", "warning", "info":
		return strings.ToLower(strings.TrimSpace(category))
	default:
		return "info"
	}
}

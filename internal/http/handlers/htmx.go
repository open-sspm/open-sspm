package handlers

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"

	"github.com/a-h/templ"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/http/events"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
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

func isHXBoosted(c *echo.Context) bool {
	if c == nil || c.Request() == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(c.Request().Header.Get("HX-Boosted")), "true")
}

func setHXRedirect(c *echo.Context, url string) {
	if c == nil {
		return
	}
	addVary(c, "HX-Request")
	c.Response().Header().Set("HX-Redirect", url)
}

func setHXLocation(c *echo.Context, url string) {
	if c == nil {
		return
	}
	addVary(c, "HX-Request")
	c.Response().Header().Set("HX-Location", url)
}

func addHXTrigger(c *echo.Context, eventName string, detail any) {
	if c == nil {
		return
	}
	eventName = strings.TrimSpace(eventName)
	if eventName == "" {
		return
	}

	header := c.Response().Header()
	triggers := map[string]any{}
	if existing := strings.TrimSpace(header.Get("HX-Trigger")); existing != "" {
		if strings.HasPrefix(existing, "{") {
			_ = json.Unmarshal([]byte(existing), &triggers)
		} else {
			for _, name := range strings.Split(existing, ",") {
				name = strings.TrimSpace(name)
				if name != "" {
					triggers[name] = struct{}{}
				}
			}
		}
	}
	triggers[eventName] = detail

	payload, err := json.Marshal(triggers)
	if err != nil {
		return
	}
	header.Set("HX-Trigger", string(payload))
}

// ToastPayload is the typed shape of an osspm:toast event. Kept narrow but
// distinct from the templ-side view model so we can grow it (actions, undo,
// icon override) without touching every call site.
type ToastPayload struct {
	Category    string `json:"category"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

func setHXToast(c *echo.Context, toast viewmodels.ToastViewData) {
	toast.Category = normalizeToastCategory(toast.Category)
	toast.Title = strings.TrimSpace(toast.Title)
	toast.Description = strings.TrimSpace(toast.Description)
	if toast.Title == "" && toast.Description == "" {
		return
	}
	addHXTrigger(c, events.Toast, ToastPayload{
		Category:    toast.Category,
		Title:       toast.Title,
		Description: toast.Description,
	})
}

func setResponseToast(c *echo.Context, toast viewmodels.ToastViewData) {
	if isHX(c) {
		setHXToast(c, toast)
		return
	}
	setFlashToast(c, toast)
}

// renderListWithHX renders the partial fragment when the request is an HTMX
// swap targeting targetID, and the full page otherwise. It also sets the Vary
// headers needed for safe caching across HX vs. non-HX responses. Boosted
// requests get the full page so htmx can swap the body itself.
func (h *Handlers) renderListWithHX(c *echo.Context, targetID string, fragment, full templ.Component) error {
	addVary(c, "HX-Request", "HX-Target")
	if !isHX(c) || isHXBoosted(c) {
		return h.RenderComponent(c, full)
	}
	if isHXTarget(c, targetID) {
		return h.RenderComponent(c, fragment)
	}
	return c.String(http.StatusBadRequest, "unexpected HTMX target")
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

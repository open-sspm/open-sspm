package handlers

import (
	"strings"

	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

func (h *Handlers) HandleGlobalView(c *echo.Context) error {
	ctx := c.Request().Context()
	layout, _, err := h.LayoutData(ctx, c, "Sources")
	if err != nil {
		return h.RenderError(c, err)
	}

	states, err := h.Registry.LoadStatesWithMetrics(ctx, h.Q)
	if err != nil {
		return h.RenderError(c, err)
	}

	cards := make([]viewmodels.GlobalViewAppCard, 0, len(states))
	for _, state := range states {
		cards = append(cards, h.buildGlobalViewCard(state))
	}

	data := viewmodels.GlobalViewData{
		Layout: layout,
		Cards:  cards,
	}

	return h.RenderComponent(c, views.GlobalViewPage(data))
}

func (h *Handlers) buildGlobalViewCard(state registry.ConnectorState) viewmodels.GlobalViewAppCard {
	def := state.Definition

	active := state.Configured && state.Enabled && strings.TrimSpace(state.ConfigError) == ""

	return viewmodels.GlobalViewAppCard{
		Kind:           def.Kind(),
		Name:           def.DisplayName(),
		CategoryLabel:  globalViewCategoryLabel(def.Role()),
		Subtitle:       state.Subtitle(),
		StatusLabel:    state.StatusLabel(),
		StatusClass:    state.StatusClass(),
		IsActive:       active,
		ShowScore:      active,
		ScoreLabel:     state.ScoreLabel(),
		ScoreValue:     state.CoverageScore(),
		Metrics:        filterStatusKV(state.MetricsKV()),
		Highlights:     filterStatusKV(state.HighlightsKV()),
		PrimaryHref:    state.PrimaryHref(),
		PrimaryLabel:   state.PrimaryLabel(),
		SecondaryHref:  state.SecondaryHref(),
		SecondaryLabel: state.SecondaryLabel(),
	}
}

func globalViewCategoryLabel(role registry.IntegrationRole) string {
	switch role {
	case registry.RoleIdP:
		return "Identity provider"
	default:
		return "Application connector"
	}
}

// filterStatusKV removes rows that duplicate the section-level state framing,
// and drops entries with empty/placeholder values.
func filterStatusKV(items []viewmodels.GlobalViewKV) []viewmodels.GlobalViewKV {
	out := make([]viewmodels.GlobalViewKV, 0, len(items))
	for _, kv := range items {
		label := strings.ToLower(strings.TrimSpace(kv.Label))
		value := strings.TrimSpace(kv.Value)
		if label == "status" || label == "connector" {
			continue
		}
		if value == "" || value == "—" || value == "-" {
			continue
		}
		out = append(out, kv)
	}
	return out
}

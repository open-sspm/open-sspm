package handlers

import (
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

func (h *Handlers) HandleGlobalView(c *echo.Context) error {
	ctx := c.Request().Context()
	layout, _, err := h.LayoutData(ctx, c, "Global View")
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

	return viewmodels.GlobalViewAppCard{
		Kind:           def.Kind(),
		Name:           def.DisplayName(),
		Subtitle:       state.Subtitle(),
		StatusLabel:    state.StatusLabel(),
		StatusClass:    state.StatusClass(),
		Score:          state.CoverageScore(),
		ScoreLabel:     state.ScoreLabel(),
		Metrics:        state.MetricsKV(),
		Highlights:     state.HighlightsKV(),
		PrimaryHref:    state.PrimaryHref(),
		PrimaryLabel:   state.PrimaryLabel(),
		SecondaryHref:  state.SecondaryHref(),
		SecondaryLabel: state.SecondaryLabel(),
	}
}

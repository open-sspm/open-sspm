package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/http/querystate"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

func (h *Handlers) HandleAskBarSuggestions(c *echo.Context) error {
	ctx := c.Request().Context()
	cfg, err := h.askBarConfigForSuggestionScope(ctx, strings.TrimSpace(c.QueryParam("scope")))
	if err != nil {
		if errors.Is(err, errUnknownAskBarSuggestionScope) {
			return c.String(http.StatusBadRequest, "unknown askbar suggestion scope")
		}
		return h.RenderError(c, err)
	}
	if cfg.KeywordTokens == nil && cfg.FieldAliases == nil {
		return c.NoContent(http.StatusNoContent)
	}

	data := buildAskBarSuggestions(cfg, c.QueryParam("q"), isTruthyParam(c.QueryParam("force")))
	if len(data.Items) == 0 && data.Error == "" && !data.ShowFreeText {
		return c.NoContent(http.StatusNoContent)
	}
	return h.RenderComponent(c, views.AskBarSuggestions(data))
}

var errUnknownAskBarSuggestionScope = errors.New("unknown askbar suggestion scope")

func (h *Handlers) askBarConfigForSuggestionScope(ctx context.Context, scope string) (views.AskBarConfig, error) {
	switch strings.ToLower(strings.TrimSpace(scope)) {
	case "identities":
		return views.IdentitiesAskBar(querystate.IdentitiesQuery{}), nil
	case "state":
		return views.StateAskBar(querystate.BasicListQuery{}, "", ""), nil
	case "apps":
		statuses, err := h.listOktaAppStatuses(ctx)
		if err != nil {
			return views.AskBarConfig{}, err
		}
		return views.AppsAskBar(viewmodels.AppsViewData{StatusOptions: statuses}), nil
	case "credentials":
		stateView, err := h.LoadConnectorStateView(ctx)
		if err != nil {
			return views.AskBarConfig{}, err
		}
		return views.CredentialsAskBar(viewmodels.CredentialsViewData{Sources: availableProgrammaticSources(stateView)}), nil
	case "connected-apps":
		return views.ConnectedAppsAskBar(viewmodels.ConnectedAppsViewData{}), nil
	case "app-assets":
		stateView, err := h.LoadConnectorStateView(ctx)
		if err != nil {
			return views.AskBarConfig{}, err
		}
		return views.AppAssetsAskBar(viewmodels.AppAssetsViewData{Sources: availableProgrammaticSources(stateView)}), nil
	case "discovery-apps":
		stateView, err := h.LoadConnectorStateView(ctx)
		if err != nil {
			return views.AskBarConfig{}, err
		}
		return views.DiscoveryAppsAskBar(viewmodels.DiscoveryAppsViewData{SourceOptions: sourceKindOptions(discoverySourceOptions(stateView))}), nil
	case "non-human-identities":
		stateView, err := h.LoadConnectorStateView(ctx)
		if err != nil {
			return views.AskBarConfig{}, err
		}
		return views.NonHumanIdentitiesAskBar(viewmodels.NonHumanIdentitiesViewData{Sources: availableIdentitySourcePairs(stateView)}), nil
	default:
		return views.AskBarConfig{}, fmt.Errorf("%w: %s", errUnknownAskBarSuggestionScope, scope)
	}
}

type askBarSuggestionMatch struct {
	item views.AskBarSuggestionItem
	rank int
}

func buildAskBarSuggestions(cfg views.AskBarConfig, rawText string, force bool) views.AskBarSuggestionsViewData {
	displayText := strings.TrimSpace(rawText)
	if displayText == "" && !force {
		return views.AskBarSuggestionsViewData{}
	}
	t := normalizeAskBarSuggestText(displayText)
	keywords := cfg.KeywordTokens
	if keywords == nil {
		keywords = map[string]views.AskBarKeyword{}
	}
	aliases := cfg.FieldAliases
	if aliases == nil {
		aliases = map[string]string{}
	}
	fieldLabels := cfg.FieldLabel
	if fieldLabels == nil {
		fieldLabels = map[string]string{}
	}
	freeTextFields := map[string]struct{}{}
	for _, field := range cfg.FreeTextFields {
		freeTextFields[field] = struct{}{}
	}

	seen := map[string]struct{}{}
	matches := []askBarSuggestionMatch{}
	push := func(tok views.AskBarKeyword, rank int) {
		key := tok.Field + "\x00" + tok.Value
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		fieldLabel := strings.TrimSpace(fieldLabels[tok.Field])
		if fieldLabel == "" {
			fieldLabel = tok.Field
		}
		matches = append(matches, askBarSuggestionMatch{
			rank: rank,
			item: views.AskBarSuggestionItem{
				Field:      tok.Field,
				Value:      tok.Value,
				Label:      tok.Label,
				Tone:       tok.Tone,
				FieldLabel: fieldLabel,
			},
		})
	}

	var errorText string
	if colonIdx := strings.Index(t, ":"); colonIdx >= 0 {
		fieldPart := strings.TrimSpace(t[:colonIdx])
		valuePart := strings.TrimSpace(t[colonIdx+1:])
		if field := aliases[fieldPart]; field != "" {
			if valuePart == "" {
				for kw, tok := range keywords {
					if tok.Field == field {
						_ = kw
						push(tok, 0)
					}
				}
			} else if _, ok := freeTextFields[field]; ok {
				fieldLabel := strings.TrimSpace(fieldLabels[field])
				if fieldLabel == "" {
					fieldLabel = field
				}
				matches = append(matches, askBarSuggestionMatch{
					rank: 0,
					item: views.AskBarSuggestionItem{
						Field:      field,
						Value:      valuePart,
						Label:      valuePart,
						FieldLabel: fieldLabel,
					},
				})
			} else {
				for kw, tok := range keywords {
					if tok.Field != field {
						continue
					}
					key := strings.ToLower(kw)
					label := strings.ToLower(tok.Label)
					value := strings.ToLower(tok.Value)
					switch {
					case strings.HasPrefix(key, valuePart), strings.HasPrefix(label, valuePart), strings.HasPrefix(value, valuePart):
						push(tok, 0)
					case strings.Contains(label, valuePart):
						push(tok, 1)
					}
				}
				if len(matches) == 0 {
					fieldLabel := strings.TrimSpace(fieldLabels[field])
					if fieldLabel == "" {
						fieldLabel = field
					}
					errorText = fmt.Sprintf("No %s filter matches %q.", strings.ToLower(fieldLabel), valuePart)
				}
			}
		}
	} else if force && t == "" {
		for _, tok := range keywords {
			push(tok, 0)
		}
	} else {
		for kw, tok := range keywords {
			key := strings.ToLower(kw)
			if strings.HasPrefix(key, t) {
				push(tok, 0)
			}
		}
		for alias, field := range aliases {
			if strings.HasPrefix(alias, t) && len(alias) > 1 {
				for _, tok := range keywords {
					if tok.Field == field {
						push(tok, 2)
					}
				}
			}
		}
		if len(t) >= 2 {
			for _, tok := range keywords {
				if strings.HasPrefix(strings.ToLower(tok.Label), t) {
					push(tok, 3)
				}
			}
		}
	}

	sort.SliceStable(matches, func(i, j int) bool {
		if matches[i].rank != matches[j].rank {
			return matches[i].rank < matches[j].rank
		}
		if matches[i].item.FieldLabel != matches[j].item.FieldLabel {
			return matches[i].item.FieldLabel < matches[j].item.FieldLabel
		}
		return matches[i].item.Label < matches[j].item.Label
	})

	limit := len(matches)
	if limit > 8 {
		limit = 8
	}
	items := make([]views.AskBarSuggestionItem, 0, limit)
	for i := 0; i < limit; i++ {
		items = append(items, matches[i].item)
	}

	return views.AskBarSuggestionsViewData{
		Items:        items,
		DisplayText:  displayText,
		Error:        errorText,
		ShowFreeText: displayText != "",
	}
}

func normalizeAskBarSuggestText(text string) string {
	out := strings.ToLower(strings.TrimSpace(text))
	out = strings.ReplaceAll(out, "needs action", "needs-action")
	out = strings.ReplaceAll(out, "need action", "needs-action")
	out = strings.ReplaceAll(out, "never seen", "never-seen")
	return out
}

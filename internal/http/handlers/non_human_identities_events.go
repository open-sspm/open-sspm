package handlers

import (
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/authn"
	"github.com/open-sspm/open-sspm/internal/http/querystate"
)

const (
	nonHumanIdentitiesEventInventoryView = "inventory_view"
	nonHumanIdentitiesEventFilterUse     = "filter_use"
	nonHumanIdentitiesEventDetailOpen    = "detail_open"
	nonHumanIdentitiesEventOutboundClick = "outbound_click"
)

func (h *Handlers) trackNonHumanIdentitiesListEvents(c *echo.Context, query querystate.NonHumanIdentitiesQuery) {
	if h == nil || c == nil {
		return
	}

	if !(isNonHumanIdentitiesInventoryTarget(c) || isNonHumanIdentitiesResultsTarget(c)) {
		h.insertNonHumanIdentitiesEvent(c, nonHumanIdentitiesEventInventoryView, "", "", "", query.TrackingSignature())
	}

	currentSignature := query.TrackingSignature()
	previousSignature, ok := nonHumanIdentitiesRefererQuerySignature(c)
	if !ok || previousSignature == currentSignature {
		return
	}
	h.insertNonHumanIdentitiesEvent(c, nonHumanIdentitiesEventFilterUse, "", "", "", currentSignature)
}

func (h *Handlers) trackNonHumanIdentityDetailOpen(c *echo.Context, principalRef string) {
	h.insertNonHumanIdentitiesEvent(c, nonHumanIdentitiesEventDetailOpen, principalRef, "", "", "")
}

func (h *Handlers) trackNonHumanIdentitiesOutboundClick(c *echo.Context, targetKind string, targetID int64) {
	if targetID <= 0 {
		return
	}

	info, ok := nonHumanIdentitiesRefererInfo(c)
	if !ok {
		return
	}

	if info.path != querystate.NonHumanIdentitiesBasePath() && !strings.HasPrefix(info.path, querystate.NonHumanIdentitiesBasePath()+"/") {
		return
	}

	h.insertNonHumanIdentitiesEvent(
		c,
		nonHumanIdentitiesEventOutboundClick,
		info.principalRef,
		targetKind,
		strconv.FormatInt(targetID, 10),
		querystate.NonHumanIdentitiesTrackingSignature(info.query),
	)
}

func (h *Handlers) insertNonHumanIdentitiesEvent(c *echo.Context, eventKind, principalRef, targetKind, targetRef, filterSignature string) {
	if h == nil || h.Q == nil || c == nil {
		return
	}

	principal, ok := authn.PrincipalFromContext(c)
	if !ok || principal.UserID <= 0 {
		return
	}

	_ = h.Q.InsertNonHumanAccessEvent(c.Request().Context(), gen.InsertNonHumanAccessEventParams{
		AuthUserID:      principal.UserID,
		AuthUserRole:    principal.Role,
		EventKind:       eventKind,
		PrincipalRef:    principalRef,
		TargetKind:      targetKind,
		TargetRef:       targetRef,
		FilterSignature: filterSignature,
		OccurredAt:      pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true},
	})
}

type nonHumanIdentitiesReferer struct {
	path         string
	query        url.Values
	principalRef string
}

func nonHumanIdentitiesRefererInfo(c *echo.Context) (nonHumanIdentitiesReferer, bool) {
	if c == nil || c.Request() == nil {
		return nonHumanIdentitiesReferer{}, false
	}

	raw := strings.TrimSpace(c.Request().Header.Get("Referer"))
	if raw == "" {
		return nonHumanIdentitiesReferer{}, false
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return nonHumanIdentitiesReferer{}, false
	}
	if parsed.Host != "" && !sameHostname(parsed.Host, c.Request().Host) {
		return nonHumanIdentitiesReferer{}, false
	}

	path := strings.TrimSpace(parsed.Path)
	if path == "" {
		return nonHumanIdentitiesReferer{}, false
	}

	info := nonHumanIdentitiesReferer{
		path:  path,
		query: parsed.Query(),
	}
	if strings.HasPrefix(path, querystate.NonHumanIdentitiesBasePath()+"/") {
		info.principalRef = strings.TrimPrefix(path, querystate.NonHumanIdentitiesBasePath()+"/")
	}
	return info, true
}

func sameHostname(a, b string) bool {
	a = strings.TrimSpace(a)
	b = strings.TrimSpace(b)
	if a == "" || b == "" {
		return a == b
	}

	aHostname := (&url.URL{Host: a}).Hostname()
	if aHostname == "" {
		aHostname = a
	}
	bHostname := (&url.URL{Host: b}).Hostname()
	if bHostname == "" {
		bHostname = b
	}
	return strings.EqualFold(aHostname, bHostname)
}

func nonHumanIdentitiesRefererQuerySignature(c *echo.Context) (string, bool) {
	info, ok := nonHumanIdentitiesRefererInfo(c)
	if !ok || info.path != querystate.NonHumanIdentitiesBasePath() {
		return "", false
	}
	return querystate.NonHumanIdentitiesTrackingSignature(info.query), true
}

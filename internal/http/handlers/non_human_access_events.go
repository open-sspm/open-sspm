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
	nonHumanAccessEventInventoryView = "inventory_view"
	nonHumanAccessEventFilterUse     = "filter_use"
	nonHumanAccessEventDetailOpen    = "detail_open"
	nonHumanAccessEventOutboundClick = "outbound_click"
)

func (h *Handlers) trackNonHumanAccessListEvents(c *echo.Context, query querystate.NonHumanAccessQuery) {
	if h == nil || c == nil {
		return
	}

	if !(isNonHumanAccessInventoryTarget(c) || isNonHumanAccessResultsTarget(c)) {
		h.insertNonHumanAccessEvent(c, nonHumanAccessEventInventoryView, "", "", "", query.TrackingSignature())
	}

	currentSignature := query.TrackingSignature()
	previousSignature, ok := nonHumanAccessRefererQuerySignature(c)
	if !ok || previousSignature == currentSignature {
		return
	}
	h.insertNonHumanAccessEvent(c, nonHumanAccessEventFilterUse, "", "", "", currentSignature)
}

func (h *Handlers) trackNonHumanAccessDetailOpen(c *echo.Context, principalRef string) {
	h.insertNonHumanAccessEvent(c, nonHumanAccessEventDetailOpen, principalRef, "", "", "")
}

func (h *Handlers) trackNonHumanAccessOutboundClick(c *echo.Context, targetKind string, targetID int64) {
	if targetID <= 0 {
		return
	}

	info, ok := nonHumanAccessRefererInfo(c)
	if !ok {
		return
	}

	if info.path != querystate.NonHumanAccessBasePath() && !strings.HasPrefix(info.path, querystate.NonHumanAccessBasePath()+"/") {
		return
	}

	h.insertNonHumanAccessEvent(
		c,
		nonHumanAccessEventOutboundClick,
		info.principalRef,
		targetKind,
		strconv.FormatInt(targetID, 10),
		querystate.NonHumanAccessTrackingSignature(info.query),
	)
}

func (h *Handlers) insertNonHumanAccessEvent(c *echo.Context, eventKind, principalRef, targetKind, targetRef, filterSignature string) {
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

type nonHumanAccessReferer struct {
	path         string
	query        url.Values
	principalRef string
}

func nonHumanAccessRefererInfo(c *echo.Context) (nonHumanAccessReferer, bool) {
	if c == nil || c.Request() == nil {
		return nonHumanAccessReferer{}, false
	}

	raw := strings.TrimSpace(c.Request().Header.Get("Referer"))
	if raw == "" {
		return nonHumanAccessReferer{}, false
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return nonHumanAccessReferer{}, false
	}
	if parsed.Host != "" && !sameHostname(parsed.Host, c.Request().Host) {
		return nonHumanAccessReferer{}, false
	}

	path := strings.TrimSpace(parsed.Path)
	if path == "" {
		return nonHumanAccessReferer{}, false
	}

	info := nonHumanAccessReferer{
		path:  path,
		query: parsed.Query(),
	}
	if strings.HasPrefix(path, querystate.NonHumanAccessBasePath()+"/") {
		info.principalRef = strings.TrimPrefix(path, querystate.NonHumanAccessBasePath()+"/")
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

func nonHumanAccessRefererQuerySignature(c *echo.Context) (string, bool) {
	info, ok := nonHumanAccessRefererInfo(c)
	if !ok || info.path != querystate.NonHumanAccessBasePath() {
		return "", false
	}
	return querystate.NonHumanAccessTrackingSignature(info.query), true
}

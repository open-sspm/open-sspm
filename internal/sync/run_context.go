package sync

import (
	"context"
	"strings"
)

type syncRunContextKey int

const (
	syncRunContextKeyForce syncRunContextKey = iota
	syncRunContextKeyConnectorScope
)

type TriggerRequest struct {
	ConnectorKind string `json:"connector_kind,omitempty"`
	SourceName    string `json:"source_name,omitempty"`
}

func (r TriggerRequest) Normalized() TriggerRequest {
	kind := strings.ToLower(strings.TrimSpace(r.ConnectorKind))
	name := strings.TrimSpace(r.SourceName)
	if kind == "" || name == "" {
		return TriggerRequest{}
	}
	return TriggerRequest{
		ConnectorKind: kind,
		SourceName:    name,
	}
}

func (r TriggerRequest) HasConnectorScope() bool {
	n := r.Normalized()
	return n.ConnectorKind != "" && n.SourceName != ""
}

func WithForcedSync(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, syncRunContextKeyForce, true)
}

func WithConnectorScope(ctx context.Context, connectorKind, sourceName string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	req := TriggerRequest{
		ConnectorKind: connectorKind,
		SourceName:    sourceName,
	}.Normalized()
	if !req.HasConnectorScope() {
		return ctx
	}
	return context.WithValue(ctx, syncRunContextKeyConnectorScope, req)
}

func IsForcedSync(ctx context.Context) bool {
	v, ok := ctx.Value(syncRunContextKeyForce).(bool)
	return ok && v
}

func ConnectorScopeFromContext(ctx context.Context) (connectorKind, sourceName string, ok bool) {
	if ctx == nil {
		return "", "", false
	}
	req, ok := ctx.Value(syncRunContextKeyConnectorScope).(TriggerRequest)
	if !ok {
		return "", "", false
	}
	req = req.Normalized()
	if !req.HasConnectorScope() {
		return "", "", false
	}
	return req.ConnectorKind, req.SourceName, true
}

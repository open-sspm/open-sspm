package registry

import (
	"context"
	"strings"
)

type resourceScopeContextKey struct{}

func WithResourceScope(ctx context.Context, resource string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	resource = strings.TrimSpace(resource)
	if resource == "" {
		return ctx
	}
	return context.WithValue(ctx, resourceScopeContextKey{}, resource)
}

func ResourceScopeFromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	resource, ok := ctx.Value(resourceScopeContextKey{}).(string)
	resource = strings.TrimSpace(resource)
	return resource, ok && resource != ""
}

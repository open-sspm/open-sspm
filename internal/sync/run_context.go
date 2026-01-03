package sync

import "context"

type syncRunContextKey int

const (
	syncRunContextKeyForce syncRunContextKey = iota
)

func WithForcedSync(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, syncRunContextKeyForce, true)
}

func IsForcedSync(ctx context.Context) bool {
	v, ok := ctx.Value(syncRunContextKeyForce).(bool)
	return ok && v
}

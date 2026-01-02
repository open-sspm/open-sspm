package matching

import (
	"context"
	"strings"

	"github.com/open-sspm/open-sspm/internal/db/gen"
)

func NormalizeEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

func AutoLinkByEmail(ctx context.Context, q *gen.Queries, sourceKind, sourceName string) (int, error) {
	linked, err := q.BulkAutoLinkByEmail(ctx, gen.BulkAutoLinkByEmailParams{
		SourceKind: sourceKind,
		SourceName: sourceName,
	})
	return int(linked), err
}

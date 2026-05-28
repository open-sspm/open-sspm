package identity

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/metrics"
)

func RefreshMetrics(ctx context.Context, q *gen.Queries, now time.Time) error {
	if q == nil {
		return nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	metrics.NonHumanPrincipalsTotal.Set(0)
	metrics.NonHumanPrincipalsWithAccountableOwnerTotal.Set(0)
	metrics.NonHumanHighRiskCredentialsTotal.Set(0)
	metrics.NonHumanHighRiskCredentialsWithAttributionTotal.Set(0)
	metrics.NonHumanIdentitiesWeeklyAdminReviewSessions.Set(0)

	ownerCoverage, err := q.CountConfiguredNonHumanPrincipalOwnerCoverage(ctx)
	if err != nil {
		return err
	}
	metrics.NonHumanPrincipalsTotal.Set(float64(ownerCoverage.PrincipalCount))
	metrics.NonHumanPrincipalsWithAccountableOwnerTotal.Set(float64(ownerCoverage.WithOwnerCount))

	credentialCoverage, err := q.CountConfiguredNonHumanHighRiskCredentialAttribution(ctx)
	if err != nil {
		return err
	}
	metrics.NonHumanHighRiskCredentialsTotal.Set(float64(credentialCoverage.HighRiskCredentialCount))
	metrics.NonHumanHighRiskCredentialsWithAttributionTotal.Set(float64(credentialCoverage.HighRiskWithAttributionCount))

	weeklySessions, err := q.CountNonHumanAccessWeeklyAdminReviewSessions(ctx, pgtype.Timestamptz{
		Time:  now.Add(-7 * 24 * time.Hour),
		Valid: true,
	})
	if err != nil {
		return err
	}
	metrics.NonHumanIdentitiesWeeklyAdminReviewSessions.Set(float64(weeklySessions))

	return nil
}

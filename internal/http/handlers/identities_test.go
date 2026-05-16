package handlers

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestIdentityDormancyHandlesInvalidTimestamps(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	if isDormantAt(now, pgtype.Timestamptz{}, 60*24*time.Hour) {
		t.Fatal("invalid timestamp should not be dormant")
	}
}

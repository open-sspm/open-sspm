package main

import (
	"testing"
	"time"
)

func TestEventRetentionCleanupCutoffUsesUTCDateBoundary(t *testing.T) {
	now := time.Date(2026, 5, 25, 15, 2, 13, 0, time.FixedZone("CEST", 2*60*60))
	got := eventRetentionCleanupCutoff(now, 30)
	want := time.Date(2026, 4, 25, 0, 0, 0, 0, time.UTC)

	if !got.Valid || !got.Time.Equal(want) {
		t.Fatalf("eventRetentionCleanupCutoff() = %+v, want %s", got, want)
	}
}

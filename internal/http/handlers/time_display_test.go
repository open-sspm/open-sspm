package handlers

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestCalendarDateDisplayWithRelative(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	today := now.Add(-6 * time.Hour)
	threeDaysAgo := now.Add(-72 * time.Hour)

	cases := []struct {
		name         string
		value        pgtype.Timestamptz
		wantLabel    string
		wantRelative string
	}{
		{
			name:         "today",
			value:        timestamptz(today),
			wantLabel:    today.Format("Jan 2, 2006"),
			wantRelative: "today",
		},
		{
			name:         "days ago",
			value:        timestamptz(threeDaysAgo),
			wantLabel:    threeDaysAgo.Format("Jan 2, 2006"),
			wantRelative: "3d ago",
		},
		{
			name:         "invalid",
			value:        pgtype.Timestamptz{},
			wantLabel:    "—",
			wantRelative: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := calendarDateWithRelativeDisplay(tc.value)
			if got.Label != tc.wantLabel {
				t.Fatalf("label = %q, want %q", got.Label, tc.wantLabel)
			}
			if got.Relative != tc.wantRelative {
				t.Fatalf("relative = %q, want %q", got.Relative, tc.wantRelative)
			}
		})
	}
}

func TestRelativeWithTitleDisplay(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	valid := timestamptz(time.Date(2026, 4, 10, 9, 30, 0, 0, time.UTC))

	got := relativeWithTitleDisplay(now, valid, "Never", "Never logged in")
	if got.Label != "1d ago" {
		t.Fatalf("label = %q, want 1d ago", got.Label)
	}
	if got.Title != "Apr 10, 2026 9:30 AM UTC" {
		t.Fatalf("title = %q", got.Title)
	}

	invalid := relativeWithTitleDisplay(now, pgtype.Timestamptz{}, "Never", "Never logged in")
	if invalid.Label != "Never" {
		t.Fatalf("invalid label = %q, want Never", invalid.Label)
	}
	if invalid.Title != "Never logged in" {
		t.Fatalf("invalid title = %q, want Never logged in", invalid.Title)
	}
}

func TestDateDisplay(t *testing.T) {
	t.Parallel()

	valid := dateDisplay(pgtype.Date{Time: time.Date(2026, 4, 20, 0, 0, 0, 0, time.UTC), Valid: true})
	if valid.Label != "2026-04-20" {
		t.Fatalf("label = %q, want 2026-04-20", valid.Label)
	}

	invalid := dateDisplay(pgtype.Date{})
	if invalid.Label != "" {
		t.Fatalf("invalid label = %q, want empty", invalid.Label)
	}
}

func TestRelativeDateLabelAt(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name string
		when time.Time
		want string
	}{
		{
			name: "later today",
			when: now.Add(6 * time.Hour),
			want: "today",
		},
		{
			name: "future days",
			when: now.Add(4 * 24 * time.Hour),
			want: "in 4d",
		},
		{
			name: "past days",
			when: now.Add(-3 * 24 * time.Hour),
			want: "3d ago",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := relativeDateLabelAt(now, tc.when); got != tc.want {
				t.Fatalf("relativeDateLabelAt() = %q, want %q", got, tc.want)
			}
		})
	}
}

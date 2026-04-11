package handlers

import (
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

const timeDisplayTitleLayout = "Jan 2, 2006 3:04 PM UTC"

func calendarDateDisplay(value pgtype.Timestamptz) viewmodels.TimeDisplay {
	return calendarDateDisplayWithFallback(value, "—")
}

func calendarDateDisplayOrEmpty(value pgtype.Timestamptz) viewmodels.TimeDisplay {
	return calendarDateDisplayWithFallback(value, "")
}

func calendarDateDisplayWithFallback(value pgtype.Timestamptz, fallback string) viewmodels.TimeDisplay {
	if !value.Valid {
		return viewmodels.TimeDisplay{Label: fallback}
	}
	return viewmodels.TimeDisplay{Label: value.Time.UTC().Format("Jan 2, 2006")}
}

func calendarDateWithRelativeDisplay(value pgtype.Timestamptz) viewmodels.TimeDisplay {
	if !value.Valid {
		return viewmodels.TimeDisplay{Label: "—"}
	}
	return viewmodels.TimeDisplay{
		Label:    value.Time.UTC().Format("Jan 2, 2006"),
		Relative: relativeDateLabel(value.Time),
	}
}

func dateDisplay(value pgtype.Date) viewmodels.TimeDisplay {
	if !value.Valid {
		return viewmodels.TimeDisplay{}
	}
	return viewmodels.TimeDisplay{Label: value.Time.UTC().Format("2006-01-02")}
}

func relativeWithTitleDisplay(now time.Time, value pgtype.Timestamptz, emptyLabel, emptyTitle string) viewmodels.TimeDisplay {
	if !value.Valid {
		return viewmodels.TimeDisplay{
			Label: emptyLabel,
			Title: emptyTitle,
		}
	}
	return viewmodels.TimeDisplay{
		Label: formatAge(now, value.Time),
		Title: value.Time.UTC().Format(timeDisplayTitleLayout),
	}
}

func relativeDateLabel(t time.Time) string {
	days := int(time.Since(t).Hours() / 24)
	switch {
	case days <= 0:
		return "today"
	case days == 1:
		return "1d ago"
	case days < 365:
		return strconv.Itoa(days) + "d ago"
	default:
		return strconv.Itoa(days/365) + "y ago"
	}
}

package handlers

import (
	"errors"
	"testing"
	"time"
)

func TestOktaAppStatusesCacheReusesFreshValues(t *testing.T) {
	current := time.Date(2026, time.April, 12, 10, 0, 0, 0, time.UTC)
	cache := oktaAppStatusesCache{
		now: func() time.Time {
			return current
		},
	}

	calls := 0
	fetch := func() ([]string, error) {
		calls++
		return []string{"active", "inactive"}, nil
	}

	first, err := cache.get(fetch)
	if err != nil {
		t.Fatalf("cache.get() first = %v", err)
	}
	second, err := cache.get(fetch)
	if err != nil {
		t.Fatalf("cache.get() second = %v", err)
	}

	if calls != 1 {
		t.Fatalf("fetch calls = %d, want 1", calls)
	}
	first[0] = "mutated"

	third, err := cache.get(fetch)
	if err != nil {
		t.Fatalf("cache.get() third = %v", err)
	}
	if third[0] != "active" {
		t.Fatalf("cached status = %q, want %q", third[0], "active")
	}
	if second[0] != "active" {
		t.Fatalf("second status = %q, want %q", second[0], "active")
	}
}

func TestOktaAppStatusesCacheRefreshesAfterTTL(t *testing.T) {
	current := time.Date(2026, time.April, 12, 10, 0, 0, 0, time.UTC)
	cache := oktaAppStatusesCache{
		now: func() time.Time {
			return current
		},
	}

	calls := 0
	fetch := func() ([]string, error) {
		calls++
		return []string{"active", "inactive", "pending"}, nil
	}

	if _, err := cache.get(fetch); err != nil {
		t.Fatalf("cache.get() initial = %v", err)
	}

	current = current.Add(oktaAppStatusesCacheTTL)
	if _, err := cache.get(fetch); err != nil {
		t.Fatalf("cache.get() after ttl = %v", err)
	}

	if calls != 2 {
		t.Fatalf("fetch calls = %d, want 2", calls)
	}
}

func TestOktaAppStatusesCacheDoesNotPopulateOnError(t *testing.T) {
	current := time.Date(2026, time.April, 12, 10, 0, 0, 0, time.UTC)
	cache := oktaAppStatusesCache{
		now: func() time.Time {
			return current
		},
	}

	calls := 0
	wantErr := errors.New("boom")
	fetch := func() ([]string, error) {
		calls++
		if calls == 1 {
			return nil, wantErr
		}
		return []string{"active"}, nil
	}

	if _, err := cache.get(fetch); !errors.Is(err, wantErr) {
		t.Fatalf("cache.get() err = %v, want %v", err, wantErr)
	}
	if _, err := cache.get(fetch); err != nil {
		t.Fatalf("cache.get() retry = %v", err)
	}
	if calls != 2 {
		t.Fatalf("fetch calls = %d, want 2", calls)
	}
}

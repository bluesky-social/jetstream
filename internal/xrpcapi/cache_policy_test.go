package xrpcapi

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fixedCompactionSchedule struct {
	next time.Time
	ok   bool
}

func (s fixedCompactionSchedule) NextCompactionAt() (time.Time, bool) {
	return s.next, s.ok
}

func TestDynamicCacheMaxAge(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	future := now.Add(2 * time.Hour)
	cases := []struct {
		name       string
		configured time.Duration
		grace      time.Duration
		schedule   CompactionSchedule
		want       time.Duration
	}{
		{name: "disabled max age", configured: 0, grace: time.Minute, schedule: fixedCompactionSchedule{next: future, ok: true}},
		{name: "unknown schedule", configured: time.Hour, grace: time.Minute},
		{name: "schedule reports not ready", configured: time.Hour, grace: time.Minute, schedule: fixedCompactionSchedule{next: future, ok: false}},
		{name: "zero timestamp", configured: time.Hour, grace: time.Minute, schedule: fixedCompactionSchedule{ok: true}},
		{name: "deadline expired", configured: time.Hour, grace: time.Minute, schedule: fixedCompactionSchedule{next: now.Add(-2 * time.Hour), ok: true}},
		{name: "negative grace", configured: time.Hour, grace: -time.Minute, schedule: fixedCompactionSchedule{next: future, ok: true}},
		{name: "max age caps future window", configured: time.Hour, grace: time.Minute, schedule: fixedCompactionSchedule{next: future, ok: true}, want: time.Hour},
		{name: "grace extends active pass", configured: time.Hour, grace: 10 * time.Minute, schedule: fixedCompactionSchedule{next: now.Add(-5 * time.Minute), ok: true}, want: 5 * time.Minute},
		{name: "overrun beyond grace", configured: time.Hour, grace: 10 * time.Minute, schedule: fixedCompactionSchedule{next: now.Add(-11 * time.Minute), ok: true}},
		{name: "remaining window caps max age", configured: time.Hour, grace: 10 * time.Minute, schedule: fixedCompactionSchedule{next: now.Add(20 * time.Minute), ok: true}, want: 30 * time.Minute},
		{name: "fractional deadline rounds down", configured: time.Hour, schedule: fixedCompactionSchedule{next: now.Add(time.Minute + 500*time.Millisecond), ok: true}, want: time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := dynamicCacheMaxAge(now, tc.configured, tc.grace, tc.schedule)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestDynamicCacheHeadersNoCacheForUnknownOrExpiredSchedule(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name     string
		schedule CompactionSchedule
	}{
		{name: "unknown", schedule: nil},
		{name: "disabled", schedule: fixedCompactionSchedule{next: time.Time{}, ok: false}},
		{name: "expired", schedule: fixedCompactionSchedule{next: now.Add(-time.Hour), ok: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			maxAge := dynamicCacheMaxAge(now, time.Hour, time.Minute, tc.schedule)
			require.Equal(t, "public, no-cache", cacheControlHeader(maxAge))
		})
	}
}

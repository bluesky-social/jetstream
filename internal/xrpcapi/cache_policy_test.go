package xrpcapi

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fixedCompactionDeadline struct {
	next time.Time
	ok   bool
}

func (s fixedCompactionDeadline) NextCompactionAt() (time.Time, bool) {
	return s.next, s.ok
}

func TestCacheLifetime(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	future := now.Add(2 * time.Hour)
	cases := []struct {
		name     string
		grace    time.Duration
		deadline CompactionDeadline
		want     time.Duration
	}{
		{name: "unknown deadline", grace: time.Minute},
		{name: "deadline reports not ready", grace: time.Minute, deadline: fixedCompactionDeadline{next: future, ok: false}},
		{name: "zero timestamp", grace: time.Minute, deadline: fixedCompactionDeadline{ok: true}},
		{name: "deadline expired", grace: time.Minute, deadline: fixedCompactionDeadline{next: now.Add(-2 * time.Hour), ok: true}},
		{name: "negative grace", grace: -time.Minute, deadline: fixedCompactionDeadline{next: future, ok: true}},
		{name: "future deadline with grace", grace: time.Minute, deadline: fixedCompactionDeadline{next: future, ok: true}, want: 2*time.Hour + time.Minute},
		{name: "active pass within grace", grace: 10 * time.Minute, deadline: fixedCompactionDeadline{next: now.Add(-5 * time.Minute), ok: true}, want: 5 * time.Minute},
		{name: "overrun beyond grace", grace: 10 * time.Minute, deadline: fixedCompactionDeadline{next: now.Add(-11 * time.Minute), ok: true}},
		{name: "fractional deadline truncates", deadline: fixedCompactionDeadline{next: now.Add(time.Minute + 500*time.Millisecond), ok: true}, want: time.Minute},
		{name: "sub-second lifetime truncates to zero", deadline: fixedCompactionDeadline{next: now.Add(500 * time.Millisecond), ok: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := cacheLifetime(now, tc.grace, tc.deadline)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestCacheLifetimeNoCacheForUnknownOrExpiredDeadline(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name     string
		deadline CompactionDeadline
	}{
		{name: "unknown", deadline: nil},
		{name: "disabled", deadline: fixedCompactionDeadline{next: time.Time{}, ok: false}},
		{name: "expired", deadline: fixedCompactionDeadline{next: now.Add(-time.Hour), ok: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lifetime := cacheLifetime(now, time.Minute, tc.deadline)
			require.Equal(t, "public, no-cache", cacheControlHeader(lifetime))
		})
	}
}

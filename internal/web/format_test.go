package web

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHumanDuration(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   time.Duration
		want string
	}{
		{0, "0s"},
		{500 * time.Millisecond, "0s"},
		{45 * time.Second, "45s"},
		{2*time.Minute + 5*time.Second, "2m 5s"},
		{3 * time.Hour, "3h 0m"},
		{27*time.Hour + 30*time.Minute, "1d 3h"},
	}
	for _, c := range cases {
		require.Equal(t, c.want, humanDuration(c.in), "humanDuration(%v)", c.in)
	}
}

func TestRelativeTime(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	require.Equal(t, "never", relativeTime(time.Time{}, now))
	require.Equal(t, "5s ago", relativeTime(now.Add(-5*time.Second), now))
	require.Equal(t, "in 5s", relativeTime(now.Add(5*time.Second), now))
}

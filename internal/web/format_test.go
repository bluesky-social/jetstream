package web

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHumanBytes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   int64
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.00 KiB"},
		{1536, "1.50 KiB"},
		{1024 * 1024, "1.00 MiB"},
		{int64(1.5 * 1024 * 1024 * 1024), "1.50 GiB"},
		{int64(2 * 1024 * 1024 * 1024 * 1024), "2.00 TiB"},
	}
	for _, c := range cases {
		require.Equal(t, c.want, humanBytes(c.in), "humanBytes(%d)", c.in)
	}
}

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

func TestHumanInt(t *testing.T) {
	t.Parallel()
	require.Equal(t, "0", humanInt(0))
	require.Equal(t, "999", humanInt(999))
	require.Equal(t, "1,000", humanInt(1000))
	require.Equal(t, "1,234,567", humanInt(1234567))
}

package obs_test

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/obs"
	"github.com/stretchr/testify/require"
)

func TestParseLogLevel(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   string
		want slog.Level
	}{
		{"", slog.LevelInfo},
		{"INFO", slog.LevelInfo},
		{"debug", slog.LevelDebug},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"ERROR", slog.LevelError},
	}
	for _, tc := range cases {
		got, err := obs.ParseLogLevel(tc.in)
		require.NoError(t, err, "input=%q", tc.in)
		require.Equal(t, tc.want, got, "input=%q", tc.in)
	}

	_, err := obs.ParseLogLevel("loud")
	require.Error(t, err)
}

func TestParseLogFormat(t *testing.T) {
	t.Parallel()

	got, err := obs.ParseLogFormat("json")
	require.NoError(t, err)
	require.Equal(t, obs.LogFormatJSON, got)

	// Empty string maps to JSON to match the CLI flag default.
	got, err = obs.ParseLogFormat("")
	require.NoError(t, err)
	require.Equal(t, obs.LogFormatJSON, got)

	got, err = obs.ParseLogFormat("text")
	require.NoError(t, err)
	require.Equal(t, obs.LogFormatText, got)

	_, err = obs.ParseLogFormat("xml")
	require.Error(t, err)
}

func TestNewLogger_JSONFormat(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := obs.NewLogger(&buf, slog.LevelInfo, obs.LogFormatJSON)
	logger.Info("hello", "k", "v")

	out := buf.String()
	require.True(t, strings.HasPrefix(out, "{"), "json handler should emit objects, got %q", out)
	require.Contains(t, out, `"msg":"hello"`)
}

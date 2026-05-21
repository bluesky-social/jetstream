package livestream

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/stretchr/testify/require"
)

func TestConfig_Validate(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Ensure store is imported
	_ = (*store.Store)(nil)

	st := newTestStore(t)
	good := Config{
		SegmentsDir: t.TempDir(),
		Store:       st,
		SeqKey:      "live_segments/seq/next",
		CursorKey:   "relay/cursor",
		RelayURL:    "https://bsky.network",
		Logger:      logger,
	}

	t.Run("happy", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, good.validate())
	})

	cases := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"missing SegmentsDir", func(c *Config) { c.SegmentsDir = "" }, "SegmentsDir"},
		{"missing Store", func(c *Config) { c.Store = nil }, "Store"},
		{"missing SeqKey", func(c *Config) { c.SeqKey = "" }, "SeqKey"},
		{"missing CursorKey", func(c *Config) { c.CursorKey = "" }, "CursorKey"},
		{"missing RelayURL", func(c *Config) { c.RelayURL = "" }, "RelayURL"},
		{"missing Logger", func(c *Config) { c.Logger = nil }, "Logger"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := good
			c.Store = st // share, since *store.Store is fine across tests
			tc.mutate(&c)
			err := c.validate()
			require.Error(t, err)
			require.True(t, errors.Is(err, ErrInvalidConfig))
			require.Contains(t, err.Error(), tc.want)
		})
	}
}

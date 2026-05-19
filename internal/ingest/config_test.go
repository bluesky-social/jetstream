package ingest

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/stretchr/testify/require"
)

// TestConfigValidate_RequiresFields pins the validation contract for
// cmd/jetstream: Open errors out before any I/O if Config is missing
// required fields.
func TestConfigValidate_RequiresFields(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{"missing ShardsDir", Config{Store: &store.Store{}, Logger: logger}, "ShardsDir"},
		{"missing Store", Config{ShardsDir: "/tmp/x", Logger: logger}, "Store"},
		{"missing Logger", Config{ShardsDir: "/tmp/x", Store: &store.Store{}}, "Logger"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.validate()
			require.ErrorIs(t, err, ErrInvalidConfig)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

// TestConfigValidate_AppliesDefaults pins the documented defaults for
// MaxSegmentBytes and MaxEventsPerBlock.
func TestConfigValidate_AppliesDefaults(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := Config{ShardsDir: "/tmp/x", Store: &store.Store{}, Logger: logger}
	require.NoError(t, cfg.validate())

	cfg.applyDefaults()
	require.Equal(t, int64(256<<20), cfg.MaxSegmentBytes)
	require.Equal(t, defaultMaxEventsPerBlock, cfg.MaxEventsPerBlock)
}

// TestConfigValidate_RejectsNegativeBytes guards against a footgun:
// MaxSegmentBytes < 0 is meaningless and would loop infinitely.
func TestConfigValidate_RejectsNegativeBytes(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := Config{
		ShardsDir:       "/tmp/x",
		Store:           &store.Store{},
		Logger:          logger,
		MaxSegmentBytes: -1,
	}
	err := cfg.validate()
	require.True(t, errors.Is(err, ErrInvalidConfig), "want ErrInvalidConfig, got %v", err)
}

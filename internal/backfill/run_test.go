package backfill

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/stretchr/testify/require"
)

// TestRun_RejectsInvalidConfig pins the contract for cmd/jetstream:
// pass the wrong Config and you get a clear error before any network
// I/O happens.
func TestRun_RejectsInvalidConfig(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tests := []struct {
		name    string
		cfg     Config
		errPart string
	}{
		{"missing Store", Config{RelayURL: "x", Logger: logger}, "Config.Store"},
		{"missing RelayURL", Config{Store: &store.Store{}, Logger: logger}, "Config.RelayURL"},
		{"missing Logger", Config{Store: &store.Store{}, RelayURL: "x"}, "Config.Logger"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Run(context.Background(), tc.cfg)
			require.ErrorContains(t, err, tc.errPart)
		})
	}
}

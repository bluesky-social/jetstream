package world

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPDSAssignmentIsDeterministicSkewedAndComplete(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Reset = true
	cfg.Seed = 918273
	cfg.Accounts = 1000
	cfg.PDSHosts = 4
	cfg.InitialRecordsMin = 0
	cfg.InitialRecordsMax = 0
	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close()) })
	wantBootstrap, err := w.EnsureSeed()
	require.NoError(t, err)
	require.True(t, wantBootstrap)
	require.NoError(t, w.Bootstrap(context.Background(), slog.Default()))

	total := 0
	for i := range cfg.PDSHosts {
		count := w.PDSAccountCount(i)
		require.Positive(t, count, "every configured PDS must own an account")
		total += count
	}
	require.Equal(t, cfg.Accounts, total)
	require.Greater(t, w.PDSAccountCount(0), cfg.Accounts/2, "host zero must model the big mushroom")
	for i := range cfg.PDSHosts {
		require.Equal(t, i, w.PDSIndexForAccount(i), "the first accounts pin topology coverage")
	}
}

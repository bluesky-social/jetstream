package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestApp_Help(t *testing.T) {
	t.Parallel()
	cmd := newApp()
	require.NoError(t, cmd.Run(context.Background(), []string{"simulator", "--help"}))
}

func TestServe_StartsAndStops(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a real listener; run with -count=1 in long mode")
	}
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "simulator")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cmd := newApp()
	err := cmd.Run(ctx, []string{
		"simulator", "serve",
		"--addr=127.0.0.1:0",
		"--data-dir", dir,
		"--accounts=5",
		"--initial-records-per-account=1",
		"--commits-per-sec=100",
	})
	// runServe's errgroup returns nil on context-canceled (ctx deadline → graceful shutdown).
	require.NoError(t, err)
}

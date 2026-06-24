package main

import (
	"os"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTuneGC verifies the GC-tuning policy: apply the requested percent only
// when the operator has not pinned GOGC, and never on a non-positive value.
// SetGCPercent returns the previous setting, which we use both to observe the
// effect and to restore the process default. These cases mutate process-global
// GC state and the GOGC env var, so the test is intentionally not parallel.
//
//nolint:paralleltest // intentionally serial: mutates process-global GC state + GOGC
func TestTuneGC(t *testing.T) {
	orig := debug.SetGCPercent(100)
	t.Cleanup(func() { debug.SetGCPercent(orig) })

	// Snapshot GOGC and restore it after the test so we can freely set/unset it.
	if v, ok := os.LookupEnv("GOGC"); ok {
		t.Cleanup(func() { _ = os.Setenv("GOGC", v) })
	} else {
		t.Cleanup(func() { _ = os.Unsetenv("GOGC") })
	}

	t.Run("applies percent when GOGC unset", func(t *testing.T) {
		require.NoError(t, os.Unsetenv("GOGC"))
		debug.SetGCPercent(100)
		tuneGC(400)
		// SetGCPercent returns the value currently installed (what tuneGC set).
		require.Equal(t, 400, debug.SetGCPercent(100), "tuneGC should set the GOGC target to 400")
	})

	t.Run("respects explicit GOGC env (no override)", func(t *testing.T) {
		require.NoError(t, os.Setenv("GOGC", "150"))
		debug.SetGCPercent(100)
		tuneGC(400)
		require.Equal(t, 100, debug.SetGCPercent(100), "tuneGC must not override an operator-set GOGC")
	})

	t.Run("non-positive percent is a no-op", func(t *testing.T) {
		require.NoError(t, os.Unsetenv("GOGC"))
		debug.SetGCPercent(123)
		tuneGC(0)
		require.Equal(t, 123, debug.SetGCPercent(123), "tuneGC(0) must leave the GC target untouched")
		tuneGC(-1)
		require.Equal(t, 123, debug.SetGCPercent(100), "tuneGC(-1) must leave the GC target untouched")
	})
}

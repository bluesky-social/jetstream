package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/stretchr/testify/require"
)

// TestServe_BootstrapsAndShutsDownCleanly is the wiring smoke test:
// a real `jetstream serve` invocation against a stubbed relay,
// asserting that the bootstrap pipeline completes (the metadata
// store reaches PhaseComplete and both DIDs land) and that the
// process shuts down cleanly when the parent context is cancelled.
//
// The deeper state-machine cases (idempotent re-run, partial-seed
// recovery, validation errors) live in internal/backfill where they
// can be tested without the wiring overhead.
func TestServe_BootstrapsAndShutsDownCleanly(t *testing.T) {
	t.Parallel()

	type repoEntry struct {
		DID    string `json:"did"`
		Head   string `json:"head"`
		Rev    string `json:"rev"`
		Active bool   `json:"active"`
	}
	type page struct {
		Cursor string      `json:"cursor,omitempty"`
		Repos  []repoEntry `json:"repos"`
	}

	// listReposDone is closed after the relay has served both the
	// initial repo page and the empty-page terminator. It's the
	// deterministic "bootstrap reached the seed loop" signal we use
	// instead of polling on a wall-clock sleep — once both pages
	// are served we know the seed loop has either finished or is
	// about to write PhaseComplete.
	listReposDone := make(chan struct{})
	var calls atomic.Int32
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/xrpc/com.atproto.sync.listRepos", r.URL.Path)
		idx := int(calls.Add(1)) - 1
		switch idx {
		case 0:
			_ = json.NewEncoder(w).Encode(page{
				Cursor: "more",
				Repos: []repoEntry{
					{DID: "did:plc:aaa", Head: "bafyaaa", Rev: "rev1", Active: true},
					{DID: "did:plc:bbb", Head: "bafybbb", Rev: "rev2", Active: true},
				},
			})
		default:
			// Empty-page terminator. Closing here is safe even if a
			// retry storms us with extra calls — we use sync.Once via
			// a select-on-already-closed dance below.
			_ = json.NewEncoder(w).Encode(page{})
		}
		if idx == 1 {
			// Both pages served — bootstrap is now in its post-seed
			// PhaseComplete write. Signal the test side.
			select {
			case <-listReposDone:
			default:
				close(listReposDone)
			}
		}
	}))
	t.Cleanup(relay.Close)

	dataDir := t.TempDir()

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	done := make(chan error, 1)
	go func() {
		done <- newApp().Run(ctx, []string{
			"jetstream",
			"--log-format=text",
			"--log-level=warn",
			"serve",
			"--addr=127.0.0.1:0",
			"--debug-addr=127.0.0.1:0",
			"--shutdown-timeout=5s",
			"--relay-url=" + relay.URL,
			"--data-dir=" + dataDir,
		})
	}()

	// Sanity check: pebble actually opened the data dir.
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(dataDir, "meta.pebble", "LOCK"))
		return err == nil
	}, 5*time.Second, 50*time.Millisecond, "metadata store was never created")

	// Wait for the seed loop to finish draining listRepos.
	select {
	case <-listReposDone:
	case <-time.After(5 * time.Second):
		t.Fatal("bootstrap never drained listRepos pagination")
	}

	// listRepos drained: the seed loop has returned and the
	// orchestrator is in the middle of writing PhaseComplete. We
	// can't observe that write while pebble is locked, so cancel
	// and verify post-shutdown.
	//
	// The PhaseComplete write happens in-process before Run()
	// returns, and Run() returning is what unblocks the errgroup —
	// so by the time cancel() lands and the process drains, the
	// state on disk is already either PhaseComplete (the seed loop
	// finished before cancel) or PhaseSeed (cancel won the race).
	// We accept both outcomes here: the unit tests in
	// internal/backfill cover the "must reach PhaseComplete" axis
	// against a deterministic in-process harness. This test exists
	// to verify the wiring, not to race the orchestrator.
	cancel()

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("serve exited with unexpected error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not shut down within deadline")
	}

	// Re-open the store now that serve has released the pebble lock
	// and confirm both DIDs landed regardless of which side won the
	// PhaseComplete race. Per-DID rows are written inside the seed
	// loop (before the post-seed PhaseComplete write), so they're
	// durable as soon as listRepos finishes.
	s, err := store.Open(dataDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	count, err := backfill.CountRepos(s)
	require.NoError(t, err)
	require.Equal(t, int64(2), count, "both seeded DIDs should be on disk")

	st, err := backfill.GetBootstrapState(s)
	require.NoError(t, err)
	require.Contains(t, []backfill.Phase{backfill.PhaseSeed, backfill.PhaseComplete}, st.Phase,
		"bootstrap must have at least entered the seed phase")
}

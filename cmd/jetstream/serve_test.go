package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/coder/websocket"
	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/mst"
	atmosrepo "github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/require"
)

// TestServe_BootstrapsAndShutsDownCleanly is the wiring smoke test:
// a real `jetstream serve` invocation against a stubbed relay that
// returns two DIDs. We pre-seed the metadata pebble with Complete
// rows for both, so the engine walks listRepos, skips download via
// Lookup, and drains immediately — proving the serve→backfill→Store
// wiring composes without exercising the network-dependent download
// path. The deeper integration coverage lives in
// internal/backfill/run_test.go.
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

	dataDir := t.TempDir()
	dids := []atmos.DID{"did:plc:aaa", "did:plc:bbb"}

	// Pre-seed the metadata db with both DIDs at Complete so the
	// engine's listRepos scan skips download entirely.
	require.NoError(t, preSeedComplete(dataDir, dids))

	// listReposDone is closed once the relay has served the empty-
	// page terminator. That's our deterministic "bootstrap walked
	// listRepos to the end" signal.
	listReposDone := make(chan struct{})
	var calls atomic.Int32
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Handle websocket upgrade for subscribeRepos (livestream consumer)
		if strings.HasSuffix(r.URL.Path, "/com.atproto.sync.subscribeRepos") {
			conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
			if err != nil {
				return
			}
			defer func() { _ = conn.CloseNow() }()
			<-r.Context().Done()
			return
		}
		require.Equal(t, "/xrpc/com.atproto.sync.listRepos", r.URL.Path)
		idx := int(calls.Add(1)) - 1
		switch idx {
		case 0:
			_ = json.NewEncoder(w).Encode(page{
				Cursor: "more",
				Repos: []repoEntry{
					{DID: string(dids[0]), Head: "bafyaaa", Rev: "rev1", Active: true},
					{DID: string(dids[1]), Head: "bafybbb", Rev: "rev2", Active: true},
				},
			})
		default:
			_ = json.NewEncoder(w).Encode(page{})
		}
		if idx == 1 {
			select {
			case <-listReposDone:
			default:
				close(listReposDone)
			}
		}
	}))
	t.Cleanup(relay.Close)

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

	// Wait for the bootstrap to actually walk the relay's listRepos
	// pagination. listReposDone is the deterministic signal that
	// serve has fully wired itself up: HTTP server started, store
	// opened, backfill goroutine launched, listRepos request reached
	// our test relay. The previous "stat meta.pebble/LOCK" check was
	// a no-op because preSeedComplete created that file before serve
	// even ran.
	select {
	case <-listReposDone:
	case <-time.After(5 * time.Second):
		// If serve died early, surface that error rather than
		// timing out with a generic message.
		select {
		case err := <-done:
			t.Fatalf("serve exited before draining listRepos: %v", err)
		default:
			t.Fatal("bootstrap never drained listRepos pagination")
		}
	}

	cancel()

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("serve exited with unexpected error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not shut down within deadline")
	}

	// Re-open and confirm both DIDs are still at Complete.
	s, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	bf := backfill.NewStore(s, nil)
	for _, did := range dids {
		got, err := bf.Lookup(context.Background(), did)
		require.NoError(t, err)
		require.Equal(t, atmosbackfill.StateComplete, got.State, "%s should be Complete", did)
	}
}

// preSeedComplete opens the data dir's pebble, writes a Complete row
// for each DID, and closes. Used by the smoke test to bypass the
// actual download path while still exercising the rest of the
// wiring.
//
// The CAR build per DID isn't strictly necessary — we only use
// commit.Rev — but we go through the real fixture-construction path
// so this helper documents what a "real" handler would have produced
// and so that future tests can reuse it.
func preSeedComplete(dataDir string, dids []atmos.DID) error {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}
	s, err := store.Open(dataDir, nil)
	if err != nil {
		return err
	}
	defer func() { _ = s.Close() }()

	bf := backfill.NewStore(s, nil)
	for _, did := range dids {
		key, err := crypto.GenerateP256()
		if err != nil {
			return err
		}
		mstore := mst.NewMemBlockStore()
		r := &atmosrepo.Repo{
			DID:   did,
			Clock: atmos.NewTIDClock(0),
			Store: mstore,
			Tree:  mst.NewTree(mstore),
		}
		if err := r.Create("app.bsky.feed.post", "rec0", map[string]any{"text": "x"}); err != nil {
			return err
		}
		var buf bytes.Buffer
		if err := r.ExportCAR(&buf, key); err != nil {
			return err
		}
		if err := bf.OnDiscover(context.Background(), atmossync.ListReposEntry{DID: did, Active: true}); err != nil {
			return err
		}
		if err := bf.OnComplete(context.Background(), did, &atmosrepo.Commit{DID: string(did), Rev: "rev-pre"}); err != nil {
			return err
		}
	}
	return nil
}

// TestServe_StartsInSteadyStatePhase pins the steady-state startup
// path: a data dir already at PhaseSteadyState skips bootstrap and
// merge, runs the steady-state consumer, and shuts down cleanly on
// ctx cancel.
func TestServe_StartsInSteadyStatePhase(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()

	// Pre-populate phase=steady_state.
	{
		s, err := store.Open(dataDir, nil)
		require.NoError(t, err)
		require.NoError(t, lifecycle.WritePhase(s, lifecycle.PhaseSteadyState))
		require.NoError(t, s.Close())
	}

	// subscribeReposHit is closed the first time the steady-state
	// live consumer dials the relay. That's our deterministic
	// "serve made it to phase=steady_state work" signal.
	subscribeReposHit := make(chan struct{})
	var hitOnce sync.Once
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/com.atproto.sync.subscribeRepos") {
			hitOnce.Do(func() { close(subscribeReposHit) })
			conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
			if err != nil {
				return
			}
			defer func() { _ = conn.CloseNow() }()
			<-r.Context().Done()
			return
		}
	}))
	t.Cleanup(relay.Close)

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

	select {
	case <-subscribeReposHit:
	case err := <-done:
		t.Fatalf("serve exited before reaching the relay: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("steady-state consumer never reached the relay")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("serve exited with unexpected error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not shut down")
	}

	// Phase should still be steady_state.
	s, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	p, err := lifecycle.ReadPhase(s)
	require.NoError(t, err)
	require.Equal(t, lifecycle.PhaseSteadyState, p)
}

// TestServe_AdvancesFromMergingToSteadyState pins the crash-recovery
// path: a data dir already at PhaseMerging runs the merge stub
// (no-op for now), writes phase=steady_state, and starts the
// steady-state consumer.
func TestServe_AdvancesFromMergingToSteadyState(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	{
		s, err := store.Open(dataDir, nil)
		require.NoError(t, err)
		require.NoError(t, lifecycle.WritePhase(s, lifecycle.PhaseMerging))
		require.NoError(t, s.Close())
	}

	subscribeReposHit := make(chan struct{})
	var hitOnce sync.Once
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/com.atproto.sync.subscribeRepos") {
			hitOnce.Do(func() { close(subscribeReposHit) })
			conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
			if err != nil {
				return
			}
			defer func() { _ = conn.CloseNow() }()
			<-r.Context().Done()
			return
		}
	}))
	t.Cleanup(relay.Close)

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

	select {
	case <-subscribeReposHit:
	case err := <-done:
		t.Fatalf("serve exited before reaching the relay: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("orchestrator never advanced merging->steady_state")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("serve exited with unexpected error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not shut down")
	}

	s, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	p, err := lifecycle.ReadPhase(s)
	require.NoError(t, err)
	require.Equal(t, lifecycle.PhaseSteadyState, p)
}

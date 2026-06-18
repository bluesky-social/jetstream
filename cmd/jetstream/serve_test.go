package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/internal/xrpcapi"
	"github.com/coder/websocket"
	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/mst"
	atmosrepo "github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

// nolint:paralleltest
func TestServeOptionsFromCLI_Defaults(t *testing.T) {
	// Intentionally NOT parallel: this asserts the CLI's built-in
	// defaults, which urfave/cli silently overrides from the $JETSTREAM_*
	// (and $OTEL_*) env sources declared on each flag. A developer or CI
	// shell that exports any of those (e.g. JETSTREAM_DATA_DIR) would
	// otherwise break this test. We clear the process environment for the
	// duration so "default" means default. Non-parallel tests run to
	// completion before any t.Parallel test resumes, so mutating global
	// env here is safe.
	withClearedEnv(t)

	app := newApp()
	var opts jetstreamd.Options
	for _, cmd := range app.Commands {
		if cmd.Name != "serve" {
			continue
		}
		cmd.Action = func(_ context.Context, cmd *cli.Command) error {
			var err error
			opts, err = serveOptionsFromCommand(cmd)
			return err
		}
		break
	}

	require.NoError(t, app.Run(t.Context(), []string{"jetstream", "serve"}))
	require.Equal(t, ":8080", opts.PublicAddr)
	require.Equal(t, ":6060", opts.DebugAddr)
	require.Equal(t, "./data", opts.DataDir)
	require.Equal(t, "https://bsky.network", opts.RelayURL)
	require.Equal(t, "", opts.PLCURL)
	require.Equal(t, "jetstream", opts.OTelServiceName)
	require.Equal(t, "info", opts.LogLevel)
	require.Equal(t, "json", opts.LogFormat)
	require.Same(t, os.Stderr, opts.LogOutput)
	require.Equal(t, 30*time.Second, opts.ShutdownTimeout)
	require.Equal(t, 10*time.Second, opts.ClientDrainTimeout)
	require.Equal(t, 0, opts.MaxBackfillRepos)
	require.Equal(t, 100, opts.BackfillWorkers)
	require.Equal(t, 100_000, opts.BackfillBatchSize)
	require.Equal(t, 4, opts.BackfillAsyncFlushWorkers)
	require.Empty(t, opts.BackfillRepos)
	require.False(t, opts.SkipMergeDiscovery)
	require.False(t, opts.DisableRepoActionRateLimits)
	require.Equal(t, 36*time.Hour, opts.CursorLookback)
	require.Equal(t, 0*time.Second, opts.SegmentCacheMaxAge)
	require.Equal(t, xrpcapi.DefaultPlanMaxDIDs, opts.PlanMaxDIDs)
	require.Equal(t, xrpcapi.DefaultPlanMaxCollections, opts.PlanMaxCollections)
	require.Equal(t, xrpcapi.DefaultPlanMaxEntries, opts.PlanMaxEntries)
	require.Equal(t, xrpcapi.DefaultPlanWholeSegmentThreshold, opts.PlanWholeSegmentThreshold)
	require.Equal(t, 256<<20, opts.SubscribeHotTailBytes)
	require.Equal(t, 64<<20, opts.SubscribeBlockCacheBytes)
	require.Equal(t, 1024, opts.SubscribeReadBatch)
	require.Equal(t, 60*time.Second, opts.SubscribeSlowWindow)
	require.Equal(t, float64(5), opts.SubscribeSlowMinRate)
	require.Equal(t, 32, opts.CursorBlockIndexCacheSize)
	require.Equal(t, 0, opts.CompactionRewriteWorkers)
	require.Nil(t, opts.BarrierAfterBootstrap)
	require.Nil(t, opts.BarrierAfterMerge)
	require.Nil(t, opts.OnSteadyStateEvent)
}

// withClearedEnv unsets every environment variable the serve flags bind
// to, restoring the prior values via t.Cleanup. urfave/cli treats those
// vars as flag sources, so a default-value assertion is only meaningful
// in an environment where none of them are set. The caller must NOT be
// parallel: this mutates global process state.
func withClearedEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"JETSTREAM_ADDR",
		"JETSTREAM_DEBUG_ADDR",
		"JETSTREAM_DATA_DIR",
		"JETSTREAM_RELAY_URL",
		"JETSTREAM_PLC_URL",
		"OTEL_SERVICE_NAME",
		"JETSTREAM_LOG_LEVEL",
		"JETSTREAM_LOG_FORMAT",
		"JETSTREAM_SHUTDOWN_TIMEOUT",
		"JETSTREAM_CLIENT_DRAIN_TIMEOUT",
		"JETSTREAM_MAX_BACKFILL_REPOS",
		"JETSTREAM_BACKFILL_WORKERS",
		"JETSTREAM_BACKFILL_BATCH_SIZE",
		"JETSTREAM_BACKFILL_ASYNC_FLUSH_WORKERS",
		"JETSTREAM_BACKFILL_REPOS",
		"JETSTREAM_SKIP_MERGE_DISCOVERY",
		"JETSTREAM_DISABLE_REPO_ACTION_RATE_LIMITS",
		"JETSTREAM_CURSOR_LOOKBACK",
		"JETSTREAM_SEGMENT_CACHE_MAX_AGE",
		"JETSTREAM_PLAN_MAX_DIDS",
		"JETSTREAM_PLAN_MAX_COLLECTIONS",
		"JETSTREAM_PLAN_MAX_ENTRIES",
		"JETSTREAM_PLAN_WHOLE_SEGMENT_THRESHOLD",
		"JETSTREAM_SUBSCRIBE_HOT_TAIL_BYTES",
		"JETSTREAM_SUBSCRIBE_BLOCK_CACHE_BYTES",
		"JETSTREAM_SUBSCRIBE_READ_BATCH",
		"JETSTREAM_SUBSCRIBE_SLOW_WINDOW",
		"JETSTREAM_SUBSCRIBE_SLOW_MIN_RATE",
		"JETSTREAM_CURSOR_BLOCK_INDEX_CACHE_SIZE",
		"JETSTREAM_COMPACTION_INTERVAL",
		"JETSTREAM_COMPACTION_TOMBSTONE_CAP",
		"JETSTREAM_COMPACTION_REWRITE_WORKERS",
	} {
		if prev, ok := os.LookupEnv(key); ok {
			require.NoError(t, os.Unsetenv(key))
			t.Cleanup(func() { _ = os.Setenv(key, prev) })
		}
	}
}

func TestServeOptionsFromCLI_Overrides(t *testing.T) {
	t.Parallel()

	app := newApp()
	var opts jetstreamd.Options
	for _, cmd := range app.Commands {
		if cmd.Name != "serve" {
			continue
		}
		cmd.Action = func(_ context.Context, cmd *cli.Command) error {
			var err error
			opts, err = serveOptionsFromCommand(cmd)
			return err
		}
		break
	}

	require.NoError(t, app.Run(t.Context(), []string{
		"jetstream",
		"--log-level=debug",
		"--log-format=text",
		"serve",
		"--addr=127.0.0.1:18080",
		"--debug-addr=127.0.0.1:16060",
		"--data-dir=/tmp/jetstream-override-data",
		"--relay-url=https://relay.example.com",
		"--plc-url=https://plc.example.com",
		"--otel-service-name=jetstream-test",
		"--shutdown-timeout=45s",
		"--client-drain-timeout=11s",
		"--backfill-workers=17",
		"--backfill-batch-size=12345",
		"--backfill-async-flush-workers=4",
		"--backfill-repos=did:plc:aaa, did:plc:bbb",
		"--skip-merge-discovery",
		"--disable-repo-action-rate-limits",
		"--cursor-lookback=7h",
		"--segment-cache-max-age=13s",
		"--plan-max-dids=8",
		"--plan-max-collections=4",
		"--plan-max-entries=123",
		"--plan-whole-segment-threshold=0.6",
		"--subscribe-hot-tail-bytes=123456",
		"--subscribe-block-cache-bytes=654321",
		"--subscribe-read-batch=77",
		"--subscribe-slow-window=22s",
		"--subscribe-slow-min-rate=9.5",
		"--cursor-block-index-cache-size=99",
		"--compaction-rewrite-workers=3",
	}))
	require.Equal(t, "127.0.0.1:18080", opts.PublicAddr)
	require.Equal(t, "127.0.0.1:16060", opts.DebugAddr)
	require.Equal(t, "/tmp/jetstream-override-data", opts.DataDir)
	require.Equal(t, "https://relay.example.com", opts.RelayURL)
	require.Equal(t, "https://plc.example.com", opts.PLCURL)
	require.Equal(t, "jetstream-test", opts.OTelServiceName)
	require.Equal(t, "debug", opts.LogLevel)
	require.Equal(t, "text", opts.LogFormat)
	require.Same(t, os.Stderr, opts.LogOutput)
	require.Equal(t, 45*time.Second, opts.ShutdownTimeout)
	require.Equal(t, 11*time.Second, opts.ClientDrainTimeout)
	require.Equal(t, 0, opts.MaxBackfillRepos)
	require.Equal(t, 17, opts.BackfillWorkers)
	require.Equal(t, 12345, opts.BackfillBatchSize)
	require.Equal(t, 4, opts.BackfillAsyncFlushWorkers)
	require.Equal(t, []atmos.DID{"did:plc:aaa", "did:plc:bbb"}, opts.BackfillRepos)
	require.True(t, opts.SkipMergeDiscovery)
	require.True(t, opts.DisableRepoActionRateLimits)
	require.Equal(t, 7*time.Hour, opts.CursorLookback)
	require.Equal(t, 13*time.Second, opts.SegmentCacheMaxAge)
	require.Equal(t, 8, opts.PlanMaxDIDs)
	require.Equal(t, 4, opts.PlanMaxCollections)
	require.Equal(t, 123, opts.PlanMaxEntries)
	require.Equal(t, 0.6, opts.PlanWholeSegmentThreshold)
	require.Equal(t, 123456, opts.SubscribeHotTailBytes)
	require.Equal(t, 654321, opts.SubscribeBlockCacheBytes)
	require.Equal(t, 77, opts.SubscribeReadBatch)
	require.Equal(t, 22*time.Second, opts.SubscribeSlowWindow)
	require.Equal(t, 9.5, opts.SubscribeSlowMinRate)
	require.Equal(t, 99, opts.CursorBlockIndexCacheSize)
	require.Equal(t, 3, opts.CompactionRewriteWorkers)
	require.Nil(t, opts.BarrierAfterBootstrap)
	require.Nil(t, opts.BarrierAfterMerge)
	require.Nil(t, opts.OnSteadyStateEvent)
}

func TestServeOptionsFromCLI_BackfillSchedulerEnv(t *testing.T) {
	withClearedEnv(t)

	app := newApp()
	var opts jetstreamd.Options
	for _, cmd := range app.Commands {
		if cmd.Name != "serve" {
			continue
		}
		cmd.Action = func(_ context.Context, cmd *cli.Command) error {
			var err error
			opts, err = serveOptionsFromCommand(cmd)
			return err
		}
		break
	}

	t.Setenv("JETSTREAM_BACKFILL_WORKERS", "33")
	t.Setenv("JETSTREAM_BACKFILL_BATCH_SIZE", "76543")
	t.Setenv("JETSTREAM_BACKFILL_ASYNC_FLUSH_WORKERS", "5")

	require.NoError(t, app.Run(t.Context(), []string{"jetstream", "serve"}))
	require.Equal(t, 33, opts.BackfillWorkers)
	require.Equal(t, 76543, opts.BackfillBatchSize)
	require.Equal(t, 5, opts.BackfillAsyncFlushWorkers)
}

func TestServeOptionsFromCLI_DisableRepoActionRateLimitsEnv(t *testing.T) {
	withClearedEnv(t)

	app := newApp()
	var opts jetstreamd.Options
	for _, cmd := range app.Commands {
		if cmd.Name != "serve" {
			continue
		}
		cmd.Action = func(_ context.Context, cmd *cli.Command) error {
			var err error
			opts, err = serveOptionsFromCommand(cmd)
			return err
		}
		break
	}

	t.Setenv("JETSTREAM_DISABLE_REPO_ACTION_RATE_LIMITS", "true")

	require.NoError(t, app.Run(t.Context(), []string{"jetstream", "serve"}))
	require.True(t, opts.DisableRepoActionRateLimits)
}

func TestServeOptionsFromCLI_RejectsBackfillReposWithMaxBackfillRepos(t *testing.T) {
	t.Parallel()

	err := newApp().Run(t.Context(), []string{
		"jetstream", "serve",
		"--max-backfill-repos=1",
		"--backfill-repos=did:plc:aaa",
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "--backfill-repos cannot be combined with --max-backfill-repos")
}

func TestServeOptionsFromCLI_MaxBackfillReposOverride(t *testing.T) {
	t.Parallel()

	app := newApp()
	var opts jetstreamd.Options
	for _, cmd := range app.Commands {
		if cmd.Name != "serve" {
			continue
		}
		cmd.Action = func(_ context.Context, cmd *cli.Command) error {
			var err error
			opts, err = serveOptionsFromCommand(cmd)
			return err
		}
		break
	}

	require.NoError(t, app.Run(t.Context(), []string{
		"jetstream", "serve",
		"--max-backfill-repos=17",
	}))
	require.Equal(t, 17, opts.MaxBackfillRepos)
	require.Empty(t, opts.BackfillRepos)
}

func TestServeOptionsFromCLI_RejectsDuplicateBackfillRepos(t *testing.T) {
	t.Parallel()

	err := newApp().Run(t.Context(), []string{
		"jetstream", "serve",
		"--backfill-repos=did:plc:aaa,did:plc:aaa",
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "duplicate DID")
}

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
		require.NoError(t, lifecycle.WritePhase(s, lifecycle.PhaseSteadyState, time.Now().UTC()))
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
		require.NoError(t, lifecycle.WritePhase(s, lifecycle.PhaseMerging, time.Now().UTC()))
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

func TestServe_StatusEndpoint(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	// Minimal relay stub — listRepos returns an empty page so backfill
	// drains immediately, and subscribeRepos blocks until cancellation.
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/com.atproto.sync.subscribeRepos") {
			conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
			if err != nil {
				return
			}
			defer func() { _ = conn.CloseNow() }()
			<-r.Context().Done()
			return
		}
		// All other requests (listRepos): empty page → drains immediately.
		_ = json.NewEncoder(w).Encode(struct {
			Cursor string `json:"cursor,omitempty"`
			Repos  []any  `json:"repos"`
		}{})
	}))
	t.Cleanup(relay.Close)

	rt, err := jetstreamd.Build(ctx, jetstreamd.Options{
		PublicAddr:                "127.0.0.1:0",
		DebugAddr:                 "127.0.0.1:0",
		DataDir:                   dataDir,
		RelayURL:                  relay.URL,
		OTelServiceName:           "jetstream-test",
		LogLevel:                  "warn",
		LogFormat:                 "text",
		LogOutput:                 io.Discard,
		ShutdownTimeout:           5 * time.Second,
		ClientDrainTimeout:        10 * time.Second,
		CursorLookback:            36 * time.Hour,
		PlanMaxDIDs:               xrpcapi.DefaultPlanMaxDIDs,
		PlanMaxCollections:        xrpcapi.DefaultPlanMaxCollections,
		PlanMaxEntries:            xrpcapi.DefaultPlanMaxEntries,
		PlanWholeSegmentThreshold: xrpcapi.DefaultPlanWholeSegmentThreshold,
		SubscribeHotTailBytes:     1 << 20,
		SubscribeBlockCacheBytes:  1 << 20,
		SubscribeReadBatch:        128,
		SubscribeSlowWindow:       time.Second,
		SubscribeSlowMinRate:      5,
		CursorBlockIndexCacheSize: 32,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		require.NoError(t, rt.Close(closeCtx))
	})

	done := make(chan error, 1)
	go func() {
		done <- rt.Run(ctx)
	}()

	// Poll /status until it answers 200 or we time out. The endpoint
	// is mounted before listenerless work starts, so a couple hundred
	// ms is plenty even on a slow CI box.
	publicAddr := waitRuntimePublicAddr(t, rt, done)
	url := "http://" + publicAddr + "/status"
	deadline := time.Now().Add(5 * time.Second)
	var resp *http.Response
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		require.NoError(t, err)
		resp, err = client.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			break
		}
		if resp != nil {
			_ = resp.Body.Close()
			resp = nil
		}
		select {
		case err := <-done:
			t.Fatalf("serve exited before /status came up: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
	}
	require.NotNil(t, resp, "/status never returned 200")
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "text/html; charset=utf-8", resp.Header.Get("Content-Type"))
	require.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
	require.NotEmpty(t, resp.Header.Get("X-Status-Generated-At"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	bodyStr := string(body)
	require.Contains(t, bodyStr, "jetstream")
	require.Contains(t, bodyStr, "Phase")
	require.Contains(t, bodyStr, "Backfill")

	cancel()

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("serve exited with unexpected error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not shut down within deadline")
	}
}

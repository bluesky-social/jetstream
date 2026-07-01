package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/internal/xrpcapi"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

// TestImport_EndToEnd is the high-level timestamp-import e2e: a real jetstream
// runtime (built via jetstreamd.Build, same harness as the /status e2e) with a
// pre-seeded sealed segment on disk, driven through the bearer-gated XRPC
// endpoints over real HTTP. It proves the whole M6 wire path composes — auth,
// path confinement, the async job, durable status, and the actual segment
// rewrite — plus the important sad paths (401 disabled/wrong-token, 400
// path-escape). Deeper per-layer behavior lives in the package unit tests; this
// is the integration seam.
func TestImport_EndToEnd(t *testing.T) {
	t.Parallel()

	const (
		token       = "test-import-token"
		did         = "did:plc:alice"
		collection  = "app.bsky.feed.post"
		rkey        = "r1"
		importedISO = "2021-12-20T11:33:20Z"
		importedUS  = int64(1_640_000_000_000_000) // 2021-12-20T11:33:20Z in micros
	)

	dataDir := t.TempDir()

	// Seed a sealed segment BEFORE Build so the manifest scans it at startup
	// and the importer's selector can route the DID to it. Two versions of one
	// path, both with the sentinel-0 display column (unimported).
	seedSealedSegment(t, dataDir, did, collection, rkey)

	// Start already in steady-state so the import endpoints (which are not
	// behind the readiness gate, but the manifest must be warm) serve promptly
	// and backfill doesn't need a live relay.
	{
		s, err := store.Open(dataDir, nil)
		require.NoError(t, err)
		require.NoError(t, lifecycle.WritePhase(s, lifecycle.PhaseSteadyState, time.Now().UTC()))
		require.NoError(t, s.Close())
	}

	// Minimal relay stub: subscribeRepos blocks, everything else is an empty
	// listRepos page.
	relay := newIdleRelay(t)

	importDir := filepath.Join(dataDir, "imports")
	require.NoError(t, os.MkdirAll(importDir, 0o755))

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	rt, err := jetstreamd.Build(ctx, importE2EOptions(dataDir, relay.URL, token, importDir))
	require.NoError(t, err)
	t.Cleanup(func() {
		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		require.NoError(t, rt.Close(closeCtx))
	})

	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()

	addr := waitRuntimePublicAddr(t, rt, done)
	base := "http://" + addr
	client := &http.Client{Timeout: 3 * time.Second}

	// --- Sad path: disabled/wrong-token before the happy path proves auth is
	// enforced regardless of state. ---
	t.Run("no token is 401", func(t *testing.T) {
		resp := doImportPost(t, ctx, client, base, "", `{"path":"import.csv"}`)
		defer func() { _ = resp.Body.Close() }()
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})
	t.Run("wrong token is 401", func(t *testing.T) {
		resp := doImportPost(t, ctx, client, base, "nope", `{"path":"import.csv"}`)
		defer func() { _ = resp.Body.Close() }()
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})
	t.Run("path escape is 400", func(t *testing.T) {
		resp := doImportPost(t, ctx, client, base, token, `{"path":"../../etc/passwd"}`)
		defer func() { _ = resp.Body.Close() }()
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	// --- Happy path. ---
	// Stage the CSV inside the confinement dir.
	csv := "uri,timestamp,scope,cid\n" +
		"at://" + did + "/" + collection + "/" + rkey + "," + importedISO + ",all_versions,\n"
	require.NoError(t, os.WriteFile(filepath.Join(importDir, "import.csv"), []byte(csv), 0o644))

	// Submit returns a job id.
	var submit struct {
		Job string `json:"job"`
	}
	{
		resp := doImportPost(t, ctx, client, base, token, `{"path":"import.csv"}`)
		defer func() { _ = resp.Body.Close() }()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&submit))
		require.NotEmpty(t, submit.Job)
	}

	// Poll getImportStatus until the job reaches a terminal state.
	final := pollImportStatus(t, ctx, client, base, token, submit.Job)
	require.Equal(t, "complete", final.State, "job failed: %s", final.Error)
	require.EqualValues(t, 1, final.SegmentsPatched)
	require.EqualValues(t, 2, final.RowsMutated, "both versions of the path patched")

	// The segment on disk now carries the imported display timestamp.
	events := readSegmentEvents(t, filepath.Join(dataDir, "segments", ingest.SegmentFilename(0)))
	require.Len(t, events, 2)
	for _, ev := range events {
		require.EqualValues(t, importedUS, ev.IndexedAt, "display column patched")
		require.EqualValues(t, importedUS, ev.DisplayTimeUS())
		require.NotEqualValues(t, importedUS, ev.WitnessedAt, "witnessed clock untouched")
	}

	// A second submit for the same file is idempotent: it completes with zero
	// mutations (every row already at the target).
	var submit2 struct {
		Job string `json:"job"`
	}
	{
		resp := doImportPost(t, ctx, client, base, token, `{"path":"import.csv"}`)
		defer func() { _ = resp.Body.Close() }()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&submit2))
	}
	final2 := pollImportStatus(t, ctx, client, base, token, submit2.Job)
	require.Equal(t, "complete", final2.State)
	require.EqualValues(t, 0, final2.SegmentsPatched, "re-import is a no-op")
	require.EqualValues(t, 0, final2.RowsMutated)

	// Shut down cleanly: cancel and wait for Run to return BEFORE the cleanup
	// closes the runtime. Runtime.Close is only safe once Run has returned
	// (otherwise the store can close under the steady-state consumer's final
	// commit), so the drain here is load-bearing, not decoration.
	cancel()
	select {
	case err := <-done:
		// Run suppresses context.Canceled on a caller-driven shutdown, so any
		// error here is a real shutdown-path regression.
		require.NoError(t, err, "runtime Run returned an error on graceful shutdown")
	case <-time.After(10 * time.Second):
		t.Fatal("runtime did not shut down within deadline")
	}
}

// importStatusResp mirrors the getImportStatus output fields the test asserts.
type importStatusResp struct {
	Job             string `json:"job"`
	State           string `json:"state"`
	Error           string `json:"error"`
	SegmentsPatched int64  `json:"segmentsPatched"`
	RowsMutated     int64  `json:"rowsMutated"`
}

func doImportPost(t *testing.T, ctx context.Context, client *http.Client, base, token, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		base+"/xrpc/network.bsky.jetstream.importTimestamps", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	require.NoError(t, err)
	return resp
}

func pollImportStatus(t *testing.T, ctx context.Context, client *http.Client, base, token, job string) importStatusResp {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			base+"/xrpc/network.bsky.jetstream.getImportStatus?job="+job, nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := client.Do(req)
		require.NoError(t, err)
		var out importStatusResp
		decErr := json.NewDecoder(resp.Body).Decode(&out)
		_ = resp.Body.Close()
		require.NoError(t, decErr)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		if out.State == "complete" || out.State == "failed" {
			return out
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("import job did not reach a terminal state in time")
	return importStatusResp{}
}

// seedSealedSegment writes seg_0000000000.jss with two create/update versions
// of one path, display column at sentinel-0 (unimported).
func seedSealedSegment(t *testing.T, dataDir, did, collection, rkey string) {
	t.Helper()
	segDir := filepath.Join(dataDir, "segments")
	require.NoError(t, os.MkdirAll(segDir, 0o755))
	path := filepath.Join(segDir, ingest.SegmentFilename(0))
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 4096})
	require.NoError(t, err)
	events := []segment.Event{
		{Seq: 1, WitnessedAt: 1_700_000_000_000_000, Kind: segment.KindCreate, DID: did, Collection: collection, Rkey: rkey, Rev: "1", Payload: []byte{0xa0}},
		{Seq: 2, WitnessedAt: 1_700_000_001_000_000, Kind: segment.KindUpdate, DID: did, Collection: collection, Rkey: rkey, Rev: "2", Payload: []byte{0xa0}},
	}
	for _, ev := range events {
		_, err := w.Append(ev)
		require.NoError(t, err)
	}
	_, err = w.Seal()
	require.NoError(t, err)
}

func readSegmentEvents(t *testing.T, path string) []segment.Event {
	t.Helper()
	r, err := segment.Open(segment.ReaderConfig{Path: path})
	require.NoError(t, err)
	defer func() { _ = r.Close() }()
	var out []segment.Event
	for i := range r.Blocks() {
		evs, err := r.DecodeBlock(i)
		require.NoError(t, err)
		out = append(out, evs...)
	}
	return out
}

// newIdleRelay is the minimal relay stub used by the serve e2e tests:
// subscribeRepos accepts the websocket and blocks until the request context is
// done; every other path (listRepos) returns an empty page so backfill drains
// immediately.
func newIdleRelay(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if strings.HasSuffix(req.URL.Path, "/com.atproto.sync.subscribeRepos") {
			// AcceptOptions.InsecureSkipVerify bypasses the websocket ORIGIN
			// check (not TLS): the in-process test dial carries no Origin
			// header. This mirrors the relay stub in serve_test.go.
			conn, err := websocket.Accept(w, req, &websocket.AcceptOptions{InsecureSkipVerify: true})
			if err != nil {
				return
			}
			defer func() { _ = conn.CloseNow() }()
			<-req.Context().Done()
			return
		}
		_ = json.NewEncoder(w).Encode(struct {
			Cursor string `json:"cursor,omitempty"`
			Repos  []any  `json:"repos"`
		}{})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func importE2EOptions(dataDir, relayURL, token, importDir string) jetstreamd.Options {
	return jetstreamd.Options{
		PublicAddr:                "127.0.0.1:0",
		DebugAddr:                 "127.0.0.1:0",
		DataDir:                   dataDir,
		RelayURL:                  relayURL,
		OTelServiceName:           "jetstream-import-e2e",
		LogLevel:                  "warn",
		LogFormat:                 "text",
		LogOutput:                 io.Discard,
		ShutdownTimeout:           5 * time.Second,
		ClientDrainTimeout:        5 * time.Second,
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
		TimestampImportToken:      token,
		TimestampImportDir:        importDir,
	}
}

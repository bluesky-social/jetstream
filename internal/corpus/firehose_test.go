package corpus

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/live"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/coder/websocket"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/streaming"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/gt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// memConn feeds queued raw websocket frames in order, then blocks
// until Close. Mirrors the injection seam used by the live consumer's
// own dial tests.
type memConn struct {
	frames chan []byte
	closed chan struct{}
	once   sync.Once
}

func newMemConn(frames [][]byte) *memConn {
	c := &memConn{frames: make(chan []byte, len(frames)), closed: make(chan struct{})}
	for _, f := range frames {
		c.frames <- f
	}
	return c
}

func (c *memConn) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	select {
	case f := <-c.frames:
		return websocket.MessageBinary, f, nil
	case <-c.closed:
		return 0, nil, io.EOF
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
}

func (c *memConn) Close(websocket.StatusCode, string) error { c.closeOnce(); return nil }
func (c *memConn) CloseNow() error                          { c.closeOnce(); return nil }
func (c *memConn) SetReadLimit(int64)                       {}
func (c *memConn) closeOnce()                               { c.once.Do(func() { close(c.closed) }) }

// cannedResolver resolves DIDs from the DID documents captured with
// the corpus window. It never touches the network; a lookup for a DID
// outside the corpus is a test bug and fails loudly.
type cannedResolver struct {
	docs map[string][]byte
}

func (r *cannedResolver) ResolveDID(_ context.Context, did atmos.DID) (*identity.DIDDocument, error) {
	doc, ok := r.docs[string(did)]
	if !ok {
		return nil, fmt.Errorf("corpus: no captured DID document for %s", did)
	}
	return identity.ParseDIDDocument(doc)
}

func (r *cannedResolver) ResolveHandle(_ context.Context, handle atmos.Handle) (atmos.DID, error) {
	return "", fmt.Errorf("corpus: offline resolver got a handle lookup for %q", handle)
}

// newCorpusVerifier builds a Sync 1.1 verifier that runs real
// signature and inversion checks against the captured DID documents,
// entirely offline. PolicyError (instead of the production
// PolicyResync) removes the getRepo network fallback: any chain break
// or inversion failure surfaces as a verification failure instead of
// dialing out.
func newCorpusVerifier(t *testing.T, docs map[string][]byte, onFailure func(did atmos.DID, err error)) *atmossync.Verifier {
	t.Helper()
	opts := atmossync.VerifierOptions{
		Directory: &identity.Directory{
			Resolver:               &cannedResolver{docs: docs},
			SkipHandleVerification: true,
		},
		StateStore: atmossync.NewMemStateStore(),
		Policy:     gt.Some(atmossync.PolicyError),
	}
	if onFailure != nil {
		opts.OnVerificationFailure = gt.Some(onFailure)
	}
	v, err := atmossync.NewVerifier(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = v.Close() })
	return v
}

// runCorpusConsumer replays raw relay frames through the full
// production live path — atmos frame decode, Sync 1.1 verification,
// ConvertEvent, ingest.Writer, segment files — and returns the
// archived events, the live-delivered events (which, unlike the
// archived rows, still carry UpstreamRelayCursor for per-frame
// attribution), and the consumer metrics.
//
// wantEvents is the number of segment events the frames must produce;
// the helper fails if the consumer stalls before reaching it. Callers
// injecting corrupted frames pass the reduced count they expect.
func runCorpusConsumer(t *testing.T, frames [][]byte, docs map[string][]byte, wantEvents int, onVerifyFailure func(did atmos.DID, err error)) ([]segment.Event, []segment.Event, *live.Metrics, *ingest.DropMetrics) {
	t.Helper()

	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	dir := filepath.Join(t.TempDir(), "segments")
	reg := prometheus.NewRegistry()
	metrics := live.NewMetrics(reg)
	dropMetrics := ingest.NewDropMetrics(reg)

	var (
		mu       sync.Mutex
		liveSeen []segment.Event
	)
	var delivered atomic.Int64
	conn := newMemConn(frames)
	c, err := live.Open(live.Config{
		SegmentsDir: dir,
		Store:       st,
		SeqKey:      "live_segments/seq/next",
		CursorKey:   "relay/cursor",
		RelayURL:    "https://relay.invalid",
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:     metrics,
		DropMetrics: dropMetrics,
		Verifier:    newCorpusVerifier(t, docs, onVerifyFailure),
		OnEvent: func(ev *segment.Event) {
			mu.Lock()
			liveSeen = append(liveSeen, *ev)
			mu.Unlock()
			delivered.Add(1)
		},
		Dial: func(context.Context, string) (streaming.Conn, *http.Response, error) {
			return conn, nil, nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(ctx) }()

	require.Eventually(t, func() bool {
		return delivered.Load() >= int64(wantEvents)
	}, 20*time.Second, 5*time.Millisecond,
		"consumer stalled: delivered %d of %d expected events", delivered.Load(), wantEvents)

	// Settle briefly so an over-delivering pipeline (an event that
	// should have been dropped sneaking through late) is caught by the
	// exact-count assertion below rather than racing the cancel.
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-runErr:
		// Run surfaces the cancellation that ended it; anything else
		// is a genuine pipeline failure.
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("consumer Run did not return after cancel")
	}
	require.NoError(t, c.Close())
	require.EqualValues(t, wantEvents, delivered.Load(),
		"consumer delivered more events than the corpus predicts")

	mu.Lock()
	defer mu.Unlock()
	return readAllSegmentEvents(t, dir), liveSeen, metrics, dropMetrics
}

// requireNoLiveDrops asserts every (live, reason) drop series is zero:
// a clean corpus replay must not shed a single op or event at the
// ingest validation gate.
func requireNoLiveDrops(t *testing.T, dm *ingest.DropMetrics) {
	t.Helper()
	for _, reason := range []ingest.DropReason{
		ingest.DropReasonInvalidRev,
		ingest.DropReasonInvalidCollection,
		ingest.DropReasonInvalidRkey,
		ingest.DropReasonFieldTooLong,
		ingest.DropReasonMissingBlock,
	} {
		require.Zerof(t, testutil.ToFloat64(dm.Counter(ingest.DropSourceLive, reason)),
			"live drop reason %q fired on clean corpus", reason)
	}
}

// readAllSegmentEvents seals any active segment in dir and decodes
// every event from every segment file.
func readAllSegmentEvents(t *testing.T, dir string) []segment.Event {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "seg_*.jss"))
	require.NoError(t, err)
	require.NotEmpty(t, matches, "no segment files were written")

	var events []segment.Event
	for _, path := range matches {
		sw, err := segment.New(segment.Config{Path: path})
		if err == nil {
			_, err = sw.Seal()
			require.NoError(t, err, "seal %s", path)
		} else {
			require.ErrorIs(t, err, segment.ErrSegmentSealed, "open %s for sealing", path)
		}

		r, err := segment.Open(segment.ReaderConfig{Path: path})
		require.NoError(t, err, "open %s", path)
		for i := range r.Blocks() {
			block, err := r.DecodeBlock(i)
			require.NoError(t, err, "decode block %d of %s", i, path)
			events = append(events, block...)
		}
		require.NoError(t, r.Close())
	}
	return events
}

// matchKey builds the identity used to pair an archived event with its
// production Jetstream v1 counterpart: commits by
// (did,rev,collection,rkey,op), identity/account by (kind,did,seq).
// The capture tool guaranteed key uniqueness within the window.
func matchKey(m map[string]any) string {
	kind, _ := m["kind"].(string)
	did, _ := m["did"].(string)
	switch kind {
	case "commit":
		c, _ := m["commit"].(map[string]any)
		if c == nil {
			return ""
		}
		rev, _ := c["rev"].(string)
		op, _ := c["operation"].(string)
		coll, _ := c["collection"].(string)
		rkey, _ := c["rkey"].(string)
		return strings.Join([]string{"c", did, rev, coll, rkey, op}, "\x00")
	case "identity", "account":
		body, _ := m[kind].(map[string]any)
		if body == nil {
			return ""
		}
		seq, _ := body["seq"].(float64)
		return fmt.Sprintf("%s\x00%s\x00%.0f", kind, did, seq)
	default:
		return ""
	}
}

// stripLocalFields removes the fields that legitimately differ between
// production Jetstream v1 and this replay: time_us is each server's
// own witness timestamp, and cursor is v2's local seq (absent on v1).
func stripLocalFields(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		if k == "time_us" || k == "cursor" {
			continue
		}
		out[k] = v
	}
	return out
}

// TestCorpusFirehoseReplay replays the captured relay window through
// the complete production live-ingest path and requires the v1 wire
// output to be semantically identical to what production Jetstream v1
// served for the same events. Production Jetstream is a foreign
// implementation, so agreement here checks the whole atmos-based
// pipeline (frame decode, CAR/MST extraction, record CBOR handling,
// CID computation, JSON encoding) against independently produced
// truth.
func TestCorpusFirehoseReplay(t *testing.T) {
	t.Parallel()

	m := loadManifest(t)
	frames := loadFrames(t)
	require.Len(t, frames, m.Frames)
	expected := loadExpectedV1(t)
	require.Len(t, expected, m.V1Events)
	docs := loadDIDDocs(t)

	var verifyFailures atomic.Int64
	events, _, metrics, dropMetrics := runCorpusConsumer(t, frames, docs, m.V1Events,
		func(did atmos.DID, err error) {
			verifyFailures.Add(1)
			t.Errorf("verification failure for %s: %v", did, err)
		})

	// Anti-vacuity: every frame decoded, nothing dropped, every
	// commit signature actually verified.
	require.Zero(t, verifyFailures.Load())
	require.Zero(t, testutil.ToFloat64(metrics.DecodeErrors), "decode errors on clean corpus")
	require.Zero(t, testutil.ToFloat64(metrics.UnknownEvents), "unknown events on clean corpus")
	requireNoLiveDrops(t, dropMetrics)
	require.EqualValues(t, m.Frames, testutil.ToFloat64(metrics.EventsReceived))

	// Encode every archived event as v1 JSON and index by match key.
	got := make(map[string]map[string]any, len(events))
	kindCounts := map[segment.Kind]int{}
	for i := range events {
		kindCounts[events[i].Kind]++
		encoded, err := subscribe.Encode(&events[i])
		require.NoError(t, err, "encode archived event seq=%d", events[i].Seq)
		var gm map[string]any
		require.NoError(t, json.Unmarshal(encoded, &gm))
		key := matchKey(gm)
		require.NotEmpty(t, key, "archived event seq=%d produced no match key", events[i].Seq)
		_, dup := got[key]
		require.False(t, dup, "duplicate archived event for key %q", key)
		got[key] = gm
	}

	// The archive's kind mix must match the manifest exactly.
	require.Equal(t, m.Creates, kindCounts[segment.KindCreate])
	require.Equal(t, m.Updates, kindCounts[segment.KindUpdate])
	require.Equal(t, m.Deletes, kindCounts[segment.KindDelete])
	require.Equal(t, m.Identity, kindCounts[segment.KindIdentity])
	require.Equal(t, m.Account, kindCounts[segment.KindAccount])

	// Every production v1 event must have an archived twin, equal in
	// every field except the local-only ones.
	for _, want := range expected {
		key := matchKey(want)
		require.NotEmpty(t, key, "expected v1 line produced no match key: %v", want)
		gm, ok := got[key]
		require.True(t, ok, "no archived event for production v1 event %q", key)
		delete(got, key)

		w, g := stripLocalFields(want), stripLocalFields(gm)
		require.True(t, reflect.DeepEqual(w, g),
			"v1 wire mismatch for %q\nwant: %s\n got: %s", key, mustJSON(t, w), mustJSON(t, g))
	}
	require.Empty(t, got, "archived events with no production v1 counterpart")
}

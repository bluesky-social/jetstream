package livestream

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/streaming"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// fakeFirehose is a minimal subscribeRepos server: it upgrades to
// a WebSocket and writes a scripted sequence of CBOR frames with
// {op:1, t:"<type>"} headers, exactly the wire format atmos's
// decoder consumes.
type fakeFirehose struct {
	t               *testing.T
	frames          [][]byte     // pre-encoded frames to send
	connWG          atomic.Int32 // tracks live connections
	receivedCursors []string     // cursors observed across reconnects
	cursorsMu       sync.Mutex
}

func (f *fakeFirehose) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/com.atproto.sync.subscribeRepos") {
			http.NotFound(w, r)
			return
		}
		f.cursorsMu.Lock()
		f.receivedCursors = append(f.receivedCursors, r.URL.Query().Get("cursor"))
		f.cursorsMu.Unlock()

		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true,
		})
		if err != nil {
			f.t.Logf("fake firehose accept: %v", err)
			return
		}
		f.connWG.Add(1)
		defer f.connWG.Add(-1)
		defer func() { _ = conn.CloseNow() }()

		ctx := r.Context()
		for _, frame := range f.frames {
			if err := conn.Write(ctx, websocket.MessageBinary, frame); err != nil {
				return
			}
		}
		// Hold open until client closes.
		<-ctx.Done()
	})
}

// encodeFrame builds the CBOR frame format atmos expects:
// {op:1, t:"<type>"} concatenated with the body CBOR.
func encodeFrame(t *testing.T, typ string, body []byte) []byte {
	t.Helper()
	hdr := cbor.AppendMapHeader(nil, 2)
	hdr = append(hdr, cbor.AppendTextKey(nil, "op")...)
	hdr = cbor.AppendInt(hdr, 1)
	hdr = append(hdr, cbor.AppendTextKey(nil, "t")...)
	hdr = cbor.AppendText(hdr, typ)
	return append(hdr, body...)
}

func encodeIdentityFrame(t *testing.T, did string, seq int64) []byte {
	t.Helper()
	id := &comatproto.SyncSubscribeRepos_Identity{
		DID:    did,
		Handle: gt.Some("h.test"),
		Seq:    seq,
		Time:   "2026-05-21T00:00:00Z",
	}
	body, err := id.MarshalCBOR()
	require.NoError(t, err)
	return encodeFrame(t, "#identity", body)
}

// TestProcessBatch_UnknownEventDoesNotAdvanceCursor pins the
// archival-correctness invariant that drives the sentinel-error
// branch in ConvertEvent: a frame whose kind we don't recognize must
// leave lastUpstream pointing at the last RECOGNIZED event so a
// future build that learns to decode the new kind can resume from
// the gap. Without the guard, the cursor jumps past data the archive
// will never contain.
func TestProcessBatch_UnknownEventDoesNotAdvanceCursor(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "live_segments")

	c, err := Open(Config{
		SegmentsDir: dir,
		Store:       st,
		SeqKey:      "live_segments/seq/next",
		CursorKey:   "relay/cursor",
		RelayURL:    "https://example.invalid",
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:    newTestVerifier(t),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	batch := []streaming.Event{
		{Seq: 5, Identity: &comatproto.SyncSubscribeRepos_Identity{
			DID: "did:plc:aaa", Time: "2026-05-21T00:00:00Z",
		}},
		// Event 6 has no recognized field — emulates a future relay
		// type the current build cannot decode.
		{Seq: 6},
		{Seq: 7, Identity: &comatproto.SyncSubscribeRepos_Identity{
			DID: "did:plc:bbb", Time: "2026-05-21T00:00:00Z",
		}},
	}

	require.NoError(t, c.processBatch(t.Context(), batch))
	require.Equal(t, int64(7), c.LastUpstreamSeq(),
		"recognized events should advance lastUpstream past the unknown one")
	// And after the unknown event arrives LAST, the cursor must
	// stop at the previous recognized seq, not skip past it.
	c.lastUpstream.Store(0)
	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{
		{Seq: 100, Identity: &comatproto.SyncSubscribeRepos_Identity{
			DID: "did:plc:ccc", Time: "2026-05-21T00:00:00Z",
		}},
		{Seq: 101},
	}))
	require.Equal(t, int64(100), c.LastUpstreamSeq(),
		"unknown trailing event must not advance the cursor past it")
}

func encodeAccountFrame(t *testing.T, did string, seq int64) []byte {
	t.Helper()
	acc := &comatproto.SyncSubscribeRepos_Account{
		DID:    did,
		Active: true,
		Seq:    seq,
		Time:   "2026-05-21T00:00:00Z",
	}
	body, err := acc.MarshalCBOR()
	require.NoError(t, err)
	return encodeFrame(t, "#account", body)
}

// TestConsumer_Run_HappyPath drives a fake firehose end-to-end and
// asserts on segment contents — not just file size and a counter.
// Specifically: every upstream event must show up in the on-disk
// segment file with the right Kind, DID, and a non-empty CBOR payload
// (where applicable). A regression in ConvertEvent (e.g. mapping
// Identity to KindAccount) would fail this test, where the prior
// version checked only LastUpstreamSeq and "file > 256 bytes" and
// would have passed.
func TestConsumer_Run_HappyPath(t *testing.T) {
	t.Parallel()

	upstream := []struct {
		seq  int64
		kind segment.Kind
		did  string
		make func() []byte
	}{
		{1, segment.KindIdentity, "did:plc:aaa", func() []byte { return encodeIdentityFrame(t, "did:plc:aaa", 1) }},
		{2, segment.KindAccount, "did:plc:aaa", func() []byte { return encodeAccountFrame(t, "did:plc:aaa", 2) }},
		{3, segment.KindIdentity, "did:plc:bbb", func() []byte { return encodeIdentityFrame(t, "did:plc:bbb", 3) }},
		{4, segment.KindAccount, "did:plc:ccc", func() []byte { return encodeAccountFrame(t, "did:plc:ccc", 4) }},
	}
	frames := make([][]byte, 0, len(upstream))
	for _, u := range upstream {
		frames = append(frames, u.make())
	}

	f := &fakeFirehose{t: t, frames: frames}
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	st := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "live_segments")

	c, err := Open(Config{
		SegmentsDir:       dir,
		Store:             st,
		SeqKey:            "live_segments/seq/next",
		CursorKey:         "relay/cursor",
		RelayURL:          srv.URL,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:          newTestVerifier(t),
		MaxEventsPerBlock: 2, // force a block flush after every 2 events
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(ctx) }()

	// Wait for the cursor to reach the last upstream seq AND at
	// least the first block boundary to have produced an
	// OnAfterFlush-driven cursor write — that way the cursor
	// assertion below proves the hook worked, not just Close-time
	// persistence.
	lastSeq := upstream[len(upstream)-1].seq
	require.Eventually(t, func() bool {
		return c.LastUpstreamSeq() >= lastSeq
	}, 3*time.Second, 10*time.Millisecond, "consumer never reached last upstream seq")

	// Read relay/cursor while Run is still active. This proves the
	// per-block OnAfterFlush hook is wired — Close has not yet been
	// called.
	hookPersisted, err := LoadUpstreamCursor(st, "relay/cursor")
	require.NoError(t, err)
	require.GreaterOrEqual(t, hookPersisted, int64(2),
		"OnAfterFlush hook must persist cursor at first block boundary, before Close")

	cancel()
	select {
	case err := <-runErr:
		require.True(t, err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded),
			"Run returned %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	require.NoError(t, c.Close())

	// Cursor at shutdown should reflect the last seq buffered.
	finalCursor, err := LoadUpstreamCursor(st, "relay/cursor")
	require.NoError(t, err)
	require.Equal(t, lastSeq, finalCursor,
		"final cursor must equal the last upstream seq we processed")

	// Decode every event from the on-disk segment files and assert
	// kind / DID / payload-non-emptiness for each.
	got := readAllSegmentEvents(t, dir)
	require.Len(t, got, len(upstream),
		"segment files must contain exactly the events we sent")
	for i, want := range upstream {
		require.Equal(t, want.kind, got[i].Kind, "event[%d] Kind", i)
		require.Equal(t, want.did, got[i].DID, "event[%d] DID", i)
		require.NotEmpty(t, got[i].Payload,
			"event[%d] non-commit kinds carry a CBOR payload", i)
		require.Equal(t, uint64(i), got[i].Seq,
			"event[%d] seq is allocated monotonically by ingest.Writer", i)
	}
}

// readAllSegmentEvents returns every event durably written across all
// segment files in dir, in on-disk order. Active segments are sealed
// in place (the same code path production uses on rotation) so the
// public segment.Reader API can decode them.
func readAllSegmentEvents(t *testing.T, dir string) []segment.Event {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "seg_*.jss"))
	require.NoError(t, err)
	require.NotEmpty(t, matches, "segments dir must have at least one seg_*.jss")

	var events []segment.Event
	for _, path := range matches {
		// Seal in place if the file is still active. segment.New
		// resumes an unsealed file; Seal makes it readable via Open.
		sw, err := segment.New(segment.Config{Path: path})
		switch {
		case err == nil:
			_, sealErr := sw.Seal()
			require.NoError(t, sealErr, "seal %s", path)
		case errors.Is(err, segment.ErrSegmentSealed):
			// already sealed — fine
		default:
			t.Fatalf("open %s for sealing: %v", path, err)
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

// TestConsumer_Run_ResumesFromPersistedCursor verifies the crash
// recovery story: kill the consumer mid-stream, reopen, and assert
// the second connection requests a cursor at or before the last
// durable seq.
func TestConsumer_Run_ResumesFromPersistedCursor(t *testing.T) {
	t.Parallel()

	f := &fakeFirehose{
		t: t,
		frames: [][]byte{
			encodeIdentityFrame(t, "did:plc:aaa", 10),
			encodeAccountFrame(t, "did:plc:aaa", 11),
			encodeIdentityFrame(t, "did:plc:bbb", 12),
			encodeIdentityFrame(t, "did:plc:ccc", 13),
		},
	}
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	st := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "live_segments")

	cfg := Config{
		SegmentsDir:       dir,
		Store:             st,
		SeqKey:            "live_segments/seq/next",
		CursorKey:         "relay/cursor",
		RelayURL:          srv.URL,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:          newTestVerifier(t),
		MaxEventsPerBlock: 2,
	}

	// First run — drain at least one block, then cancel.
	c1, err := Open(cfg)
	require.NoError(t, err)

	ctx1, cancel1 := context.WithCancel(t.Context())
	go func() { _ = c1.Run(ctx1) }()

	require.Eventually(t, func() bool { return c1.LastUpstreamSeq() >= 11 }, 3*time.Second, 10*time.Millisecond)
	cancel1()
	require.NoError(t, c1.Close())

	persistedAfterFirst, err := LoadUpstreamCursor(st, "relay/cursor")
	require.NoError(t, err)
	require.GreaterOrEqual(t, persistedAfterFirst, int64(11))

	// Second run — must request a cursor in its handshake.
	c2, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c2.Close() })

	ctx2, cancel2 := context.WithTimeout(t.Context(), 3*time.Second)
	t.Cleanup(cancel2)
	go func() { _ = c2.Run(ctx2) }()

	require.Eventually(t, func() bool {
		f.cursorsMu.Lock()
		defer f.cursorsMu.Unlock()
		return len(f.receivedCursors) >= 2
	}, 3*time.Second, 10*time.Millisecond)

	f.cursorsMu.Lock()
	defer f.cursorsMu.Unlock()
	require.NotEmpty(t, f.receivedCursors[1], "second connection must include a cursor")
	parsed, err := strconv.ParseInt(f.receivedCursors[1], 10, 64)
	require.NoError(t, err)
	require.GreaterOrEqual(t, parsed, int64(11), "second cursor advances from at least 11 (got %d)", parsed)
}

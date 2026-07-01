package subscribe

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/gt"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func newSteadyStateStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	require.NoError(t, lifecycle.WritePhase(st, lifecycle.PhaseSteadyState, time.Now().UTC()))
	return st
}

func waitForTailBlocked(t *testing.T, b *Tail) {
	t.Helper()
	select {
	case <-b.blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber did not park at live tip")
	}
}

func TestHandler_RejectsWhenNotSteadyState(t *testing.T) {
	t.Parallel()

	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	require.NoError(t, lifecycle.WritePhase(st, lifecycle.PhaseBootstrap, time.Now().UTC()))

	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)

	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	require.Contains(t, string(body), "service not ready")
}

func TestHandler_HappyPath_DeliversIdentityEvent(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)

	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	// Give the handler time to register the subscriber.
	waitForTailBlocked(t, b)

	id := &comatproto.SyncSubscribeRepos_Identity{
		DID:  "did:plc:test",
		Seq:  42,
		Time: "2026-05-25T00:00:00Z",
	}
	payload, err := id.MarshalCBOR()
	require.NoError(t, err)
	var seq uint64
	appendSeq(b, &seq, &segment.Event{
		IndexedAt: 1779719010267528,
		Kind:      segment.KindIdentity,
		DID:       "did:plc:test",
		Payload:   payload,
	})

	_, frame, err := conn.Read(ctx)
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(frame, &got))
	require.Equal(t, "identity", got["kind"])
	require.Equal(t, "did:plc:test", got["did"])
}

func TestHandler_SyncEventNotEmitted(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)

	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	waitForTailBlocked(t, b)

	// Publish a sync event (which the encoder skips) followed by an
	// identity (which the encoder emits). Only the identity should
	// arrive on the wire.
	var seq uint64
	appendSeq(b, &seq, &segment.Event{Kind: segment.KindSync, DID: "did:plc:s"})

	id := &comatproto.SyncSubscribeRepos_Identity{
		DID: "did:plc:i", Seq: 1, Time: "2026-05-25T00:00:00Z",
	}
	payload, err := id.MarshalCBOR()
	require.NoError(t, err)
	appendSeq(b, &seq, &segment.Event{
		IndexedAt: 2, Kind: segment.KindIdentity,
		DID: "did:plc:i", Payload: payload,
	})

	readCtx, readCancel := context.WithTimeout(ctx, time.Second)
	defer readCancel()
	_, frame, err := conn.Read(readCtx)
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(frame, &got))
	require.Equal(t, "identity", got["kind"])
	require.Equal(t, "did:plc:i", got["did"])
}

func TestHandler_DefaultModeDoesNotEmitResyncReplacementRows(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)

	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	waitForTailBlocked(t, b)

	var seq uint64
	appendSeq(b, &seq, &segment.Event{
		IndexedAt:  1,
		Kind:       segment.KindCreateResync,
		DID:        "did:plc:resync",
		Collection: "app.bsky.feed.post",
		Rkey:       "resync",
		Rev:        "3lresync",
		Payload:    []byte{0xa0},
	})
	publishIdentity(t, b, &seq, "did:plc:afterresync", 2)

	frame := readOneFrame(t, ctx, conn)
	var got map[string]any
	require.NoError(t, json.Unmarshal(frame, &got))
	require.Equal(t, "identity", got["kind"])
	require.Equal(t, "did:plc:afterresync", got["did"])
}

func TestHandler_ResyncModeEmitsResyncReplacementRows(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)

	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		V2:     true,
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	waitForTailBlocked(t, b)

	var seq uint64
	appendSeq(b, &seq, &segment.Event{
		IndexedAt:  1,
		Kind:       segment.KindCreateResync,
		DID:        "did:plc:resync",
		Collection: "app.bsky.feed.post",
		Rkey:       "resync",
		Rev:        "3lresync",
		Payload:    []byte{0xa0},
	})

	frame := readOneFrame(t, ctx, conn)
	var got map[string]any
	require.NoError(t, json.Unmarshal(frame, &got))
	require.Equal(t, "commit", got["kind"])
	require.Equal(t, "did:plc:resync", got["did"])
	commit, ok := got["commit"].(map[string]any)
	require.True(t, ok, "commit payload should be present")
	require.Equal(t, "create", commit["operation"])
}

// A subscriber with a collection filter receives #identity AND #account events
// (they carry no collection and always bypass the collection filter, on v1 and
// v2 alike). After dropping client-side tombstone suppression these DID-level
// events are a collection-scoped consumer's only signal to purge a dead
// account's records, so they must be delivered. We publish an identity, then an
// account, then a matching commit, and assert all three arrive in order.
func TestHandler_DIDLevelEventsBypassCollectionFilter(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)

	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		V2:     true,
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?wantedCollections=app.bsky.feed.post"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	waitForTailBlocked(t, b)

	var seq uint64
	// Identity bypasses the collection filter and must be delivered.
	publishIdentity(t, b, &seq, "did:plc:ident", 1)
	// Account (DID-deletion tombstone bearer) must be delivered too.
	acct := &comatproto.SyncSubscribeRepos_Account{
		DID: "did:plc:acct", Active: false, Status: gt.Some("deleted"), Seq: 2, Time: "2026-05-27T00:00:00Z",
	}
	acctPayload, err := acct.MarshalCBOR()
	require.NoError(t, err)
	appendSeq(b, &seq, &segment.Event{
		IndexedAt: 2, Kind: segment.KindAccount, DID: "did:plc:acct", Payload: acctPayload,
	})
	// Matching commit, to bound the test.
	publishCommit(t, b, &seq, "did:plc:acct", "app.bsky.feed.post", 3)

	// First frame must be the identity.
	identFrame := readOneFrame(t, ctx, conn)
	var gotIdent map[string]any
	require.NoError(t, json.Unmarshal(identFrame, &gotIdent))
	require.Equal(t, "identity", gotIdent["kind"], "identity must pass the collection filter")
	require.Equal(t, "did:plc:ident", gotIdent["did"])

	// Second frame must be the account.
	acctFrame := readOneFrame(t, ctx, conn)
	var gotAcct map[string]any
	require.NoError(t, json.Unmarshal(acctFrame, &gotAcct))
	require.Equal(t, "account", gotAcct["kind"], "account must pass the collection filter")
	require.Equal(t, "did:plc:acct", gotAcct["did"])

	// Third frame must be the matching commit.
	commitFrame := readOneFrame(t, ctx, conn)
	var gotCommit map[string]any
	require.NoError(t, json.Unmarshal(commitFrame, &gotCommit))
	require.Equal(t, "commit", gotCommit["kind"])
}

func TestHandler_V2DeliversRecordCBORAndSync(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)

	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		V2:     true,
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	waitForTailBlocked(t, b)

	var seq uint64
	payload := []byte{0xa0}
	appendSeq(b, &seq, &segment.Event{
		IndexedAt:  1779719010267528,
		Kind:       segment.KindCreate,
		DID:        "did:plc:v2",
		Collection: "app.bsky.feed.post",
		Rkey:       "abcd1234",
		Rev:        "3lXrev",
		Payload:    payload,
	})

	commitFrame := readOneFrame(t, ctx, conn)
	var commit map[string]any
	require.NoError(t, json.Unmarshal(commitFrame, &commit))
	require.Equal(t, "commit", commit["kind"])
	require.Equal(t, float64(0), commit["cursor"])
	require.Equal(t, float64(0), commit["seq"])
	require.NotContains(t, commit, "upstream_relay_cursor")
	commitPayload, ok := commit["commit"].(map[string]any)
	require.True(t, ok, "commit payload not a map")
	require.Equal(t, base64.StdEncoding.EncodeToString(payload), commitPayload["record_cbor"])

	syncEvt := &comatproto.SyncSubscribeRepos_Sync{
		DID: "did:plc:v2", Rev: "rev-sync", Seq: 222, Time: "2026-05-25T00:00:00Z",
		Blocks: []byte{0x01},
	}
	syncPayload, err := syncEvt.MarshalCBOR()
	require.NoError(t, err)
	appendSeq(b, &seq, &segment.Event{
		IndexedAt: 1779719010267529,
		Kind:      segment.KindSync,
		DID:       "did:plc:v2",
		Rev:       "rev-sync",
		Payload:   syncPayload,
	})

	syncFrame := readOneFrame(t, ctx, conn)
	var syncGot map[string]any
	require.NoError(t, json.Unmarshal(syncFrame, &syncGot))
	require.Equal(t, "sync", syncGot["kind"])
	require.Equal(t, float64(1), syncGot["cursor"])
	require.Contains(t, syncGot, "sync")
}

func TestHandler_V1ModeDoesNotEmitV2Fields(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)

	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	waitForTailBlocked(t, b)

	var seq uint64
	appendSeq(b, &seq, &segment.Event{
		IndexedAt:  1779719010267528,
		Kind:       segment.KindCreate,
		DID:        "did:plc:simple",
		Collection: "app.bsky.feed.post",
		Rkey:       "abcd1234",
		Rev:        "3lXrev",
		Payload:    []byte{0xa0},
	})

	frame := readOneFrame(t, ctx, conn)
	var got map[string]any
	require.NoError(t, json.Unmarshal(frame, &got))
	require.Equal(t, "commit", got["kind"])
	require.NotContains(t, got, "seq")
	require.NotContains(t, got, "upstream_relay_cursor")
	commit, ok := got["commit"].(map[string]any)
	require.True(t, ok, "commit not a map")
	require.NotContains(t, commit, "record_cbor")
}

// readOneFrame reads one text frame with a 1s deadline. Centralizes the
// pattern so tests stay terse.
func readOneFrame(t *testing.T, ctx context.Context, conn *websocket.Conn) []byte {
	t.Helper()
	rctx, rcancel := context.WithTimeout(ctx, 1*time.Second)
	defer rcancel()
	_, frame, err := conn.Read(rctx)
	require.NoError(t, err)
	return frame
}

// readOneZstdFrame reads one websocket frame and asserts it is a BINARY
// frame, then decodes it with a dictionary-seeded reader — exactly what a
// v1 client does. Returns the decoded JSON bytes.
func readOneZstdFrame(t *testing.T, ctx context.Context, conn *websocket.Conn) []byte {
	t.Helper()
	rctx, rcancel := context.WithTimeout(ctx, 1*time.Second)
	defer rcancel()
	mt, frame, err := conn.Read(rctx)
	require.NoError(t, err)
	require.Equal(t, websocket.MessageBinary, mt, "zstd clients must receive binary frames")

	dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(zstdDictionary))
	require.NoError(t, err)
	defer dec.Close()
	got, err := dec.DecodeAll(frame, nil)
	require.NoError(t, err, "frame must decode with a dictionary-seeded reader")
	return got
}

// appendSeq assigns the next dense seq and appends, mirroring how ingest
// drives the Tail in production (Append after durable seq assignment).
func appendSeq(tl *Tail, seqCtr *uint64, ev *segment.Event) {
	ev.Seq = *seqCtr
	*seqCtr++
	tl.Append(ev)
}

// publishIdentity publishes a minimal identity event the encoder can render.
func publishIdentity(t *testing.T, tl *Tail, seqCtr *uint64, did string, indexedAt int64) {
	t.Helper()
	id := &comatproto.SyncSubscribeRepos_Identity{
		DID: did, Seq: indexedAt, Time: "2026-05-27T00:00:00Z",
	}
	payload, err := id.MarshalCBOR()
	require.NoError(t, err)
	appendSeq(tl, seqCtr, &segment.Event{
		IndexedAt: indexedAt, Kind: segment.KindIdentity,
		DID: did, Payload: payload,
	})
}

// publishCommit publishes a minimal create commit. The Payload is a
// DAG-CBOR-encoded empty map (0xa0), which the encoder will turn into "{}".
func publishCommit(t *testing.T, tl *Tail, seqCtr *uint64, did, collection string, indexedAt int64) {
	t.Helper()
	appendSeq(tl, seqCtr, &segment.Event{
		IndexedAt:  indexedAt,
		Kind:       segment.KindCreate,
		DID:        did,
		Collection: collection,
		Rkey:       "abcd1234",
		Rev:        "3lXrev",
		Payload:    []byte{0xa0}, // CBOR empty map
	})
}

// publishOversizeCommit publishes a commit with a payload large enough
// that the encoded JSON envelope will exceed any modest maxMessageSizeBytes.
func publishOversizeCommit(t *testing.T, tl *Tail, seqCtr *uint64, did, collection string, indexedAt int64) {
	t.Helper()
	// CBOR map of 1 entry: key "x" → byte string of 4096 bytes.
	// 0xa1 = map(1); 0x61 = text(1); 0x78 ... = bytes header.
	big := bytes.NewBuffer(nil)
	big.WriteByte(0xa1) // map(1)
	big.WriteByte(0x61) // text(1)
	big.WriteByte('x')  // "x"
	big.WriteByte(0x59) // bytes, 2-byte length follows
	big.WriteByte(0x10) // 0x1000 = 4096
	big.WriteByte(0x00)
	big.Write(make([]byte, 0x1000)) // 4096 zero bytes
	appendSeq(tl, seqCtr, &segment.Event{
		IndexedAt:  indexedAt,
		Kind:       segment.KindCreate,
		DID:        did,
		Collection: collection,
		Rkey:       "abcd1234",
		Rev:        "3lXrev",
		Payload:    big.Bytes(),
	})
}

func TestHandler_Filter_RejectsInvalidQuery(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	// Illegal prefix (must be at NSID boundary).
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"?wantedCollections=app.bsky.fo*", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	require.Contains(t, string(body), "invalid collection")
}

// TestHandler_Filter_RejectsTooManyQueryParams verifies the handler
// returns HTTP 400 when the unique wantedDids count exceeds
// MaxWantedDIDs. Uses unique DIDs so the post-dedupe cap fires —
// duplicates would dedupe back below the cap (V1 PARITY).
func TestHandler_Filter_RejectsTooManyQueryParams(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	var sb strings.Builder
	for i := 0; i <= MaxWantedDIDs; i++ {
		if i > 0 {
			sb.WriteByte('&')
		}
		fmt.Fprintf(&sb, "wantedDids=did:web:host%d.example.com", i)
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"?"+sb.String(), nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	require.Contains(t, string(body), "subscribe: invalid options",
		"the response should be wrapped in our ErrInvalidOptions string")
}

// TestHandler_ZstdQueryParam_DeliversDictCompressedFrames verifies the v1
// custom-zstd opt-in via ?compress=true: frames arrive BINARY and decode,
// with the v1 dictionary, to the same JSON the uncompressed path emits.
func TestHandler_ZstdQueryParam_DeliversDictCompressedFrames(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?compress=true"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// CompressionDisabled: the client must NOT offer permessage-deflate,
	// or the handler rejects the both-at-once combination.
	conn, resp, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		CompressionMode: websocket.CompressionDisabled,
	})
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	waitForTailBlocked(t, b)
	var seq uint64
	publishIdentity(t, b, &seq, "did:plc:zstdquery", 1)

	got := readOneZstdFrame(t, ctx, conn)
	require.Contains(t, string(got), "did:plc:zstdquery")
	require.Contains(t, string(got), `"kind":"identity"`)
}

// TestHandler_ZstdSocketEncodingHeader_DeliversDictCompressedFrames
// verifies the alternate v1 opt-in signal: Socket-Encoding: zstd. Same
// contract as the query-param path.
func TestHandler_ZstdSocketEncodingHeader_DeliversDictCompressedFrames(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		CompressionMode: websocket.CompressionDisabled,
		HTTPHeader:      http.Header{"Socket-Encoding": []string{"zstd"}},
	})
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	waitForTailBlocked(t, b)
	var seq uint64
	publishIdentity(t, b, &seq, "did:plc:zstdheader", 1)

	got := readOneZstdFrame(t, ctx, conn)
	require.Contains(t, string(got), "did:plc:zstdheader")
}

// TestHandler_RejectsZstdAndDeflateTogether verifies the mutual-exclusion
// rule: a client opting into custom zstd (?compress=true) while ALSO
// offering RFC 7692 permessage-deflate is rejected with a 400, rather than
// silently double-compressing or dropping one scheme.
func TestHandler_RejectsZstdAndDeflateTogether(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	// Plain HTTP GET (not a ws Dial) so we can set the exact extension
	// offer header and read the 400 body. The handler's mutual-exclusion
	// check runs before the upgrade, so a 400 is returned regardless.
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"?compress=true", nil)
	require.NoError(t, err)
	req.Header.Set("Sec-WebSocket-Extensions", "permessage-deflate; client_max_window_bits")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	require.Contains(t, string(body), "choose one compression scheme",
		"the 400 body should explain zstd and permessage-deflate are mutually exclusive")
}

// TestHandler_AllowsCompressFalse verifies the compress=false (the v1
// default) is accepted — we only reject affirmative requests for zstd.
func TestHandler_AllowsCompressFalse(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?compress=false"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()
}

// TestHandler_NegotiatesCompression_WhenClientOffers verifies the
// handler negotiates RFC 7692 permessage-deflate when a client offers
// it. The handshake response must echo the extension; we assert on the
// Sec-WebSocket-Extensions header because compression is otherwise
// transparent on the read path. Note this is independent of the v1
// zstd-with-custom-dictionary scheme rejected via ?compress=true.
func TestHandler_NegotiatesCompression_WhenClientOffers(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		CompressionMode: websocket.CompressionContextTakeover,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	if resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	require.Contains(t, resp.Header.Get("Sec-WebSocket-Extensions"), "permessage-deflate",
		"the handshake response must echo permessage-deflate when the client offers it")

	// Compression is transparent on the wire: a compressed frame must
	// still decode to the same JSON the uncompressed path produces.
	waitForTailBlocked(t, b)
	var seq uint64
	publishIdentity(t, b, &seq, "did:plc:compressed", 1)
	frame := readOneFrame(t, ctx, conn)
	require.Contains(t, string(frame), "did:plc:compressed")
}

// TestHandler_NoCompression_WhenClientDoesNotOffer verifies graceful
// fallback: a client that does not advertise permessage-deflate gets an
// uncompressed connection, and the handshake response carries no
// extension. This is the path Safari and bare non-browser clients take.
func TestHandler_NoCompression_WhenClientDoesNotOffer(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// CompressionDisabled (the Dial default) means the client sends no
	// Sec-WebSocket-Extensions offer.
	conn, resp, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		CompressionMode: websocket.CompressionDisabled,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	if resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	require.Empty(t, resp.Header.Get("Sec-WebSocket-Extensions"),
		"no compression extension should be negotiated when the client does not offer one")

	waitForTailBlocked(t, b)
	var seq uint64
	publishIdentity(t, b, &seq, "did:plc:plain", 1)
	frame := readOneFrame(t, ctx, conn)
	require.Contains(t, string(frame), "did:plc:plain")
}

func TestHandler_Filter_WantedCollections_DeliversMatching(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?wantedCollections=app.bsky.feed.post"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	waitForTailBlocked(t, b)

	// Build a record-bearing commit. Encoder needs DAG-CBOR Payload + a CID;
	// the simplest valid CBOR is an empty map: 0xa0 = empty map.
	var seq uint64
	publishCommit(t, b, &seq, "did:plc:abc", "app.bsky.feed.like", 1)
	publishCommit(t, b, &seq, "did:plc:abc", "app.bsky.feed.post", 2)

	frame := readOneFrame(t, ctx, conn)
	var got map[string]any
	require.NoError(t, json.Unmarshal(frame, &got))
	require.Equal(t, "commit", got["kind"])
	commit, ok := got["commit"].(map[string]any)
	require.True(t, ok, "commit field should be a map")
	require.Equal(t, "app.bsky.feed.post", commit["collection"])
}

// V1 PARITY end-to-end check: a top-level vendor prefix
// "app.bsky.*" (only 2 segments before the wildcard) must be accepted
// as a filter and match commits in any sub-collection. This is the
// regression case the v1 code accepts but our previous strict
// NSID-validating prefix branch rejected.
func TestHandler_Filter_WantedCollections_TopLevelPrefix(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?wantedCollections=app.bsky.*"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err, "two-segment prefix must be accepted (v1 parity)")
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()
	waitForTailBlocked(t, b)

	var seq uint64
	publishCommit(t, b, &seq, "did:plc:abc", "com.example.foo", 1)       // outside prefix
	publishCommit(t, b, &seq, "did:plc:abc", "app.bsky.feed.post", 2)    // inside prefix
	publishCommit(t, b, &seq, "did:plc:abc", "app.bsky.graph.follow", 3) // inside prefix

	for range 2 {
		frame := readOneFrame(t, ctx, conn)
		var got map[string]any
		require.NoError(t, json.Unmarshal(frame, &got))
		commit, ok := got["commit"].(map[string]any)
		require.True(t, ok)
		col, _ := commit["collection"].(string)
		require.True(t, strings.HasPrefix(col, "app.bsky."),
			"unexpected collection %q delivered through app.bsky.* filter", col)
	}
}

func TestHandler_Filter_WantedCollections_PrefixMatch(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?wantedCollections=app.bsky.graph.*"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()
	waitForTailBlocked(t, b)

	var seq uint64
	publishCommit(t, b, &seq, "did:plc:abc", "app.bsky.feed.post", 1)
	publishCommit(t, b, &seq, "did:plc:abc", "app.bsky.graph.follow", 2)

	frame := readOneFrame(t, ctx, conn)
	var got map[string]any
	require.NoError(t, json.Unmarshal(frame, &got))
	commit, ok := got["commit"].(map[string]any)
	require.True(t, ok, "commit field should be a map")
	require.Equal(t, "app.bsky.graph.follow", commit["collection"])
}

func TestHandler_Filter_WantedDIDs_DeliversMatching(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?wantedDids=did:plc:want"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()
	waitForTailBlocked(t, b)

	var seq uint64
	publishCommit(t, b, &seq, "did:plc:other", "app.bsky.feed.post", 1)
	publishCommit(t, b, &seq, "did:plc:want", "app.bsky.feed.post", 2)

	frame := readOneFrame(t, ctx, conn)
	var got map[string]any
	require.NoError(t, json.Unmarshal(frame, &got))
	require.Equal(t, "did:plc:want", got["did"])
}

func TestHandler_Filter_IdentityBypassesCollectionFilter(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?wantedCollections=app.bsky.feed.post"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()
	waitForTailBlocked(t, b)

	var seq uint64
	publishIdentity(t, b, &seq, "did:plc:any", 1)

	frame := readOneFrame(t, ctx, conn)
	var got map[string]any
	require.NoError(t, json.Unmarshal(frame, &got))
	require.Equal(t, "identity", got["kind"])
}

func TestHandler_Filter_IdentityRespectsDIDFilter(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?wantedDids=did:plc:want"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()
	waitForTailBlocked(t, b)

	var seq uint64
	publishIdentity(t, b, &seq, "did:plc:other", 1)
	publishIdentity(t, b, &seq, "did:plc:want", 2)

	frame := readOneFrame(t, ctx, conn)
	var got map[string]any
	require.NoError(t, json.Unmarshal(frame, &got))
	require.Equal(t, "did:plc:want", got["did"])
}

func TestHandler_Filter_MaxMessageSize_DropsOversize(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	// 200 bytes is enough for a small commit envelope but not for a giant one.
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?maxMessageSizeBytes=200"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()
	waitForTailBlocked(t, b)

	// Identity events are tiny; one should fit. Use them as the
	// "delivered" half of this test rather than constructing oversize
	// commits (which require valid CBOR + CID).
	var seq uint64
	publishOversizeCommit(t, b, &seq, "did:plc:big", "app.bsky.feed.post", 1)
	publishIdentity(t, b, &seq, "did:plc:small", 2)

	frame := readOneFrame(t, ctx, conn)
	var got map[string]any
	require.NoError(t, json.Unmarshal(frame, &got))
	// We should see the identity (small) but never the oversize commit.
	require.Equal(t, "identity", got["kind"])
}

// V1 PARITY regression guard — empty maxMessageSizeBytes coerces to "no cap".
func TestHandler_Filter_MaxMessageSize_EmptyMeansNoCap(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?maxMessageSizeBytes="

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err, "empty maxMessageSizeBytes must NOT reject the connection")
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()
}

// V1 PARITY regression guard — negative maxMessageSizeBytes coerces to "no cap".
func TestHandler_Filter_MaxMessageSize_NegativeMeansNoCap(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?maxMessageSizeBytes=-1"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err, "negative maxMessageSizeBytes must NOT reject the connection")
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()
}

func TestHandler_OptionsUpdate_ChangesFilter(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()
	waitForTailBlocked(t, b)

	// Narrow to likes only.
	update := SubscriberSourcedMessage{
		Type: SubMessageTypeOptionsUpdate,
		Payload: jsonMust(t, UpdatePayload{
			WantedCollections: []string{"app.bsky.feed.like"},
		}),
	}
	require.NoError(t, conn.Write(ctx, websocket.MessageText, jsonMust(t, update)))

	// Give the reader goroutine a moment to apply the update.
	time.Sleep(10 * time.Millisecond)

	var seq uint64
	publishCommit(t, b, &seq, "did:plc:abc", "app.bsky.feed.post", 1)
	publishCommit(t, b, &seq, "did:plc:abc", "app.bsky.feed.like", 2)

	frame := readOneFrame(t, ctx, conn)
	var got map[string]any
	require.NoError(t, json.Unmarshal(frame, &got))
	commit, ok := got["commit"].(map[string]any)
	require.True(t, ok, "commit field should be a map")
	require.Equal(t, "app.bsky.feed.like", commit["collection"])
}

func TestHandler_OptionsUpdate_InvalidPayloadDisconnects(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusInternalError, "test done") }()
	waitForTailBlocked(t, b)

	// Send malformed JSON envelope.
	require.NoError(t, conn.Write(ctx, websocket.MessageText, []byte("not json")))

	// The next read should observe a close.
	rctx, rcancel := context.WithTimeout(ctx, 2*time.Second)
	defer rcancel()
	_, _, err = conn.Read(rctx)
	require.Error(t, err, "expected close after malformed envelope")
}

func TestHandler_OptionsUpdate_BadNSIDDisconnects(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusInternalError, "test done") }()
	waitForTailBlocked(t, b)

	update := SubscriberSourcedMessage{
		Type: SubMessageTypeOptionsUpdate,
		Payload: jsonMust(t, UpdatePayload{
			WantedCollections: []string{"app.bsky.fo*"}, // illegal prefix
		}),
	}
	require.NoError(t, conn.Write(ctx, websocket.MessageText, jsonMust(t, update)))

	rctx, rcancel := context.WithTimeout(ctx, 2*time.Second)
	defer rcancel()
	_, _, err = conn.Read(rctx)
	require.Error(t, err, "expected close after bad NSID in options_update")
}

func TestHandler_OptionsUpdate_OversizePayload(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	// The handler raises the server-side read limit to exactly
	// MaxSubscriberMessageBytes; one byte beyond it should be the
	// websocket layer's read-limit close (StatusMessageTooBig), which
	// the application path observes as a Read error and counts via
	// the optionsUpdateErrorReasonOversize metric label.
	defer func() { _ = conn.Close(websocket.StatusInternalError, "test done") }()
	waitForTailBlocked(t, b)

	// Send a payload just over MaxSubscriberMessageBytes.
	big := make([]byte, MaxSubscriberMessageBytes+1)
	for i := range big {
		big[i] = ' '
	}
	require.NoError(t, conn.Write(ctx, websocket.MessageText, big))

	rctx, rcancel := context.WithTimeout(ctx, 2*time.Second)
	defer rcancel()
	_, _, err = conn.Read(rctx)
	require.Error(t, err, "expected close after oversize subscriber message")
}

func TestHandler_OptionsUpdate_UnknownTypeIgnored(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()
	waitForTailBlocked(t, b)

	// V1 PARITY: unknown message types are logged and ignored, not fatal.
	unknown := SubscriberSourcedMessage{
		Type:    "unknown_type",
		Payload: json.RawMessage(`null`),
	}
	require.NoError(t, conn.Write(ctx, websocket.MessageText, jsonMust(t, unknown)))

	// Subsequent events must still flow.
	time.Sleep(10 * time.Millisecond)
	var seq uint64
	publishIdentity(t, b, &seq, "did:plc:still-alive", 1)
	frame := readOneFrame(t, ctx, conn)
	var got map[string]any
	require.NoError(t, json.Unmarshal(frame, &got))
	require.Equal(t, "identity", got["kind"])
}

func TestHandler_RequireHello_BlocksUntilOptionsUpdate(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)

	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?requireHello=true"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	// Give the handler time to start the reader goroutine but NOT
	// time to subscribe (it shouldn't subscribe until hello).
	time.Sleep(10 * time.Millisecond)

	// Append a matching event. The subscriber loop hasn't started yet (it
	// waits for hello), so it will begin reading at the live tip — which is
	// already past this event. A pre-hello event is therefore never
	// delivered: live subscribers do not replay history.
	var seq uint64
	publishIdentity(t, b, &seq, "did:plc:pre-hello", 1)

	// Wait long enough to ensure that IF the event were going to be
	// delivered, it would have been (but it won't be, because we
	// haven't sent hello yet).
	time.Sleep(10 * time.Millisecond)

	// Send the hello.
	hello := SubscriberSourcedMessage{
		Type:    SubMessageTypeOptionsUpdate,
		Payload: jsonMust(t, UpdatePayload{}),
	}
	require.NoError(t, conn.Write(ctx, websocket.MessageText, jsonMust(t, hello)))

	// Give the handler time to subscribe.
	waitForTailBlocked(t, b)

	// Publish a fresh event AFTER hello. Only this one should arrive.
	publishIdentity(t, b, &seq, "did:plc:post-hello", 2)

	frame := readOneFrame(t, ctx, conn)
	require.Contains(t, string(frame), "did:plc:post-hello")
	require.NotContains(t, string(frame), "did:plc:pre-hello",
		"pre-hello append precedes the live-tip start and must not be delivered")
}

// V1 PARITY: a filter delivered in the hello options_update must take
// effect before any events flow. This is the load-bearing reason
// requireHello exists — clients use it to install their filter before
// the firehose opens, avoiding a window of unfiltered traffic.
func TestHandler_RequireHello_FilterFromHelloApplies(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)

	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?requireHello=true"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	// Hello with a wantedDids filter that excludes "did:plc:other".
	hello := SubscriberSourcedMessage{
		Type: SubMessageTypeOptionsUpdate,
		Payload: jsonMust(t, UpdatePayload{
			WantedDIDs: []string{"did:plc:wanted"},
		}),
	}
	require.NoError(t, conn.Write(ctx, websocket.MessageText, jsonMust(t, hello)))

	// Give the handler time to apply the filter and Subscribe.
	waitForTailBlocked(t, b)

	// Publish an event that the filter should drop, then one it should
	// pass. The reader can only see the second one if the filter was
	// installed before Subscribe — which is the contract.
	var seq uint64
	publishIdentity(t, b, &seq, "did:plc:other", 1)
	publishIdentity(t, b, &seq, "did:plc:wanted", 2)

	frame := readOneFrame(t, ctx, conn)
	require.Contains(t, string(frame), "did:plc:wanted")
	require.NotContains(t, string(frame), "did:plc:other",
		"hello-supplied filter must apply before events flow")
}

func TestHandler_RequireHello_InvalidUpdateDisconnects(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		frame     []byte
		wantClose websocket.StatusCode
	}{
		{
			name:      "malformed envelope JSON",
			frame:     []byte(`{`),
			wantClose: websocket.StatusInvalidFramePayloadData,
		},
		{
			name:      "well-formed envelope with bad payload JSON",
			frame:     []byte(`{"type":"options_update","payload":"not-json"}`),
			wantClose: websocket.StatusInvalidFramePayloadData,
		},
		{
			name: "well-formed payload with bad DID",
			frame: jsonMust(t, SubscriberSourcedMessage{
				Type: SubMessageTypeOptionsUpdate,
				Payload: jsonMust(t, UpdatePayload{
					WantedDIDs: []string{"not-a-did"},
				}),
			}),
			wantClose: websocket.StatusPolicyViolation,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := newSteadyStateStore(t)
			b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
			require.NoError(t, err)

			h := NewHandler(Subscription{
				Tail:   b,
				Store:  st,
				Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			})
			srv := httptest.NewServer(h)
			defer srv.Close()

			wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?requireHello=true"

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			conn, resp, err := websocket.Dial(ctx, wsURL, nil)
			require.NoError(t, err)
			if resp != nil && resp.Body != nil {
				defer func() { _ = resp.Body.Close() }()
			}
			defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

			require.NoError(t, conn.Write(ctx, websocket.MessageText, tc.frame))

			// The server should close the connection. Read returns the
			// close as an error; CloseStatus extracts the code.
			_, _, rerr := conn.Read(ctx)
			require.Error(t, rerr)
			require.Equal(t, tc.wantClose, websocket.CloseStatus(rerr),
				"close status mismatch (err=%v)", rerr)
		})
	}
}

func TestHandler_RequireHello_FalseHasNoEffect(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		// queryFragment is appended to "ws://..." with "?" already
		// present iff non-empty. "" means no query string at all.
		queryFragment string
	}{
		{"absent", ""},
		{"false", "?requireHello=false"},
		{"capitalized True", "?requireHello=True"},
		{"garbage", "?requireHello=garbage"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := newSteadyStateStore(t)
			b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
			require.NoError(t, err)

			h := NewHandler(Subscription{
				Tail:   b,
				Store:  st,
				Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			})
			srv := httptest.NewServer(h)
			defer srv.Close()

			wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + tc.queryFragment

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			conn, resp, err := websocket.Dial(ctx, wsURL, nil)
			require.NoError(t, err)
			if resp != nil && resp.Body != nil {
				defer func() { _ = resp.Body.Close() }()
			}
			defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

			// Wait for the handler to register the subscriber.
			waitForTailBlocked(t, b)

			// No hello sent. Publish and expect immediate delivery.
			var seq uint64
			publishIdentity(t, b, &seq, "did:plc:no-hello-needed", 1)

			frame := readOneFrame(t, ctx, conn)
			require.Contains(t, string(frame), "did:plc:no-hello-needed")
		})
	}
}

// Small helper used by the options_update tests.
func jsonMust[T any](t *testing.T, v T) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

// truncateCloseReason must always emit valid UTF-8 (RFC 6455 §5.5.1
// requires the close-frame reason to be valid UTF-8, and
// coder/websocket rejects close frames whose reason is not). The
// echoed-back-input close path can mid-cut a multi-byte rune unless
// the cut snaps to a rune boundary.
func TestTruncateCloseReason_RuneAligned(t *testing.T) {
	t.Parallel()

	// Build an input where a naive byte-cut at 120 lands inside a
	// 3-byte rune. Each "λ" is 2 bytes; "你" is 3 bytes. Pad with
	// ASCII so the truncate point falls inside a multi-byte rune.
	pad := strings.Repeat("a", 119)
	in := pad + "你你你你你你"
	out := truncateCloseReason(in)
	require.LessOrEqual(t, len(out), 123, "must fit close-frame cap")
	require.True(t, utf8.ValidString(out), "truncated reason must be valid UTF-8: %q", out)
	require.True(t, strings.HasSuffix(out, "..."), "expected truncation suffix")

	// Short input — no truncation, no suffix.
	short := "hello"
	require.Equal(t, short, truncateCloseReason(short))

	// Boundary: input exactly at the cap is returned unchanged.
	exact := strings.Repeat("a", 123)
	require.Equal(t, exact, truncateCloseReason(exact))
}

func TestHandler_RequireHello_NoLeakOnClientDisconnect(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)

	// Wrap the real handler so we can observe ServeHTTP returning. That
	// signal is deterministic: serve() returning means its deferred
	// conn.CloseNow ran, which unblocks the reader goroutine's conn.Read.
	// We avoid runtime.NumGoroutine() because t.Parallel tests in the
	// same binary spawn/retire goroutines independently and would race
	// the global counter.
	inner := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	served := make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(served)
		inner.ServeHTTP(w, r)
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?requireHello=true"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}

	// Let the handler get into its hello wait. The reader goroutine
	// is running; the writer-side serve() body is blocked on helloCh.
	time.Sleep(10 * time.Millisecond)

	// Client closes without sending hello. The handler's reader goroutine
	// observes the read error, defer cancel()s the connection context,
	// the wait select exits via <-ctx.Done(), and serve returns.
	require.NoError(t, conn.Close(websocket.StatusNormalClosure, "go away"))

	select {
	case <-served:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return after client disconnect during hello wait")
	}
}

func TestHandler_RequireHello_MultipleUpdatesDoNotPanic(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)

	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?requireHello=true"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	hello := SubscriberSourcedMessage{
		Type:    SubMessageTypeOptionsUpdate,
		Payload: jsonMust(t, UpdatePayload{}),
	}
	helloBytes := jsonMust(t, hello)

	// Send the first hello.
	require.NoError(t, conn.Write(ctx, websocket.MessageText, helloBytes))
	// Immediately send a second valid options_update.
	require.NoError(t, conn.Write(ctx, websocket.MessageText, helloBytes))
	// And a third, for good measure.
	require.NoError(t, conn.Write(ctx, websocket.MessageText, helloBytes))

	// Wait for the handler to process the hellos and subscribe.
	waitForTailBlocked(t, b)

	// Confirm normal flow works after the chatty start.
	var seq uint64
	publishIdentity(t, b, &seq, "did:plc:still-flowing", 1)
	frame := readOneFrame(t, ctx, conn)
	require.Contains(t, string(frame), "did:plc:still-flowing")
}

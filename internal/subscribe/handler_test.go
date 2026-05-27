package subscribe

import (
	"bytes"
	"context"
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

	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/api/comatproto"
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

func TestHandler_RejectsWhenNotSteadyState(t *testing.T) {
	t.Parallel()

	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	require.NoError(t, lifecycle.WritePhase(st, lifecycle.PhaseBootstrap, time.Now().UTC()))

	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)

	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
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
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)

	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
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
	time.Sleep(50 * time.Millisecond)

	id := &comatproto.SyncSubscribeRepos_Identity{
		DID:  "did:plc:test",
		Seq:  42,
		Time: "2026-05-25T00:00:00Z",
	}
	payload, err := id.MarshalCBOR()
	require.NoError(t, err)
	b.Publish(&segment.Event{
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
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)

	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
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

	time.Sleep(50 * time.Millisecond)

	// Publish a sync event (which the encoder skips) followed by an
	// identity (which the encoder emits). Only the identity should
	// arrive on the wire.
	b.Publish(&segment.Event{Kind: segment.KindSync, DID: "did:plc:s"})

	id := &comatproto.SyncSubscribeRepos_Identity{
		DID: "did:plc:i", Seq: 1, Time: "2026-05-25T00:00:00Z",
	}
	payload, err := id.MarshalCBOR()
	require.NoError(t, err)
	b.Publish(&segment.Event{
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

// publishIdentity publishes a minimal identity event the encoder can render.
func publishIdentity(t *testing.T, b *Broadcaster, did string, indexedAt int64) {
	t.Helper()
	id := &comatproto.SyncSubscribeRepos_Identity{
		DID: did, Seq: indexedAt, Time: "2026-05-27T00:00:00Z",
	}
	payload, err := id.MarshalCBOR()
	require.NoError(t, err)
	b.Publish(&segment.Event{
		IndexedAt: indexedAt, Kind: segment.KindIdentity,
		DID: did, Payload: payload,
	})
}

// publishCommit publishes a minimal create commit. The Payload is a
// DAG-CBOR-encoded empty map (0xa0), which the encoder will turn into "{}".
func publishCommit(t *testing.T, b *Broadcaster, did, collection string, indexedAt int64) {
	t.Helper()
	b.Publish(&segment.Event{
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
func publishOversizeCommit(t *testing.T, b *Broadcaster, did, collection string, indexedAt int64) {
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
	b.Publish(&segment.Event{
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
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
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
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
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

func TestHandler_Filter_WantedCollections_DeliversMatching(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
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

	time.Sleep(50 * time.Millisecond)

	// Build a record-bearing commit. Encoder needs DAG-CBOR Payload + a CID;
	// the simplest valid CBOR is an empty map: 0xa0 = empty map.
	publishCommit(t, b, "did:plc:abc", "app.bsky.feed.like", 1)
	publishCommit(t, b, "did:plc:abc", "app.bsky.feed.post", 2)

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
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
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
	time.Sleep(50 * time.Millisecond)

	publishCommit(t, b, "did:plc:abc", "com.example.foo", 1)       // outside prefix
	publishCommit(t, b, "did:plc:abc", "app.bsky.feed.post", 2)    // inside prefix
	publishCommit(t, b, "did:plc:abc", "app.bsky.graph.follow", 3) // inside prefix

	for i := 0; i < 2; i++ {
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
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
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
	time.Sleep(50 * time.Millisecond)

	publishCommit(t, b, "did:plc:abc", "app.bsky.feed.post", 1)
	publishCommit(t, b, "did:plc:abc", "app.bsky.graph.follow", 2)

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
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
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
	time.Sleep(50 * time.Millisecond)

	publishCommit(t, b, "did:plc:other", "app.bsky.feed.post", 1)
	publishCommit(t, b, "did:plc:want", "app.bsky.feed.post", 2)

	frame := readOneFrame(t, ctx, conn)
	var got map[string]any
	require.NoError(t, json.Unmarshal(frame, &got))
	require.Equal(t, "did:plc:want", got["did"])
}

func TestHandler_Filter_IdentityBypassesCollectionFilter(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
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
	time.Sleep(50 * time.Millisecond)

	publishIdentity(t, b, "did:plc:any", 1)

	frame := readOneFrame(t, ctx, conn)
	var got map[string]any
	require.NoError(t, json.Unmarshal(frame, &got))
	require.Equal(t, "identity", got["kind"])
}

func TestHandler_Filter_IdentityRespectsDIDFilter(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
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
	time.Sleep(50 * time.Millisecond)

	publishIdentity(t, b, "did:plc:other", 1)
	publishIdentity(t, b, "did:plc:want", 2)

	frame := readOneFrame(t, ctx, conn)
	var got map[string]any
	require.NoError(t, json.Unmarshal(frame, &got))
	require.Equal(t, "did:plc:want", got["did"])
}

func TestHandler_Filter_MaxMessageSize_DropsOversize(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
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
	time.Sleep(50 * time.Millisecond)

	// Identity events are tiny; one should fit. Use them as the
	// "delivered" half of this test rather than constructing oversize
	// commits (which require valid CBOR + CID).
	publishOversizeCommit(t, b, "did:plc:big", "app.bsky.feed.post", 1)
	publishIdentity(t, b, "did:plc:small", 2)

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
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
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
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
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
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
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
	time.Sleep(50 * time.Millisecond)

	// Narrow to likes only.
	update := SubscriberSourcedMessage{
		Type: SubMessageTypeOptionsUpdate,
		Payload: jsonMust(t, UpdatePayload{
			WantedCollections: []string{"app.bsky.feed.like"},
		}),
	}
	require.NoError(t, conn.Write(ctx, websocket.MessageText, jsonMust(t, update)))

	// Give the reader goroutine a moment to apply the update.
	time.Sleep(50 * time.Millisecond)

	publishCommit(t, b, "did:plc:abc", "app.bsky.feed.post", 1)
	publishCommit(t, b, "did:plc:abc", "app.bsky.feed.like", 2)

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
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
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
	time.Sleep(50 * time.Millisecond)

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
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
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
	time.Sleep(50 * time.Millisecond)

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
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
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
	time.Sleep(50 * time.Millisecond)

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
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
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
	time.Sleep(50 * time.Millisecond)

	// V1 PARITY: unknown message types are logged and ignored, not fatal.
	unknown := SubscriberSourcedMessage{
		Type:    "unknown_type",
		Payload: json.RawMessage(`null`),
	}
	require.NoError(t, conn.Write(ctx, websocket.MessageText, jsonMust(t, unknown)))

	// Subsequent events must still flow.
	time.Sleep(50 * time.Millisecond)
	publishIdentity(t, b, "did:plc:still-alive", 1)
	frame := readOneFrame(t, ctx, conn)
	var got map[string]any
	require.NoError(t, json.Unmarshal(frame, &got))
	require.Equal(t, "identity", got["kind"])
}

func TestHandler_RequireHello_BlocksUntilOptionsUpdate(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)

	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
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
	time.Sleep(50 * time.Millisecond)

	// Publish a matching event. Because Subscribe() hasn't been called
	// yet, the broadcaster has no per-connection channel to queue this
	// into — the event must be dropped silently.
	publishIdentity(t, b, "did:plc:pre-hello", 1)

	// Wait long enough to ensure that IF the event were going to be
	// delivered, it would have been (but it won't be, because we
	// haven't sent hello yet).
	time.Sleep(50 * time.Millisecond)

	// Send the hello.
	hello := SubscriberSourcedMessage{
		Type:    SubMessageTypeOptionsUpdate,
		Payload: jsonMust(t, UpdatePayload{}),
	}
	require.NoError(t, conn.Write(ctx, websocket.MessageText, jsonMust(t, hello)))

	// Give the handler time to subscribe.
	time.Sleep(50 * time.Millisecond)

	// Publish a fresh event AFTER hello. Only this one should arrive.
	publishIdentity(t, b, "did:plc:post-hello", 2)

	frame := readOneFrame(t, ctx, conn)
	require.Contains(t, string(frame), "did:plc:post-hello")
	require.NotContains(t, string(frame), "did:plc:pre-hello",
		"pre-hello publish must be dropped, not queued")
}

// V1 PARITY: a filter delivered in the hello options_update must take
// effect before any events flow. This is the load-bearing reason
// requireHello exists — clients use it to install their filter before
// the firehose opens, avoiding a window of unfiltered traffic.
func TestHandler_RequireHello_FilterFromHelloApplies(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)

	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
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
	time.Sleep(50 * time.Millisecond)

	// Publish an event that the filter should drop, then one it should
	// pass. The reader can only see the second one if the filter was
	// installed before Subscribe — which is the contract.
	publishIdentity(t, b, "did:plc:other", 1)
	publishIdentity(t, b, "did:plc:wanted", 2)

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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := newSteadyStateStore(t)
			b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
			require.NoError(t, err)

			h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := newSteadyStateStore(t)
			b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
			require.NoError(t, err)

			h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
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
			time.Sleep(50 * time.Millisecond)

			// No hello sent. Publish and expect immediate delivery.
			publishIdentity(t, b, "did:plc:no-hello-needed", 1)

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
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)

	// Wrap the real handler so we can observe ServeHTTP returning. That
	// signal is deterministic: serve() returning means its deferred
	// conn.CloseNow ran, which unblocks the reader goroutine's conn.Read.
	// We avoid runtime.NumGoroutine() because t.Parallel tests in the
	// same binary spawn/retire goroutines independently and would race
	// the global counter.
	inner := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
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
	time.Sleep(50 * time.Millisecond)

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
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)

	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
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

	// Give the handler time to process all three and Subscribe.
	time.Sleep(100 * time.Millisecond)

	// Confirm normal flow works after the chatty start.
	publishIdentity(t, b, "did:plc:still-flowing", 1)
	frame := readOneFrame(t, ctx, conn)
	require.Contains(t, string(frame), "did:plc:still-flowing")
}

package subscribe

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

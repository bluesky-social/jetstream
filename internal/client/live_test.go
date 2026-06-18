package client

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

// scriptedConn replays a fixed sequence of read results. A readStep is either
// a text frame (data) or an error. When the script is exhausted it returns
// errScriptEOF so the consumer treats the session as ended.
type readStep struct {
	data []byte
	err  error
}

var errScriptEOF = errors.New("script eof")

type scriptedConn struct {
	mu    sync.Mutex
	steps []readStep
	pos   int
	limit int64
}

func (c *scriptedConn) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pos >= len(c.steps) {
		return 0, nil, errScriptEOF
	}
	step := c.steps[c.pos]
	c.pos++
	if step.err != nil {
		return 0, nil, step.err
	}
	return websocket.MessageText, step.data, nil
}

func (c *scriptedConn) Close(websocket.StatusCode, string) error { return nil }
func (c *scriptedConn) SetReadLimit(limit int64)                 { c.limit = limit }

// scriptedDialer returns a dialer that hands out the given conns in order,
// one per dial (reconnect). After the last, it returns the final conn again so
// a reconnect loop has something to read (which will EOF and back off).
func scriptedDialer(conns ...*scriptedConn) (dialFunc, *int) {
	var dials int
	var mu sync.Mutex
	return func(ctx context.Context, _ string) (wsConn, error) {
		mu.Lock()
		defer mu.Unlock()
		i := dials
		dials++
		if i >= len(conns) {
			i = len(conns) - 1
		}
		return conns[i], nil
	}, &dials
}

// runConsumer runs a consumer until it has emitted wantEvents events, then
// cancels. Returns the emitted events (errors are recorded separately).
func runConsumer(t *testing.T, cfg liveConfig, wantEvents int) ([]Event, []error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		mu     sync.Mutex
		events []Event
		errs   []error
	)
	c := newLiveConsumer(cfg)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Run(ctx, func(ev *Event, _ []byte, err error) bool {
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return true
			}
			events = append(events, *ev)
			if len(events) >= wantEvents {
				cancel()
				return false
			}
			return true
		})
	}()
	<-done
	return events, errs
}

func TestLiveConsumerDeliversInOrder(t *testing.T) {
	t.Parallel()
	conn := &scriptedConn{steps: []readStep{
		{data: liveCommitFrame(t, 1, "did:plc:a", "create", "app.bsky.feed.post", "r1", true)},
		{data: liveCommitFrame(t, 2, "did:plc:a", "create", "app.bsky.feed.post", "r2", true)},
		{data: liveCommitFrame(t, 3, "did:plc:b", "create", "app.bsky.feed.post", "r3", true)},
	}}
	dial, _ := scriptedDialer(conn)

	events, _ := runConsumer(t, liveConfig{host: "https://h", dial: dial}, 3)
	require.Equal(t, []uint64{1, 2, 3}, seqs(events))
}

func TestLiveConsumerDedupsReconnectOverlap(t *testing.T) {
	t.Parallel()
	// First connection delivers 1,2,3 then errors. Reconnect re-delivers 2,3
	// (at-least-once overlap) and adds 4,5. The consumer must dedup 2,3.
	first := &scriptedConn{steps: []readStep{
		{data: liveCommitFrame(t, 1, "did:plc:a", "create", "c", "r1", true)},
		{data: liveCommitFrame(t, 2, "did:plc:a", "create", "c", "r2", true)},
		{data: liveCommitFrame(t, 3, "did:plc:a", "create", "c", "r3", true)},
		{err: errors.New("connection reset")},
	}}
	second := &scriptedConn{steps: []readStep{
		{data: liveCommitFrame(t, 2, "did:plc:a", "create", "c", "r2", true)},
		{data: liveCommitFrame(t, 3, "did:plc:a", "create", "c", "r3", true)},
		{data: liveCommitFrame(t, 4, "did:plc:a", "create", "c", "r4", true)},
		{data: liveCommitFrame(t, 5, "did:plc:a", "create", "c", "r5", true)},
	}}
	dial, dials := scriptedDialer(first, second)

	events, _ := runConsumer(t, liveConfig{host: "https://h", dial: dial}, 5)
	require.Equal(t, []uint64{1, 2, 3, 4, 5}, seqs(events), "reconnect overlap must be deduped")
	require.GreaterOrEqual(t, *dials, 2, "must have reconnected")
}

func TestLiveConsumerSkipsControlAndMalformed(t *testing.T) {
	t.Parallel()
	conn := &scriptedConn{steps: []readStep{
		{data: []byte(`{"kind":"heartbeat","seq":0}`)},                        // skipped
		{data: liveCommitFrame(t, 1, "did:plc:a", "create", "c", "r1", true)}, // emit
		{data: []byte(`{not valid json`)},                                     // malformed -> error, keep going
		{data: liveCommitFrame(t, 2, "did:plc:a", "create", "c", "r2", true)}, // emit
	}}
	dial, _ := scriptedDialer(conn)

	events, errs := runConsumer(t, liveConfig{host: "https://h", dial: dial}, 2)
	require.Equal(t, []uint64{1, 2}, seqs(events))
	require.NotEmpty(t, errs, "malformed frame must surface an error")
	for _, e := range errs {
		require.NotContains(t, e.Error(), "reconnecting", "malformed frame must not drop the connection")
	}
}

func TestLiveConsumerSubscribeURL(t *testing.T) {
	t.Parallel()
	c := newLiveConsumer(liveConfig{host: "https://jetstream.example", cursor: 123})
	u := c.subscribeURL()
	require.True(t, strings.HasPrefix(u, "wss://jetstream.example/subscribe-v2?"), "got %s", u)
	require.Contains(t, u, "extended=true")
	require.Contains(t, u, "cursor=123")

	c2 := newLiveConsumer(liveConfig{host: "http://localhost:8080"})
	u2 := c2.subscribeURL()
	require.True(t, strings.HasPrefix(u2, "ws://localhost:8080/subscribe-v2?"), "got %s", u2)
	require.Contains(t, u2, "extended=true")
	require.NotContains(t, u2, "cursor=", "no cursor when zero")
}

func TestLiveConsumerContextCancelCleanStop(t *testing.T) {
	t.Parallel()
	conn := &scriptedConn{steps: []readStep{
		{data: liveCommitFrame(t, 1, "did:plc:a", "create", "c", "r1", true)},
	}}
	dial, _ := scriptedDialer(conn)

	ctx, cancel := context.WithCancel(context.Background())
	c := newLiveConsumer(liveConfig{host: "https://h", dial: dial})
	var got int
	errCh := make(chan error, 1)
	go func() {
		errCh <- c.Run(ctx, func(ev *Event, _ []byte, err error) bool {
			if err == nil && ev != nil {
				got++
				cancel()
			}
			return true
		})
	}()
	require.NoError(t, <-errCh, "context cancel is a clean stop")
	require.Equal(t, 1, got)
}

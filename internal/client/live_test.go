package client

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jcalabro/gt"
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
	// Tiny reconnect backoff so overlap/reconnect tests don't wait real time.
	if cfg.backoffMin == 0 {
		cfg.backoffMin = time.Millisecond
	}
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
	// Safety timeout: if the consumer never reaches wantEvents (e.g. a regression
	// silently drops an event), cancel and return what we have so the caller's
	// assertion fails legibly instead of the goroutine blocking until go test's
	// package-level panic.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		cancel()
		<-done
	}
	mu.Lock()
	defer mu.Unlock()
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

// TestLiveConsumerDeliversSeqZero guards against the 0-based-seq-space drop
// (the live analog of #111): the seq space starts at 0, so a from-tip stream
// against a fresh archive can legitimately deliver seq 0 as its first event.
// The dedup must not treat the zero-initialized lastSeq as "already delivered
// seq 0" and swallow it.
func TestLiveConsumerDeliversSeqZero(t *testing.T) {
	t.Parallel()
	conn := &scriptedConn{steps: []readStep{
		{data: liveCommitFrame(t, 0, "did:plc:a", "create", "c", "r0", true)},
		{data: liveCommitFrame(t, 1, "did:plc:a", "create", "c", "r1", true)},
	}}
	dial, _ := scriptedDialer(conn)

	// cursor=0, no explicitCursor: pure live-from-tip start.
	events, _ := runConsumer(t, liveConfig{host: "https://h", dial: dial}, 2)
	require.Equal(t, []uint64{0, 1}, seqs(events), "seq 0 must not be swallowed by the zero-initialized dedup cursor")
}

// TestLiveConsumerReconnectResumesFromSeqZero pins the interaction between the
// seq-0 fix and the reconnect-resume fix: after delivering seq 0 on a from-tip
// stream, a reconnect must resume from cursor=0 (an established resume point),
// NOT re-anchor at the tip by omitting the cursor.
func TestLiveConsumerReconnectResumesFromSeqZero(t *testing.T) {
	t.Parallel()
	first := &scriptedConn{steps: []readStep{
		{data: liveCommitFrame(t, 0, "did:plc:a", "create", "c", "r0", true)},
		{err: errors.New("connection reset")},
	}}
	second := &scriptedConn{steps: []readStep{
		{data: liveCommitFrame(t, 1, "did:plc:a", "create", "c", "r1", true)},
	}}
	var (
		mu   sync.Mutex
		urls []string
	)
	dial := capturingDialer(&urls, &mu, first, second)

	events, _ := runConsumer(t, liveConfig{host: "https://h", dial: dial}, 2)
	require.Equal(t, []uint64{0, 1}, seqs(events))

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(urls), 2, "must have reconnected")
	require.NotContains(t, urls[0], "cursor=", "initial from-tip dial omits cursor; got %s", urls[0])
	require.Contains(t, urls[1], "cursor=0", "reconnect after delivering seq 0 must resume from cursor=0, not re-anchor at tip; got %s", urls[1])
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
	c := newLiveConsumer(liveConfig{host: "https://jetstream.example", cursor: gt.Some[uint64](123)})
	u := c.subscribeURL()
	require.True(t, strings.HasPrefix(u, "wss://jetstream.example/subscribe-v2?"), "got %s", u)
	require.Contains(t, u, "extended=true")
	require.Contains(t, u, "cursor=123")

	c2 := newLiveConsumer(liveConfig{host: "http://localhost:8080"})
	u2 := c2.subscribeURL()
	require.True(t, strings.HasPrefix(u2, "ws://localhost:8080/subscribe-v2?"), "got %s", u2)
	require.Contains(t, u2, "extended=true")
	require.NotContains(t, u2, "cursor=", "no cursor when none (live from tip)")
}

// TestLiveConsumerSubscribeURLCursorZero guards #112: a backfill->live cutover
// whose rewind start lands at seq 0 (sealed tip below the rewind margin) must
// REPLAY from seq 0, not live-tail from the tip. A present cursor of Some(0)
// sends cursor=0 onto the wire (which the server resolves as a seq replay from
// the start), while a None cursor stays the "live from tip" sentinel. Without
// the Some(0) distinction the entire (plannedThroughSeq, tip] band is dropped.
func TestLiveConsumerSubscribeURLCursorZero(t *testing.T) {
	t.Parallel()
	c := newLiveConsumer(liveConfig{host: "https://h", cursor: gt.Some[uint64](0)})
	u := c.subscribeURL()
	require.Contains(t, u, "cursor=0", "Some(0) must send cursor=0 for a replay from the start; got %s", u)

	// None cursor keeps the "live from tip" sentinel.
	c2 := newLiveConsumer(liveConfig{host: "https://h"})
	require.NotContains(t, c2.subscribeURL(), "cursor=", "None cursor must remain live-from-tip")
}

// capturingDialer wraps scriptedDialer to record the URL requested on each
// dial, so a test can assert that reconnects advance the resume cursor.
func capturingDialer(urls *[]string, mu *sync.Mutex, conns ...*scriptedConn) dialFunc {
	inner, _ := scriptedDialer(conns...)
	return func(ctx context.Context, u string) (wsConn, error) {
		mu.Lock()
		*urls = append(*urls, u)
		mu.Unlock()
		return inner(ctx, u)
	}
}

// TestLiveConsumerReconnectResumesFromLastSeq guards against a regression where
// reconnects rebuilt the subscribe URL from the immutable initial cursor rather
// than from lastSeq. On a live-from-tip stream (cursor=0) that would re-anchor
// each reconnect at the new tip and silently drop the events produced during
// the disconnect window. The reconnect must request cursor=<highest delivered>.
func TestLiveConsumerReconnectResumesFromLastSeq(t *testing.T) {
	t.Parallel()
	// Live-from-tip start (cursor=0): first session delivers 1,2,3 then errors.
	first := &scriptedConn{steps: []readStep{
		{data: liveCommitFrame(t, 1, "did:plc:a", "create", "c", "r1", true)},
		{data: liveCommitFrame(t, 2, "did:plc:a", "create", "c", "r2", true)},
		{data: liveCommitFrame(t, 3, "did:plc:a", "create", "c", "r3", true)},
		{err: errors.New("connection reset")},
	}}
	second := &scriptedConn{steps: []readStep{
		{data: liveCommitFrame(t, 4, "did:plc:a", "create", "c", "r4", true)},
	}}

	var (
		mu   sync.Mutex
		urls []string
	)
	dial := capturingDialer(&urls, &mu, first, second)

	events, _ := runConsumer(t, liveConfig{host: "https://h", dial: dial}, 4)
	require.Equal(t, []uint64{1, 2, 3, 4}, seqs(events))

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(urls), 2, "must have reconnected")
	// First dial is live-from-tip: no cursor.
	require.NotContains(t, urls[0], "cursor=", "initial live-from-tip dial must omit cursor; got %s", urls[0])
	// The reconnect must resume from the highest seq delivered (3), not re-anchor
	// at the tip (which would omit the cursor and drop events during downtime).
	require.Contains(t, urls[1], "cursor=3", "reconnect must resume from lastSeq; got %s", urls[1])
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

package client

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

// scriptedConn replays a fixed sequence of read results. A readStep is either
// a text frame (data) or an error. When the script is exhausted it returns
// errScriptEOF so the consumer treats the session as ended.
type readStep struct {
	data []byte
	err  error
	// msgType overrides the frame type; zero value means MessageText.
	msgType websocket.MessageType
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
	mt := step.msgType
	if mt == 0 {
		mt = websocket.MessageText
	}
	return mt, step.data, nil
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
		_ = c.Run(ctx, func(ev *Event, err error) bool {
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

// TestLiveAccountDeleteDeliveredDespiteCollectionFilter guards that, with no
// client-side suppression, the cutover live tail still DELIVERS an account-delete
// under a collection filter (it carries no collection and bypasses the filter via
// the engine's wantsLive matcher). This is the consumer's only signal to purge a
// dead account's records, so it must not be dropped — while a non-matching commit
// still is.
func TestLiveAccountDeleteDeliveredDespiteCollectionFilter(t *testing.T) {
	t.Parallel()

	e := &Engine{matcher: NewMatcher(PlanRequest{Collections: []string{"app.bsky.feed.post"}})}

	acctDel, err := decodeLiveFrame(liveAccountFrame(100, "did:plc:a", false, "deleted"), recordDecodeMode{})
	require.NoError(t, err)
	require.True(t, e.wantsLive(&acctDel), "account-delete must be delivered under a collection filter")

	likeCreate, err := decodeLiveFrame(liveCommitFrame(t, 101, "did:plc:a", "create", "app.bsky.feed.like", "r1", true), recordDecodeMode{})
	require.NoError(t, err)
	require.False(t, e.wantsLive(&likeCreate), "non-matching commit must be dropped")
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

// TestLiveConsumerDeliversFirstEvent guards that a from-tip stream delivers the
// first-ever event (seq 1) and does not swallow it against the zero-initialized
// dedup floor (0 means "nothing delivered yet" under 1-based seqs).
func TestLiveConsumerDeliversFirstEvent(t *testing.T) {
	t.Parallel()
	conn := &scriptedConn{steps: []readStep{
		{data: liveCommitFrame(t, 1, "did:plc:a", "create", "c", "r1", true)},
		{data: liveCommitFrame(t, 2, "did:plc:a", "create", "c", "r2", true)},
	}}
	dial, _ := scriptedDialer(conn)

	// fromTip: pure live-from-tip start.
	events, _ := runConsumer(t, liveConfig{host: "https://h", dial: dial, fromTip: true}, 2)
	require.Equal(t, []uint64{1, 2}, seqs(events), "the first real event (seq 1) must not be swallowed by the zero-initialized dedup floor")
}

// TestLiveConsumerReconnectResumesFromFirstSeq pins the reconnect-resume fix:
// after delivering seq 1 on a from-tip stream, a reconnect must resume from
// cursor=1 (an established resume point), NOT re-anchor at the tip by omitting
// the cursor.
func TestLiveConsumerReconnectResumesFromFirstSeq(t *testing.T) {
	t.Parallel()
	first := &scriptedConn{steps: []readStep{
		{data: liveCommitFrame(t, 1, "did:plc:a", "create", "c", "r1", true)},
		{err: errors.New("connection reset")},
	}}
	second := &scriptedConn{steps: []readStep{
		{data: liveCommitFrame(t, 2, "did:plc:a", "create", "c", "r2", true)},
	}}
	var (
		mu   sync.Mutex
		urls []string
	)
	dial := capturingDialer(&urls, &mu, first, second)

	events, _ := runConsumer(t, liveConfig{host: "https://h", dial: dial, fromTip: true}, 2)
	require.Equal(t, []uint64{1, 2}, seqs(events))

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(urls), 2, "must have reconnected")
	require.NotContains(t, urls[0], "cursor=", "initial from-tip dial omits cursor; got %s", urls[0])
	require.Contains(t, urls[1], "cursor=1", "reconnect after delivering seq 1 must resume from cursor=1, not re-anchor at tip; got %s", urls[1])
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
	require.NotContains(t, u, "extended=", "the removed extended param must not be sent")
	require.Contains(t, u, "cursor=123")

	c2 := newLiveConsumer(liveConfig{host: "http://localhost:8080", fromTip: true})
	u2 := c2.subscribeURL()
	require.True(t, strings.HasPrefix(u2, "ws://localhost:8080/subscribe-v2"), "got %s", u2)
	require.NotContains(t, u2, "cursor=", "no cursor when fromTip (live from tip)")
}

// TestLiveConsumerSubscribeURLForwardsFilters verifies the live tail forwards
// the caller's collection/DID filters on the wire as wantedCollections/
// wantedDids. The server filters server-side (v1 ParseQuery), so forwarding
// them avoids pulling the full firehose over the socket and discarding most of
// it client-side. Each value is sent as its own repeated query param (the v1
// wire shape ParseQuery expects). The client-side matcher remains a backstop.
func TestLiveConsumerSubscribeURLForwardsFilters(t *testing.T) {
	t.Parallel()
	c := newLiveConsumer(liveConfig{
		host:        "https://h",
		collections: []string{"app.bsky.feed.post", "app.bsky.feed.like"},
		dids:        []string{"did:plc:a", "did:plc:b"},
	})
	u := c.subscribeURL()
	parsed, err := url.Parse(u)
	require.NoError(t, err)
	q := parsed.Query()
	require.Equal(t, []string{"app.bsky.feed.post", "app.bsky.feed.like"}, q["wantedCollections"],
		"each collection must be a repeated wantedCollections param; got %s", u)
	require.Equal(t, []string{"did:plc:a", "did:plc:b"}, q["wantedDids"],
		"each DID must be a repeated wantedDids param; got %s", u)

	// No filters -> no params (unfiltered tail unaffected).
	c2 := newLiveConsumer(liveConfig{host: "https://h"})
	u2 := c2.subscribeURL()
	require.NotContains(t, u2, "wantedCollections", "no collection filter -> no param")
	require.NotContains(t, u2, "wantedDids", "no DID filter -> no param")
}

// TestLiveConsumerSubscribeURLCursorZero guards the bufferless cutover's
// empty-archive case: when the sealed archive is empty the cutover seq is 0, so
// the live consumer connects with cursor=0, which must REPLAY from the first
// event, not live-tail from the tip. A cursor of 0 with fromTip unset sends
// cursor=0 onto the wire (the server resolves it as a replay from the start);
// fromTip is the distinct WithLiveCursor(0) "live from tip" contract that omits
// the param. Without sending cursor=0 the first events on a from-empty archive
// would be skipped.
func TestLiveConsumerSubscribeURLCursorZero(t *testing.T) {
	t.Parallel()
	c := newLiveConsumer(liveConfig{host: "https://h", cursor: 0})
	u := c.subscribeURL()
	require.Contains(t, u, "cursor=0", "cursor=0 (not fromTip) must replay from the start; got %s", u)

	// fromTip keeps the "live from tip" sentinel (no cursor param).
	c2 := newLiveConsumer(liveConfig{host: "https://h", fromTip: true})
	require.NotContains(t, c2.subscribeURL(), "cursor=", "fromTip must remain live-from-tip")
}

// TestLiveDialOptionsDoNotOfferDeflate pins the #294 contract: the v2
// dialer must NOT offer RFC 7692 permessage-deflate — the server never
// negotiates it on /subscribe-v2 (per-connection deflate is the dominant
// server cost at fanout scale). Compression on v2 is the dict-zstd
// scheme, negotiated via ?zstdDictionary=<id>, not a websocket extension.
func TestLiveDialOptionsDoNotOfferDeflate(t *testing.T) {
	t.Parallel()
	opts := liveDialOptions(nil)
	require.NotNil(t, opts)
	require.Equal(t, websocket.CompressionDisabled, opts.CompressionMode,
		"v2 live dial must not offer permessage-deflate")
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
	// Live-from-tip start (fromTip): first session delivers 1,2,3 then errors.
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

	events, _ := runConsumer(t, liveConfig{host: "https://h", dial: dial, fromTip: true}, 4)
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

// TestLiveConsumerCursorTooOldIsTerminal pins the §14 handoff signal: a dial
// that fails with errLiveCursorTooOld must end Run with that error (NOT a clean
// nil and NOT a reconnect loop), so the cutover engine re-enters the backfill
// loop rather than churning reconnects against a cursor the server keeps
// rejecting. The dialer fails every attempt; if Run reconnect-looped the test
// would hang past its deadline.
func TestLiveConsumerCursorTooOldIsTerminal(t *testing.T) {
	t.Parallel()
	var dials atomic.Int64
	dial := func(ctx context.Context, _ string) (wsConn, error) {
		dials.Add(1)
		return nil, errLiveCursorTooOld
	}
	c := newLiveConsumer(liveConfig{host: "https://h", dial: dial, backoffMin: time.Millisecond})

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.Run(context.Background(), func(*Event, error) bool { return true })
	}()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, errLiveCursorTooOld, "a too-old dial must end Run terminally")
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return on a too-old dial (reconnect-looping?)")
	}
	require.LessOrEqual(t, dials.Load(), int64(2), "must not reconnect-loop on a terminal too-old cursor")
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
		errCh <- c.Run(ctx, func(ev *Event, err error) bool {
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

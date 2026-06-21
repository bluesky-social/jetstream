package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"time"

	"github.com/coder/websocket"
	"github.com/jcalabro/gt"
)

const (
	// defaultLiveReadLimit bounds a single websocket message. Extended frames
	// carry base64 record CBOR; a generous ceiling tolerates large records
	// without allowing an unbounded allocation from a hostile server.
	defaultLiveReadLimit = 32 << 20 // 32 MiB

	// reconnect backoff bounds.
	liveBackoffMin = 250 * time.Millisecond
	liveBackoffMax = 30 * time.Second
)

// wsConn is the subset of *websocket.Conn the consumer uses, extracted so
// tests can substitute a fake transport without a real socket.
type wsConn interface {
	Read(ctx context.Context) (websocket.MessageType, []byte, error)
	Close(code websocket.StatusCode, reason string) error
	SetReadLimit(limit int64)
}

// dialFunc establishes a live websocket connection to url. Tests inject a
// fake; production uses dialWebsocket.
type dialFunc func(ctx context.Context, url string) (wsConn, error)

// liveConfig configures a liveConsumer.
type liveConfig struct {
	host string // normalized base URL, e.g. "https://host"
	// cursor is the initial WIRE resume point — the value sent as ?cursor= on
	// the first connection — as an explicit optional so the seq space's 0
	// sentinel is never overloaded:
	//
	//   - None      -> "live from the current tip"; the cursor param is omitted.
	//   - Some(seq) -> replay/resume from seq; the param is always sent, including
	//     Some(0) (the #112 cutover from a sealed tip below the rewind margin, and
	//     the empty-archive cutover, both of which must replay from the start
	//     rather than anchor at the tip).
	//
	// The server replays inclusively (it delivers seq >= cursor; see
	// internal/subscribe/replay.go); the consumer's own seq dedup turns that into
	// the effective "> last delivered" on resume.
	cursor gt.Option[uint64]
	// dedupFloor is the initial value of lastSeq, the seq dedup floor. It is kept
	// SEPARATE from cursor because the wire resume point and the dedup floor are
	// not always the same value:
	//
	//   - On a resume/replay where the caller already HOLDS the events up to seq
	//     (a saved resume cursor, or the #112 small-tip cutover where the backfill
	//     emitted through the sealed tip), dedupFloor == cursor == Some(seq): the
	//     at-least-once re-delivery of seq itself is dropped.
	//   - On an EMPTY-archive cutover the backfill covered NOTHING, so the live
	//     tail owns the whole stream from the start: the wire cursor is Some(0)
	//     (replay from seq 0) but dedupFloor is None (nothing delivered yet), so
	//     the genuine first-ever event at seq 0 passes the dedup instead of being
	//     swallowed against a Some(0) floor it never actually delivered.
	//
	// None means "nothing delivered yet" — the first event, even seq 0, passes.
	dedupFloor gt.Option[uint64]
	readLimit  int64
	dial       dialFunc
	logger     *slog.Logger
	// backoffMin/backoffMax override the reconnect backoff bounds. Zero uses
	// the package defaults. Tests set tiny values to avoid real-time waits.
	backoffMin time.Duration
	backoffMax time.Duration
}

func (c liveConfig) minBackoff() time.Duration {
	if c.backoffMin > 0 {
		return c.backoffMin
	}
	return liveBackoffMin
}

func (c liveConfig) maxBackoff() time.Duration {
	if c.backoffMax > 0 {
		return c.backoffMax
	}
	return liveBackoffMax
}

// liveConsumer tails /subscribe-v2 in extended mode, decoding frames into
// engine events, deduplicating the at-least-once overlap by seq, and
// reconnecting with bounded exponential backoff. It is the live half of the
// stream: the engine consumes its output during cutover (buffered) and in
// steady state (direct).
type liveConsumer struct {
	cfg liveConfig
	// lastSeq is the highest seq delivered, used both as the dedup floor and the
	// reconnect resume cursor. It is an optional because the seq space is 0-based
	// (a fresh archive assigns the first event seq 0): None means "nothing
	// delivered yet" and Some(0) means "seq 0 has been delivered". Collapsing the
	// two onto a bare 0 would make the dedup (ev.Seq <= lastSeq) swallow the
	// first-ever event and make a reconnect re-anchor at the tip.
	lastSeq gt.Option[uint64]
}

func newLiveConsumer(cfg liveConfig) *liveConsumer {
	if cfg.readLimit <= 0 {
		cfg.readLimit = defaultLiveReadLimit
	}
	if cfg.dial == nil {
		cfg.dial = dialWebsocket
	}
	if cfg.logger == nil {
		cfg.logger = slog.New(slog.NewTextHandler(discardWriter{}, nil))
	}
	// Seed lastSeq (the dedup floor) from dedupFloor, NOT from the wire cursor:
	// the two diverge in the empty-archive cutover, where the wire cursor is
	// Some(0) (replay from the start) but dedupFloor is None (the live tail
	// delivers the first-ever event, so nothing has been delivered yet). A None
	// dedupFloor leaves lastSeq None so the first event delivered — even seq 0 —
	// passes the dedup. A Some(seq) dedupFloor (a genuine resume, or the #112
	// small-tip cutover where the backfill already emitted through seq) drops the
	// at-least-once re-delivery of seq itself. See liveConfig.dedupFloor.
	return &liveConsumer{cfg: cfg, lastSeq: cfg.dedupFloor}
}

// Run tails the live stream until ctx is cancelled, invoking emit for each
// decoded event in delivery order. emit returning false stops the consumer.
// Recoverable read/dial failures trigger a reconnect with backoff (reported to
// emit as a non-nil error with a nil event so the caller can observe churn);
// a context cancellation is a clean stop and returns nil.
func (c *liveConsumer) Run(ctx context.Context, emit func(*Event, []byte, error) bool) error {
	minB, maxB := c.cfg.minBackoff(), c.cfg.maxBackoff()
	backoff := minB
	for {
		if ctx.Err() != nil {
			return nil //nolint:nilerr // context cancellation is a clean shutdown, not an error
		}
		seqBefore := c.lastSeq
		err := c.session(ctx, emit)
		if ctx.Err() != nil {
			return nil //nolint:nilerr // ctx cancelled mid-session: clean shutdown, the session err is incidental
		}
		if errors.Is(err, errEmitStop) {
			return nil
		}
		// A session that made progress (delivered new events) is healthy; reset
		// backoff so a long-lived connection that finally drops reconnects
		// promptly rather than at the accumulated max. lastSeq advances
		// monotonically (None -> Some, then strictly increasing), so any change
		// means the session delivered at least one new event.
		if c.lastSeq != seqBefore {
			backoff = minB
		}
		// Report the disconnect and back off before reconnecting.
		if err != nil && !emit(nil, nil, fmt.Errorf("jetstream: live tail reconnecting: %w", err)) {
			return nil
		}
		if !sleep(ctx, backoff) {
			return nil
		}
		backoff = nextBackoff(backoff, maxB)
	}
}

// errEmitStop unwinds the session loop when the consumer asks to stop.
var errEmitStop = errors.New("jetstream: live emit stop")

// session runs one connection: dial, read-decode-emit until an error or stop.
// A successful read resets the caller's backoff via the return path (nil err).
func (c *liveConsumer) session(ctx context.Context, emit func(*Event, []byte, error) bool) error {
	conn, err := c.cfg.dial(ctx, c.subscribeURL())
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	conn.SetReadLimit(c.cfg.readLimit)
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "client closing") }()

	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		if typ != websocket.MessageText {
			continue // jetstream frames are text JSON; ignore stray binary
		}
		ev, derr := decodeLiveFrame(data)
		if errors.Is(derr, errSkipFrame) {
			continue
		}
		if derr != nil {
			// A malformed data frame is upstream input; surface it but keep the
			// connection (one bad frame must not drop the tail).
			if !emit(nil, nil, derr) {
				return errEmitStop
			}
			continue
		}
		// Deduplicate the at-least-once reconnect overlap: skip anything at or
		// below the highest seq already delivered. While lastSeq is None nothing
		// has been delivered yet, so the first event always passes — this is what
		// lets a from-tip / replay-from-0 start deliver the first-ever event
		// (seq 0) instead of swallowing it against a bare zero floor.
		if c.lastSeq.HasVal() && ev.Seq <= c.lastSeq.Val() {
			continue
		}
		c.lastSeq = gt.Some(ev.Seq)
		evCopy := ev
		// Pass the raw frame too so the cutover buffer can persist verbatim
		// bytes (re-decoded on replay) rather than re-marshal the decoded event.
		if !emit(&evCopy, data, nil) {
			return errEmitStop
		}
	}
}

func (c *liveConsumer) subscribeURL() string {
	u, _ := url.Parse(c.cfg.host) // host is pre-normalized by the caller
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	}
	u.Path = "/subscribe-v2"
	q := url.Values{}
	q.Set("extended", "true")
	// Wire cursor: resume from lastSeq (the highest seq delivered) once anything
	// has been delivered, otherwise from the initial wire cursor (cfg.cursor).
	// subscribeURL is rebuilt on every reconnect, so anchoring each new session
	// at lastSeq is what keeps a live-from-tip stream from re-anchoring at the
	// reconnect-time tip and silently dropping events produced while disconnected.
	// Before any delivery lastSeq is the dedupFloor (which is None in the
	// empty-archive cutover), so we fall back to cfg.cursor to send the intended
	// replay-from-start cursor=0 rather than omitting the param (= live from tip)
	// and dropping the whole stream. The optional carries presence directly:
	// Some(seq) sends cursor=seq (a resume point, a replay-from-0 cutover, or any
	// delivered event including seq 0); None omits the param = "live from tip".
	wireCursor := c.lastSeq
	if !wireCursor.HasVal() {
		wireCursor = c.cfg.cursor
	}
	if wireCursor.HasVal() {
		q.Set("cursor", strconv.FormatUint(wireCursor.Val(), 10))
	}
	u.RawQuery = q.Encode()
	return u.String()
}

// dialWebsocket is the production dialer.
func dialWebsocket(ctx context.Context, rawURL string) (wsConn, error) {
	conn, resp, err := websocket.Dial(ctx, rawURL, nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func nextBackoff(d, maxB time.Duration) time.Duration {
	d *= 2
	if d > maxB {
		return maxB
	}
	return d
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

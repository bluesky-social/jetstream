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
	// cursor is the initial resume point. The server replays inclusively (it
	// delivers seq >= cursor; see internal/subscribe/replay.go); the consumer's
	// own seq dedup turns that into the effective "> last delivered" on resume.
	cursor uint64
	// explicitCursor forces the cursor onto the wire even when it is 0. A bare
	// cursor=0 (omitted param) is the "live from the current tip" sentinel; the
	// backfill->live cutover sets this so a rewind start of 0 (sealed tip below
	// the rewind margin) REPLAYS from seq 0 instead of anchoring at the tip and
	// dropping the (plannedThroughSeq, tip] band. See #112.
	explicitCursor bool
	readLimit      int64
	dial           dialFunc
	logger         *slog.Logger
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
	cfg     liveConfig
	lastSeq uint64 // highest seq delivered; the reconnect resume cursor
	// haveLastSeq distinguishes "lastSeq holds a real delivered/resume seq" from
	// "lastSeq is the zero default". The seq space is 0-based (a fresh archive
	// assigns the first event seq 0), so lastSeq==0 is ambiguous: it can mean
	// "nothing delivered yet" OR "seq 0 has been delivered/already-held". Without
	// this flag the dedup (ev.Seq <= lastSeq) would unconditionally swallow the
	// first-ever event (seq 0). Mirrors the afterSeq>0 gate from #111.
	haveLastSeq bool
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
	// A non-zero resume cursor means the caller already holds events up to it, so
	// seed lastSeq as an established resume point to dedup the at-least-once
	// re-delivery of cursor itself. A zero cursor (live-from-tip, or the #112
	// explicit replay-from-0 cutover) is NOT an established point: seq 0 has not
	// been delivered, so it must pass the dedup.
	return &liveConsumer{cfg: cfg, lastSeq: cfg.cursor, haveLastSeq: cfg.cursor > 0}
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
		// promptly rather than at the accumulated max.
		if c.lastSeq > seqBefore {
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
		// below the highest seq already delivered. The haveLastSeq guard keeps
		// this from swallowing the first-ever event (seq 0) on a from-tip /
		// replay-from-0 start, where lastSeq==0 means "nothing delivered yet".
		if c.haveLastSeq && ev.Seq <= c.lastSeq {
			continue
		}
		c.lastSeq = ev.Seq
		c.haveLastSeq = true
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
	// Resume from lastSeq (the highest seq delivered), not the immutable initial
	// cursor: subscribeURL is rebuilt on every reconnect, and anchoring each new
	// session at cfg.cursor would, on a live-from-tip stream, re-anchor at the
	// reconnect-time tip and silently drop events produced while disconnected.
	// Gate on haveLastSeq, not lastSeq>0: once seq 0 has been delivered we hold
	// an established resume point (lastSeq==0) and MUST send cursor=0 so the
	// reconnect resumes from 0 instead of re-anchoring at the tip. explicitCursor
	// forces a replay-from-0 cutover (#112) onto the wire before any event has
	// been delivered. A bare from-tip start (no event yet, no explicitCursor)
	// stays omitted = "live from tip".
	if c.haveLastSeq || c.cfg.explicitCursor {
		q.Set("cursor", strconv.FormatUint(c.lastSeq, 10))
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

package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
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
	// cursor is the initial WIRE resume point sent as ?cursor= on the first
	// connection. cursor=0 means "replay from the beginning" (everything, since
	// the first real event is seq 1); a positive value resumes from that seq. The
	// server replays inclusively (it delivers seq >= cursor; see
	// internal/subscribe/replay.go); the consumer's own seq dedup turns that into
	// the effective "> last delivered" on resume. Ignored when fromTip is set.
	cursor uint64
	// fromTip, when true, omits the ?cursor= param so the server starts at the
	// live tip with no replay. This is the WithLiveCursor(0) "live from tip"
	// user-API contract — distinct from cursor=0 ("replay everything"). Only the
	// pure live-only path sets it; the cutover always sends an explicit cursor.
	fromTip bool
	// collections and dids are the caller's filters, forwarded on the wire as
	// repeated wantedCollections/wantedDids query params so the server filters
	// server-side (v1 ParseQuery) rather than streaming the full firehose for
	// the client to discard. Empty means "no filter" (the param is omitted).
	// The client-side matcher remains a correctness backstop.
	collections []string
	dids        []string
	// dedupFloor seeds lastSeq: the highest seq the caller already holds, so the
	// at-least-once re-delivery at or below it is dropped. 0 means "nothing
	// delivered yet", so the first real event (seq >= 1) always passes — the
	// seq-0 swallow is structurally impossible under 1-based seqs (design §R8).
	dedupFloor uint64
	readLimit  int64
	dial       dialFunc
	httpClient *http.Client // optional; routes the live websocket upgrade through a custom transport
	logger     *slog.Logger
	// backoffMin/backoffMax override the reconnect backoff bounds. Zero uses
	// the package defaults. Tests set tiny values to avoid real-time waits.
	backoffMin time.Duration
	backoffMax time.Duration
	// mode selects raw vs. map record materialization for live commits. Zero
	// value = the default map build.
	mode recordDecodeMode
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
	// reconnect resume cursor. 0 means "nothing delivered yet" (the first real
	// event is seq >= 1, so it passes the ev.Seq <= lastSeq dedup); a positive
	// value resumes/dedups above it. seenAny disambiguates a from-tip start
	// (lastSeq 0, nothing delivered → omit the wire cursor on reconnect) from a
	// replay-from-0 start.
	lastSeq uint64
	seenAny bool
}

func newLiveConsumer(cfg liveConfig) *liveConsumer {
	if cfg.readLimit <= 0 {
		cfg.readLimit = defaultLiveReadLimit
	}
	if cfg.dial == nil {
		hc := cfg.httpClient
		cfg.dial = func(ctx context.Context, rawURL string) (wsConn, error) {
			return dialWebsocket(ctx, rawURL, hc)
		}
	}
	if cfg.logger == nil {
		cfg.logger = slog.New(slog.NewTextHandler(discardWriter{}, nil))
	}
	// Seed lastSeq (the dedup floor) from dedupFloor, NOT from the wire cursor:
	// the two diverge in the empty-archive cutover, where the wire cursor is 0
	// (replay from the start) but dedupFloor is 0 meaning "nothing delivered yet"
	// so the first real event (seq >= 1) passes the dedup. A positive dedupFloor
	// (a genuine resume, or a small-tip cutover where the backfill already emitted
	// through seq) drops the at-least-once re-delivery of seq itself. seenAny
	// stays false until the first delivery so reconnect knows whether to omit the
	// wire cursor (from-tip) or resume. See liveConfig.dedupFloor.
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
		// monotonically (strictly increasing on each delivery), so any change
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
		ev, derr := decodeLiveFrame(data, c.cfg.mode)
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
		// below the highest seq already delivered. lastSeq 0 with nothing yet
		// delivered means the first real event (seq >= 1) passes — the seq-0
		// swallow is structurally impossible under 1-based seqs.
		if ev.Seq <= c.lastSeq {
			continue
		}
		c.lastSeq = ev.Seq
		c.seenAny = true
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
	// Wire cursor: once any event has been delivered, resume each new session at
	// lastSeq (the highest seq delivered). subscribeURL is rebuilt on every
	// reconnect, so anchoring at lastSeq is what keeps a stream from re-anchoring
	// at the reconnect-time tip and silently dropping events produced while
	// disconnected. Before any delivery we use the configured start: omit the
	// param when fromTip (the WithLiveCursor(0) "live from tip" contract),
	// otherwise send cursor=cfg.cursor (cursor=0 replays from the beginning).
	switch {
	case c.seenAny:
		q.Set("cursor", strconv.FormatUint(c.lastSeq, 10))
	case !c.cfg.fromTip:
		q.Set("cursor", strconv.FormatUint(c.cfg.cursor, 10))
	}
	// Forward the caller's filters server-side. v1's ParseQuery reads each
	// collection/DID as its own repeated param, so append (not Set) one entry
	// per value. Empty slices add nothing, leaving an unfiltered tail's URL
	// unchanged.
	for _, c := range c.cfg.collections {
		q.Add("wantedCollections", c)
	}
	for _, d := range c.cfg.dids {
		q.Add("wantedDids", d)
	}
	u.RawQuery = q.Encode()
	return u.String()
}

// liveDialOptions builds the websocket DialOptions for the live tail. It
// offers RFC 7692 permessage-deflate with context takeover: the server
// auto-negotiates deflate when the client advertises it, which cuts bandwidth
// substantially on this repetitive JSON firehose. A server that does not
// negotiate it falls back to an uncompressed stream transparently.
func liveDialOptions(hc *http.Client) *websocket.DialOptions {
	return &websocket.DialOptions{
		CompressionMode: websocket.CompressionContextTakeover,
		HTTPClient:      hc, // nil is fine: websocket.Dial falls back to its default
	}
}

// dialWebsocket is the production dialer. hc, when non-nil, routes the HTTP/1.1
// upgrade through a custom transport (e.g. an in-process pipe); nil uses the
// websocket default.
func dialWebsocket(ctx context.Context, rawURL string, hc *http.Client) (wsConn, error) {
	conn, resp, err := websocket.Dial(ctx, rawURL, liveDialOptions(hc))
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

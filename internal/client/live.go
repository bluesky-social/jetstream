package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/bluesky-social/jetstream/internal/zstddict"
	"github.com/coder/websocket"
	"github.com/klauspost/compress/zstd"
)

const (
	// defaultLiveReadLimit bounds a single websocket message. v2 frames
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
	// zstdDict, when non-nil, opts the connection into the /subscribe-v2
	// dict-zstd compression scheme: the dictionary ID (parsed from the
	// blob's header) is sent as ?zstdDictionary=<id> and incoming BINARY
	// frames are decompressed with it. The caller obtains the blob via
	// getZstdDictionary before constructing the consumer. nil = plain
	// uncompressed text frames (v2 never negotiates permessage-deflate).
	zstdDict []byte
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

// liveConsumer tails /subscribe-v2, decoding frames into
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

	// zstd decompression state, set only when cfg.zstdDict is non-nil.
	zstdDictID  uint32
	zstdDecoder *zstd.Decoder
}

// LastSeq returns the highest seq the consumer has delivered, or its seeded
// dedup floor if it delivered nothing. Read it only after Run returns (the
// field is mutated on Run's goroutine); the cutover engine uses it to resume a
// re-backfill from the last durably-processed seq after a too-old 400.
func (c *liveConsumer) LastSeq() uint64 { return c.lastSeq }

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
	var dictID uint32
	var dec *zstd.Decoder
	if cfg.zstdDict != nil {
		id, err := zstddict.ParseID(cfg.zstdDict)
		if err != nil {
			// The blob came from getZstdDictionary moments ago; a parse
			// failure is a server/transport fault, not a reason to crash.
			// Fall back to uncompressed (a documented degradation, logged).
			cfg.logger.Warn("invalid zstd dictionary; falling back to uncompressed live tail", "err", err)
			cfg.zstdDict = nil
		} else {
			d, derr := zstd.NewReader(nil, zstd.WithDecoderDicts(cfg.zstdDict))
			if derr != nil {
				cfg.logger.Warn("zstd decoder construction failed; falling back to uncompressed live tail", "err", derr)
				cfg.zstdDict = nil
			} else {
				dictID = id
				dec = d
			}
		}
	}
	// Seed lastSeq (the dedup floor) from dedupFloor, NOT from the wire cursor:
	// the two diverge in the empty-archive cutover, where the wire cursor is 0
	// (replay from the start) but dedupFloor is 0 meaning "nothing delivered yet"
	// so the first real event (seq >= 1) passes the dedup. A positive dedupFloor
	// (a genuine resume, or a small-tip cutover where the backfill already emitted
	// through seq) drops the at-least-once re-delivery of seq itself. seenAny
	// stays false until the first delivery so reconnect knows whether to omit the
	// wire cursor (from-tip) or resume. See liveConfig.dedupFloor.
	return &liveConsumer{cfg: cfg, lastSeq: cfg.dedupFloor, zstdDictID: dictID, zstdDecoder: dec}
}

// Run tails the live stream until ctx is cancelled, invoking emit for each
// decoded event in delivery order. emit returning false stops the consumer.
// Recoverable read/dial failures trigger a reconnect with backoff (reported to
// emit as a non-nil error with a nil event so the caller can observe churn);
// a context cancellation is a clean stop and returns nil.
func (c *liveConsumer) Run(ctx context.Context, emit func(*Event, error) bool) error {
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
		// A too-old cursor is terminal, not transient: the seq will not become
		// valid by reconnecting (the lookback floor only advances). Return it so
		// the cutover engine re-enters the backfill pagination loop from the last
		// durably-processed seq (design §14 client side) instead of churning
		// reconnects against a cursor the server will keep rejecting. This covers
		// both the terminal handoff connect and a mid-stream fell-off-live drop.
		if errors.Is(err, errLiveCursorTooOld) {
			return err
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
		if err != nil && !emit(nil, fmt.Errorf("jetstream: live tail reconnecting: %w", err)) {
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
func (c *liveConsumer) session(ctx context.Context, emit func(*Event, error) bool) error {
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
		switch {
		case c.zstdDecoder != nil && typ == websocket.MessageBinary:
			// dict-zstd connection: every event frame is a BINARY zstd
			// frame; decompress before decode. DecodeAll enforces the
			// decoder's memory limit, bounding a hostile frame.
			data, err = c.zstdDecoder.DecodeAll(data, nil)
			if err != nil {
				// Upstream input, never crash: surface and keep the tail.
				if !emit(nil, fmt.Errorf("jetstream: zstd frame decode: %w", err)) {
					return errEmitStop
				}
				continue
			}
		case typ != websocket.MessageText:
			continue // jetstream frames are text JSON; ignore stray binary
		}
		ev, derr := decodeLiveFrame(data, c.cfg.mode)
		if errors.Is(derr, errSkipFrame) {
			continue
		}
		if derr != nil {
			// A malformed data frame is upstream input; surface it but keep the
			// connection (one bad frame must not drop the tail).
			if !emit(nil, derr) {
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
		if !emit(&evCopy, nil) {
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
	if c.zstdDecoder != nil {
		q.Set("zstdDictionary", strconv.FormatUint(uint64(c.zstdDictID), 10))
	}
	u.RawQuery = q.Encode()
	return u.String()
}

// liveDialOptions builds the websocket DialOptions for the live tail.
// permessage-deflate is deliberately NOT offered: /subscribe-v2 never
// negotiates it (#294 removed it server-side — per-connection deflate is
// the dominant server cost at fanout scale), so offering it is dead
// weight on the handshake. Compression on v2 is the dict-zstd scheme,
// negotiated at the application layer via ?zstdDictionary=<id>.
func liveDialOptions(hc *http.Client) *websocket.DialOptions {
	return &websocket.DialOptions{
		CompressionMode: websocket.CompressionDisabled,
		HTTPClient:      hc, // nil is fine: websocket.Dial falls back to its default
	}
}

// errLiveCursorTooOld marks a terminal /subscribe-v2 connect refusal: the seq
// cursor resolved below the server's lookback floor and the server returned a
// pre-upgrade HTTP 400 (subscribe §14 / design §14). It is NOT a transient dial
// failure to reconnect-loop on — the cursor will not become valid by retrying.
// The cutover engine catches it and re-enters the backfill pagination loop from
// the last durably-processed seq (design §14 client side). The wrapped message
// carries the server's floor-seq body for observability.
var errLiveCursorTooOld = errors.New("jetstream: live cursor too old")

// cursorTooOldMarker is the substring the server embeds in its pre-upgrade
// "cursor too old" HTTP 400 body, which dialWebsocket matches to recognize a
// too-old refusal. It MUST equal internal/subscribe.CursorTooOldMarker (the
// server's source of truth); the client cannot import that package without
// pulling the server's storage deps into the public module, so the literal is
// duplicated here and pinned equal by TestDialWebsocketMatchesServerTooOld
// (live_subscribe_contract_test.go), which fails CI if either side drifts.
const cursorTooOldMarker = "cursor too old"

// dialWebsocket is the production dialer. hc, when non-nil, routes the HTTP/1.1
// upgrade through a custom transport (e.g. an in-process pipe); nil uses the
// websocket default.
//
// A pre-upgrade HTTP 400 "cursor too old" from /subscribe-v2 is mapped to the
// typed errLiveCursorTooOld so the consumer surfaces it terminally rather than
// reconnect-looping. coder/websocket leaves the first 1024 bytes of the
// response body readable on a non-101 handshake, which carries the floor seq.
func dialWebsocket(ctx context.Context, rawURL string, hc *http.Client) (wsConn, error) {
	conn, resp, err := websocket.Dial(ctx, rawURL, liveDialOptions(hc))
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusBadRequest && resp.Body != nil {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
			_ = resp.Body.Close()
			if strings.Contains(string(body), cursorTooOldMarker) {
				return nil, fmt.Errorf("%w: %s", errLiveCursorTooOld, strings.TrimSpace(string(body)))
			}
		} else if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		return nil, err
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
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

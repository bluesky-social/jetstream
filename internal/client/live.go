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
	host      string // normalized base URL, e.g. "https://host"
	cursor    uint64 // resume point; the server delivers seq > cursor
	readLimit int64
	dial      dialFunc
	logger    *slog.Logger
}

// liveConsumer tails /subscribe-v2 in extended mode, decoding frames into
// engine events, deduplicating the at-least-once overlap by seq, and
// reconnecting with bounded exponential backoff. It is the live half of the
// stream: the engine consumes its output during cutover (buffered) and in
// steady state (direct).
type liveConsumer struct {
	cfg     liveConfig
	lastSeq uint64 // highest seq delivered; the reconnect resume cursor
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
	return &liveConsumer{cfg: cfg, lastSeq: cfg.cursor}
}

// Run tails the live stream until ctx is cancelled, invoking emit for each
// decoded event in delivery order. emit returning false stops the consumer.
// Recoverable read/dial failures trigger a reconnect with backoff (reported to
// emit as a non-nil error with a nil event so the caller can observe churn);
// a context cancellation is a clean stop and returns nil.
func (c *liveConsumer) Run(ctx context.Context, emit func(*Event, error) bool) error {
	backoff := liveBackoffMin
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
			backoff = liveBackoffMin
		}
		// Report the disconnect and back off before reconnecting.
		if err != nil && !emit(nil, fmt.Errorf("jetstream: live tail reconnecting: %w", err)) {
			return nil
		}
		if !sleep(ctx, backoff) {
			return nil
		}
		backoff = nextBackoff(backoff)
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
			if !emit(nil, derr) {
				return errEmitStop
			}
			continue
		}
		// Deduplicate the at-least-once reconnect overlap: skip anything at or
		// below the highest seq already delivered.
		if ev.Seq <= c.lastSeq {
			continue
		}
		c.lastSeq = ev.Seq
		evCopy := ev
		if !emit(&evCopy, nil) {
			return errEmitStop
		}
	}
}

// LastSeq returns the highest seq delivered so far (the live resume cursor).
func (c *liveConsumer) LastSeq() uint64 { return c.lastSeq }

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
	if c.cfg.cursor > 0 {
		q.Set("cursor", strconv.FormatUint(c.cfg.cursor, 10))
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

func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > liveBackoffMax {
		return liveBackoffMax
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

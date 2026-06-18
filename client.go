package jetstream

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
)

// Client is a Jetstream v2 consumer. Construct one with Subscribe. A Client
// drives at most one Events iteration at a time; create separate Clients for
// concurrent streams. Close releases its resources and is safe to call
// concurrently with a running Events (the natural way to stop a live tail) and
// to call more than once.
type Client struct {
	cfg    config
	host   string // normalized base URL, e.g. "https://host"
	engine engine

	// closed reports whether Close has been called. atomic so a concurrent
	// Close can interrupt a running Events without a data race; closeOnce
	// makes Close idempotent and engine.close() run exactly once.
	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

// Batch is a group of events delivered together by the Events iterator. It
// amortizes per-event overhead (notably cursor persistence): handle the whole
// batch, then save LastCursor once.
type Batch struct {
	events []Event
}

// Events returns the events in this batch. The slice is owned by the caller
// for the lifetime of the loop iteration; do not retain it past the next
// iteration without copying.
func (b *Batch) Events() []Event { return b.events }

// LastCursor returns the highest Seq in the batch, suitable for persisting as
// a resume point. Returns 0 for an empty batch.
func (b *Batch) LastCursor() uint64 {
	var max uint64
	for i := range b.events {
		if b.events[i].Seq > max {
			max = b.events[i].Seq
		}
	}
	return max
}

// Subscribe creates a Client for the given Jetstream host. host may be a bare
// hostname ("jetstream.us-west.bsky.network"), a host:port, or a full
// http(s):// URL; the scheme defaults to https.
//
// With no backfill option, the Client live-tails from the current tip (or
// from WithLiveCursor). With WithAfterSeq/WithBeforeSeq it backfills the
// sealed archive and then cuts over to live.
func Subscribe(host string, opts ...Option) (*Client, error) {
	cfg := defaultConfig()
	for i, opt := range opts {
		if opt == nil {
			return nil, fmt.Errorf("jetstream: option %d is nil", i)
		}
		opt(&cfg)
	}
	if cfg.logger == nil {
		cfg.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	base, err := normalizeHost(host)
	if err != nil {
		return nil, err
	}
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}

	eng, err := newEngine(base, cfg)
	if err != nil {
		return nil, err
	}

	return &Client{cfg: cfg, host: base, engine: eng}, nil
}

// Events streams event batches in delivery order until ctx is cancelled or a
// terminal error occurs. It is a Go range-over-func iterator:
//
//	for batch, err := range client.Events(ctx) {
//		...
//	}
//
// A non-nil err is yielded for recoverable problems; iteration continues so
// the caller may log and move on. When ctx is done or the stream ends, the
// iterator returns. Events must not be called concurrently on the same Client.
func (c *Client) Events(ctx context.Context) iter.Seq2[*Batch, error] {
	return func(yield func(*Batch, error) bool) {
		if c.closed.Load() {
			yield(nil, fmt.Errorf("jetstream: client is closed"))
			return
		}
		c.engine.run(ctx, yield)
	}
}

// Close releases the Client's resources. It is safe to call after Events
// returns, concurrently with a running Events (to stop a live tail), and more
// than once; the underlying engine is closed exactly once and every call
// returns that same result. Calling Events after Close yields an error.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.closeErr = c.engine.close()
	})
	return c.closeErr
}

// engine is the internal seam between the public Client and the orchestration
// implementation (planning, download, suppression, cutover, live tail). The
// concrete engine is wired in subsequent work; this interface keeps the public
// surface stable while that lands.
type engine interface {
	// run drives the stream, invoking yield for each batch or recoverable
	// error. It returns when ctx is done, the stream ends, or yield returns
	// false.
	run(ctx context.Context, yield func(*Batch, error) bool)
	close() error
}

// normalizeHost turns a bare host, host:port, or URL into a normalized
// "scheme://host[:port]" base URL with no path, defaulting to https.
func normalizeHost(host string) (string, error) {
	raw := strings.TrimSpace(host)
	if raw == "" {
		return "", fmt.Errorf("jetstream: host is required")
	}
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("jetstream: parse host: %w", err)
	}
	switch u.Scheme {
	case "http", "https":
	case "ws":
		u.Scheme = "http"
	case "wss":
		u.Scheme = "https"
	default:
		return "", fmt.Errorf("jetstream: unsupported host scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return "", fmt.Errorf("jetstream: host is required")
	}
	return u.Scheme + "://" + u.Host, nil
}

// validateConfig rejects internally inconsistent option combinations.
func validateConfig(c *config) error {
	if c.hasAfterSeq && c.hasBeforeSeq && c.beforeSeq <= c.afterSeq {
		return fmt.Errorf("jetstream: beforeSeq (%d) must be greater than afterSeq (%d)", c.beforeSeq, c.afterSeq)
	}
	return nil
}

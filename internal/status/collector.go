package status

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"golang.org/x/sync/singleflight"
)

// Options configures a Collector. Store and DataDir are required;
// TTL/NegTTL/Now have sensible defaults.
type Options struct {
	Store   *store.Store
	DataDir string

	// TTL is the cache lifetime for successful snapshots. Default 30s.
	TTL time.Duration
	// NegTTL is the cache lifetime for errored snapshots. Default 1s.
	NegTTL time.Duration

	// Now overrides the wall clock; tests pin it for determinism.
	// Default time.Now.
	Now func() time.Time
}

const (
	defaultTTL    = 30 * time.Second
	defaultNegTTL = 1 * time.Second
)

// Collector builds Snapshots on demand and caches them.
type Collector struct {
	opts      Options
	startedAt time.Time

	mu     sync.Mutex
	cached *cacheEntry

	sf singleflight.Group
}

type cacheEntry struct {
	snap      *Snapshot
	err       error
	expiresAt time.Time
}

// New validates opts and returns a Collector ready for use.
func New(opts Options) (*Collector, error) {
	if opts.Store == nil {
		return nil, fmt.Errorf("status: Options.Store is required")
	}
	if opts.DataDir == "" {
		return nil, fmt.Errorf("status: Options.DataDir is required")
	}
	if opts.TTL <= 0 {
		opts.TTL = defaultTTL
	}
	if opts.NegTTL <= 0 {
		opts.NegTTL = defaultNegTTL
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Collector{
		opts:      opts,
		startedAt: opts.Now(),
	}, nil
}

// TTL returns the configured success-cache TTL. The renderer uses
// this to set Cache-Control: max-age.
func (c *Collector) TTL() time.Duration { return c.opts.TTL }

// Snapshot returns the latest cached snapshot, building a new one if
// the cache is cold or expired. Concurrent callers on a cold cache
// share a single in-flight build via singleflight.
func (c *Collector) Snapshot(ctx context.Context) (*Snapshot, error) {
	now := c.opts.Now()

	c.mu.Lock()
	cached := c.cached
	c.mu.Unlock()

	if cached != nil && now.Before(cached.expiresAt) {
		return cached.snap, cached.err
	}

	v, err, _ := c.sf.Do("status", func() (any, error) {
		// Re-check inside singleflight; another goroutine may have
		// populated the cache while we were queued.
		c.mu.Lock()
		if c.cached != nil && c.opts.Now().Before(c.cached.expiresAt) {
			cached := c.cached
			c.mu.Unlock()
			return cached, nil
		}
		c.mu.Unlock()

		snap, buildErr := build(ctx, c.opts, c.startedAt)

		entry := &cacheEntry{snap: snap, err: buildErr}
		if buildErr == nil {
			entry.expiresAt = c.opts.Now().Add(c.opts.TTL)
		} else {
			entry.expiresAt = c.opts.Now().Add(c.opts.NegTTL)
		}

		c.mu.Lock()
		c.cached = entry
		c.mu.Unlock()

		return entry, nil
	})
	if err != nil {
		return nil, err
	}
	entry, ok := v.(*cacheEntry)
	if !ok {
		return nil, fmt.Errorf("status: singleflight returned unexpected type %T", v)
	}
	return entry.snap, entry.err
}

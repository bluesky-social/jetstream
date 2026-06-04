package status

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/manifest"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"golang.org/x/sync/singleflight"
)

// Options configures a Collector. Store and DataDir are required.
type Options struct {
	Store   *store.Store
	DataDir string

	// Now overrides the wall clock; tests pin it for determinism.
	// Default time.Now.
	Now func() time.Time

	// Manifest is the in-memory segment manifest. Optional; when nil,
	// the snapshot's CursorLookback section will report zero values.
	Manifest *manifest.Manifest

	// CursorLookback is the operator-configured --cursor-lookback
	// duration. Zero means cursor replay is disabled; the snapshot
	// reports zero values for the cursor-lookback panel.
	CursorLookback time.Duration
}

// Collector builds Snapshots on demand. Concurrent callers share one
// in-flight build via singleflight, but completed snapshots are not cached.
type Collector struct {
	opts      Options
	startedAt time.Time

	sf singleflight.Group
}

// New validates opts and returns a Collector ready for use.
func New(opts Options) (*Collector, error) {
	if opts.Store == nil {
		return nil, fmt.Errorf("status: Options.Store is required")
	}
	if opts.DataDir == "" {
		return nil, fmt.Errorf("status: Options.DataDir is required")
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Collector{
		opts:      opts,
		startedAt: opts.Now(),
	}, nil
}

// Snapshot builds a fresh snapshot. Concurrent callers share a single
// in-flight build via singleflight.
func (c *Collector) Snapshot(ctx context.Context) (*Snapshot, error) {
	v, err, _ := c.sf.Do("status:summary", func() (any, error) {
		return build(ctx, c.opts, c.startedAt)
	})
	if err != nil {
		return nil, err
	}
	snap, ok := v.(*Snapshot)
	if !ok {
		return nil, fmt.Errorf("status: singleflight returned unexpected type %T", v)
	}
	return snap, nil
}

// SnapshotForRequest builds a snapshot with request-scoped diagnostics.
func (c *Collector) SnapshotForRequest(ctx context.Context, req Request) (*Snapshot, error) {
	req = normalizeRequest(req)
	key := requestSingleflightKey(req)
	v, err, _ := c.sf.Do(key, func() (any, error) {
		return buildForRequest(ctx, c.opts, c.startedAt, req)
	})
	if err != nil {
		return nil, err
	}
	snap, ok := v.(*Snapshot)
	if !ok {
		return nil, fmt.Errorf("status: singleflight returned unexpected type %T", v)
	}
	return snap, nil
}

func requestSingleflightKey(req Request) string {
	return "status:" + lengthPrefixed(req.Tab) + lengthPrefixed(req.DID) + lengthPrefixed(req.Handle)
}

func lengthPrefixed(s string) string {
	return strconv.Itoa(len(s)) + ":" + s
}

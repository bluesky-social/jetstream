package subscribe

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/bluesky-social/jetstream/segment"
)

// errColdUnavailable is returned by the cold reader when disk replay deps
// are not wired (test default). Production always injects a real reader.
var errColdUnavailable = errors.New("subscribe: cold reader unavailable")

// coldReader serves a bounded batch of entries with Seq >= cursor from disk
// (sealed segments, active flushed blocks, pending). Returns the entries and
// the next cursor to resume from. It MUST advance the cursor (next > cursor)
// whenever it returns without error and the cursor is below the live tip;
// an empty batch with an unchanged cursor would spin the caller. See
// NewColdReader for the production implementation.
type coldReader func(ctx context.Context, cursor uint64, max int) ([]*Entry, uint64, error)

type tailConfig struct {
	hotBytes int
	cold     coldReader
	logger   *slog.Logger

	// nextSeq returns the authoritative next (one-past-newest) durable seq.
	// Tail uses it to position fresh live subscribers and to classify an
	// empty/edge cursor as "block at tip" vs "read cold from disk". Optional
	// in tests; nil means "use the ring's resident tip only".
	nextSeq func() uint64
}

// Tail is the unified event-fanout core. Ingest calls Append; every
// subscriber goroutine calls ReadFrom in a loop. Hot reads come from the
// in-memory ring (encode-once shared); cold reads fall through to disk via
// the injected coldReader. Replaces the old push-based fanout.
type Tail struct {
	mu      sync.Mutex
	ring    *hotRing
	notify  chan struct{} // closed + replaced on each Append to wake tip waiters
	blocked chan uint64   // nonblocking test/diagnostic signal when a reader parks at the tip
	cold    coldReader
	nextSeq func() uint64
	logger  *slog.Logger

	metrics   *Metrics
	readBatch int
	slowCfg   slowConfig

	// connMu guards the graceful-close registry below. It is distinct
	// from mu: mu guards the hot ring fanout; the conn registry tracks the
	// websocket connections themselves so Shutdown can send each a clean
	// close frame regardless of which read phase it's in.
	connMu   sync.Mutex
	conns    map[uint64]func()
	nextConn uint64
	draining bool
}

// New validates cfg, builds the hot ring + cold-backed Tail, and returns it
// ready for Append and ReadFrom. cold is the disk-backed reader (from
// NewColdReader); nextSeq is the authoritative durable next-seq source
// (ingest writer's NextSeq, resolved through the late-publication pointer).
// Both may be nil in tests that stay hot.
func New(cfg Config, cold coldReader, nextSeq func() uint64) (*Tail, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.applyDefaults()
	t := newTail(tailConfig{
		hotBytes: cfg.HotTailBytes,
		cold:     cold,
		nextSeq:  nextSeq,
		logger:   cfg.Logger.With(slog.String("component", "subscribe/tail")),
	})
	t.metrics = cfg.Metrics
	t.readBatch = cfg.ReadBatch
	t.slowCfg = slowConfig{
		window:       cfg.SlowWindow,
		lagThreshold: cfg.SlowLagThreshold,
		minRate:      cfg.SlowMinRate,
	}
	t.conns = make(map[uint64]func())
	return t, nil
}

func (t *Tail) ReadBatch() int         { return t.readBatch }
func (t *Tail) SlowConfig() slowConfig { return t.slowCfg }

func newTail(cfg tailConfig) *Tail {
	if cfg.cold == nil {
		cfg.cold = func(context.Context, uint64, int) ([]*Entry, uint64, error) {
			return nil, 0, errColdUnavailable
		}
	}
	return &Tail{
		ring:    newHotRing(cfg.hotBytes),
		notify:  make(chan struct{}),
		blocked: make(chan uint64, 1024),
		cold:    cfg.cold,
		nextSeq: cfg.nextSeq,
		logger:  cfg.logger,
	}
}

// Append copies ev into the hot ring and wakes any readers parked at the
// tip. Non-blocking (honors ingest's no-block OnEvent contract). The event
// struct is copied; Payload is shared read-only (the caller must not retain
// or mutate ev's payload after Append returns).
//
// Events MUST arrive in dense seq order (the ingest writer's ordered
// append hook guarantees this in production). A non-dense append resets
// the ring — readers inside the dropped window fall through to the cold
// path — and is surfaced loudly here: it means a producer is appending
// durable events without feeding the tail, the #244 bug class.
func (t *Tail) Append(ev *segment.Event) {
	cp := *ev
	e := newEntry(&cp)
	t.mu.Lock()
	reset := t.ring.append(e)
	t.metrics.incEventsAppended()
	if reset {
		t.metrics.incHotRingResets()
	}
	t.metrics.setHotRingBytes(t.ring.bytes())
	old := t.notify
	t.notify = make(chan struct{})
	t.mu.Unlock()
	close(old) // wake all waiters; they re-read under the lock
	if reset && t.logger != nil {
		t.logger.Warn("hot ring reset on non-dense append; a durable-writer producer is bypassing the tail feed",
			"seq", cp.Seq)
	}
}

// liveTipLocked returns the seq at which a subscriber is caught up to the live
// edge and should block for the next Append (and where a fresh live subscriber
// starts).
//
// When the ring holds events, its tip IS the live edge: the ingest loop calls
// writer.Append then tail.Append per event, so the durable writer is at most
// one event ahead of the ring tip, and that single in-flight event arrives in
// the ring momentarily. Blocking for it — rather than diving to disk — keeps a
// caught-up live subscriber on the hot, encode-once path. Only when the ring
// is empty (cold start / bootstrap, before any Append has been seen) do we
// fall back to the durable writer's NextSeq, which may legitimately be far
// ahead of an empty ring. Caller must hold t.mu.
func (t *Tail) liveTipLocked() uint64 {
	if t.ring.has() {
		return t.ring.tip()
	}
	if t.nextSeq != nil {
		return t.nextSeq()
	}
	return 0
}

// coldThresholdLocked returns the seq below which events live on disk rather
// than in the ring, i.e. the boundary that sends a ring-miss to the cold
// reader instead of blocking. With a populated ring, anything below its base
// was evicted to disk. With an empty ring, anything below the durable tip is
// replayable history not yet (or never) captured live. Caller must hold t.mu.
func (t *Tail) coldThresholdLocked() uint64 {
	if t.ring.has() {
		return t.ring.base()
	}
	return t.liveTipLocked()
}

// ReadFrom returns up to max entries with Seq >= cursor in seq order, plus
// the next cursor. It blocks (until ctx or the next Append) only when cursor
// is at the live edge with nothing available. Cold-misses delegate to the
// injected coldReader. The hot/cold boundary is transparent to the caller.
func (t *Tail) ReadFrom(ctx context.Context, cursor uint64, max int) ([]*Entry, uint64, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, cursor, err
		}
		t.mu.Lock()
		entries, ok := t.ring.lookup(cursor)
		// lookup returns a slice aliasing the ring's backing array. A
		// concurrent Append→evict nils and reslices that array, which would
		// race with — and corrupt — a slice handed to the caller after we
		// unlock. Copy the (bounded) pointer slice out under the lock; the
		// *Entry values are immutable post-construction, so sharing the
		// pointers themselves is safe.
		var out []*Entry
		if ok {
			if len(entries) > max {
				entries = entries[:max]
			}
			out = make([]*Entry, len(entries))
			copy(out, entries)
		}
		coldBelow := t.coldThresholdLocked()
		notify := t.notify
		t.mu.Unlock()

		if ok && len(out) > 0 {
			// Hot hit: serve from the resident ring.
			next := out[len(out)-1].Event.Seq + 1
			t.metrics.incHotReads()
			return out, next, nil
		}
		// ok with an empty batch cannot happen (lookup returns a non-empty
		// suffix or !ok) — but if index math ever drifted again (#244), an
		// unguarded out[len(out)-1] would panic. The mutex is already
		// released here, but subscribers would still crash-loop; fall
		// through to the cold/block classification instead.

		// Ring can't serve it. A cursor below the cold threshold is on disk
		// (evicted, or pre-ring history); anything at/above it is the live
		// edge — at most the single in-flight event between writer.Append and
		// tail.Append — so block for the imminent Append rather than reading
		// it cold from the pending block.
		if cursor < coldBelow {
			t.metrics.incColdReads()
			return t.cold(ctx, cursor, max)
		}

		select {
		case t.blocked <- cursor:
		default:
		}
		select {
		case <-ctx.Done():
			return nil, cursor, ctx.Err()
		case <-notify:
		}
	}
}

// Tip returns the live-edge seq where a no-cursor (live) subscriber starts: it
// will block until the next Append rather than replaying history.
func (t *Tail) Tip() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.liveTipLocked()
}

// ringBytes returns the current hot-ring byte fill under the lock.
func (t *Tail) ringBytes() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.ring.bytes()
}

// RegisterConn enrolls a websocket connection's graceful-close function
// in the shutdown registry and returns an opaque id plus ok=true. The
// close func must send a clean close frame and unblock the handler (for
// coder/websocket this is conn.Close(StatusGoingAway, ...)); Shutdown
// invokes it concurrently with all other registered conns.
//
// ok=false means the tail is already draining: the caller must
// not proceed to serve and should close its own connection immediately.
// This closes the race where a connection accepted after Shutdown
// snapshotted its closers would otherwise never be told to leave.
//
// Pair every ok=true RegisterConn with a DeregisterConn (via the
// returned id) when the connection ends normally, so a long-lived
// process doesn't accumulate dead closers.
func (t *Tail) RegisterConn(closeFn func()) (uint64, bool) {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	if t.draining {
		return 0, false
	}
	id := t.nextConn
	t.nextConn++
	t.conns[id] = closeFn
	return id, true
}

// DeregisterConn removes a previously registered connection. Safe to
// call with an unknown id (e.g. after Shutdown already drained it).
func (t *Tail) DeregisterConn(id uint64) {
	t.connMu.Lock()
	delete(t.conns, id)
	t.connMu.Unlock()
}

// Shutdown gracefully closes every registered connection, fanning the
// per-connection close calls out concurrently and waiting until they
// all finish or ctx expires. The first call flips the registry into
// draining mode so no new connection is admitted mid-drain; subsequent
// calls are no-ops that return nil.
//
// Returns ctx.Err() if the deadline elapses before every connection
// finishes its close handshake — the bound the caller (cmd/jetstream)
// places on how long a clean shutdown may take. Connections still
// closing when the deadline hits are abandoned to the process exit /
// the server's own listener teardown.
func (t *Tail) Shutdown(ctx context.Context) error {
	t.connMu.Lock()
	if t.draining {
		t.connMu.Unlock()
		return nil
	}
	t.draining = true
	closers := make([]func(), 0, len(t.conns))
	for id, fn := range t.conns {
		closers = append(closers, fn)
		delete(t.conns, id)
	}
	t.connMu.Unlock()

	if len(closers) == 0 {
		return nil
	}

	t.logger.Info("gracefully closing subscribers", "count", len(closers))

	// Each close func runs in its own goroutine so one wedged peer can't
	// serialize the others behind it; the whole fan-out is bounded by ctx.
	var wg sync.WaitGroup
	wg.Add(len(closers))
	for _, fn := range closers {
		go func() {
			defer wg.Done()
			fn()
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		// Deadline hit: return without waiting for the stragglers. Their
		// goroutines keep running until their own close calls return,
		// then exit harmlessly; the process is exiting regardless.
		return ctx.Err()
	}
}

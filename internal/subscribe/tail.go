package subscribe

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/bluesky-social/jetstream/internal/ingest"
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
	cold   coldReader
	logger *slog.Logger

	// nextSeq returns the authoritative next (one-past-newest) durable seq.
	// Tail uses it to position fresh live subscribers and to classify an
	// empty/edge cursor as "block at tip" vs "read cold from disk". Optional
	// in tests; nil means "use the ring's resident tip only".
	nextSeq func() uint64
	readLog func() *ingest.ReadableLog
}

// Tail is the unified event-fanout core. Ingest calls Append; every
// subscriber goroutine calls ReadFrom in a loop. Hot reads come from the
// in-memory ring (encode-once shared); cold reads fall through to disk via
// the injected coldReader. Replaces the old push-based fanout.
type Tail struct {
	mu      sync.Mutex
	notify  chan struct{} // closed when a read-log source is installed
	blocked chan uint64   // nonblocking test/diagnostic signal when a reader parks at the tip
	cold    coldReader
	nextSeq func() uint64
	readLog func() *ingest.ReadableLog
	logger  *slog.Logger

	metrics   *Metrics
	readBatch int
	slowCfg   slowConfig

	// connMu guards the graceful-close registry below. It is distinct
	// from mu: mu guards read-log source publication; the conn registry tracks
	// the websocket connections themselves so Shutdown can send each a clean
	// close frame regardless of which read phase it's in.
	connMu   sync.Mutex
	conns    map[uint64]func()
	nextConn uint64
	draining bool
}

// New validates cfg, builds the cold-backed Tail, and returns it ready for
// ReadFrom. cold is the disk-backed reader (from NewColdReader); nextSeq is the
// authoritative durable next-seq source used before the writer read log is
// published.
func New(cfg Config, cold coldReader, nextSeq func() uint64) (*Tail, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.applyDefaults()
	t := newTail(tailConfig{
		cold:    cold,
		nextSeq: nextSeq,
		logger:  cfg.Logger.With(slog.String("component", "subscribe/tail")),
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
		notify:  make(chan struct{}),
		blocked: make(chan uint64, 1024),
		cold:    cfg.cold,
		nextSeq: cfg.nextSeq,
		readLog: cfg.readLog,
		logger:  cfg.logger,
		conns:   make(map[uint64]func()),
	}
}

// SetReadLogSource points the tail at the writer-owned readable log. Wire it
// before publishing the steady-state writer to subscribers.
func (t *Tail) SetReadLogSource(fn func() *ingest.ReadableLog) {
	t.mu.Lock()
	old := t.notify
	t.notify = make(chan struct{})
	defer t.mu.Unlock()
	t.readLog = fn
	close(old)
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
		readLog := t.readLog
		notify := t.notify
		nextSeq := t.nextSeq
		t.mu.Unlock()
		if readLog != nil {
			if log := readLog(); log != nil {
				entries, notify, ok, atTip := log.ReadFrom(cursor, max)
				if ok {
					out := make([]*Entry, len(entries))
					for i := range entries {
						out[i] = entryFromReadLog(entries[i])
					}
					next := out[len(out)-1].Event.Seq + 1
					t.metrics.incHotReads()
					return out, next, nil
				}
				if !atTip && cursor < log.FloorSeq() {
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
					continue
				}
			}
		}
		if nextSeq != nil && cursor < nextSeq() {
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
	readLog := t.readLog
	nextSeq := t.nextSeq
	t.mu.Unlock()
	if readLog != nil {
		if log := readLog(); log != nil {
			return log.TipSeq()
		}
	}
	if nextSeq != nil {
		return nextSeq()
	}
	return 0
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

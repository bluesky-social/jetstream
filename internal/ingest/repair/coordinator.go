package repair

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/jetstream/internal/datamodel"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/syncstate"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/repo"
	"github.com/jcalabro/atmos/streaming"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	defaultChunkSize           = 1024
	defaultMetaFlushInterval   = 250 * time.Millisecond
	defaultMetaFlushByteThresh = 64 << 10
)

var (
	// ErrAlreadyGated is returned by BeginFailedRetry when a DID already has
	// an active repair gate in flight.
	ErrAlreadyGated = errors.New("repair: DID already has an active repair gate")
	// ErrStaleGeneration is returned when a failed-repo retry tries to publish
	// after a newer repair generation superseded it.
	ErrStaleGeneration = errors.New("repair: stale repair generation")
	// ErrInvalidRev is returned when a resync or snapshot carries a non-TID rev.
	ErrInvalidRev = errors.New("repair: invalid rev")
	// ErrStaleSnapshot is returned by PublishRepo when the fetched repo rev is
	// strictly older than the archive head applied before the gate activated.
	// The gate drains buffered live events without emitting KindSync, and the
	// caller must treat the retry attempt as retryable rather than complete.
	ErrStaleSnapshot = errors.New("repair: snapshot rev older than applied head")
)

// FetchRepoFunc downloads and verifies a repo snapshot when resuming a crashed
// PhaseQueued job whose in-memory event is no longer present.
type FetchRepoFunc func(ctx context.Context, did atmos.DID) (*repo.Repo, *repo.Commit, error)

// Config configures a Coordinator for one writer session.
type Config struct {
	Store          metastore.Store
	Stage          StageStore
	SyncStateStore *syncstate.StateStore
	DropMetrics    *ingest.DropMetrics
	Metrics        *Metrics
	Logger         *slog.Logger

	// OnEvent mirrors live.Config.OnEvent for test/oracle observation of rows
	// appended by the repair worker and inbox drainer. Optional.
	OnEvent func(*segment.Event)

	// FetchRepo is invoked when resuming a PhaseQueued job after a crash where
	// the original in-memory streaming.Event is no longer available. Optional;
	// if nil, crashed PhaseQueued jobs drain their inbox without a reset so no
	// live events are lost.
	FetchRepo FetchRepoFunc

	// ChunkSize bounds the number of replacement rows passed to each ClassBulk
	// AppendBatch call. Zero uses defaultChunkSize (1024).
	ChunkSize int

	// MetaFlushInterval bounds how long uncommitted repair metadata (durable
	// inbox entries or queued jobs) waits when no archive rows are committing.
	// Zero uses defaultMetaFlushInterval (250ms). Negative disables periodic
	// background flushing (tests that drive DrainDurability explicitly).
	MetaFlushInterval time.Duration

	// Now overrides time.Now for deterministic tests.
	Now func() time.Time

	// Test hooks (nil in production).
	OnAfterChunkCommit func(ctx context.Context, did atmos.DID, nextRow, totalRows int64) error
}

// MetaSnapshot captures repair metadata ops staged between two writer durable
// batch preparation points. Committed atomically with relay/cursor and archive
// rows through Consumer.onDurableBatch.
type MetaSnapshot struct {
	ops []metastore.Op
}

// Empty reports whether this snapshot carries no metadata operations.
func (s *MetaSnapshot) Empty() bool {
	return s == nil || len(s.ops) == 0
}

type gateEntry struct {
	job          JobRecord
	nextInboxPos uint64
	lastInboxSeq int64
}

type queuedWork struct {
	did         atmos.DID
	gen         uint64
	liveEvt     *streaming.Event
	witnessedAt int64
}

type appendMark struct {
	row   *segment.Event
	onHit func() error
}

// Coordinator owns the per-DID repair gate, durable inbox, staged replacement
// store, and single-concurrency ClassBulk publisher for one writer session.
type Coordinator struct {
	cfg    Config
	logger *slog.Logger
	stage  StageStore

	writerMu sync.RWMutex
	writer   *ingest.Writer

	// mu protects gate state, generations, cutoffs, and queued work.
	// NEVER hold mu across storage, stage, or writer calls (synctest rule).
	mu       sync.Mutex
	gens     map[atmos.DID]uint64
	cutoffs  map[atmos.DID]string
	gated    map[atmos.DID]*gateEntry
	queue    []atmos.DID
	queued   map[atmos.DID]bool
	workData map[atmos.DID]*queuedWork
	workWake chan struct{}

	// metaMu protects the ordered metadata delta staged into ingest.Writer's
	// durable batches. Lock order: mu -> metaMu (never metaMu -> mu).
	metaMu        sync.Mutex
	unsnapped     []metastore.Op
	unsnappedByte int64
	staged        []*MetaSnapshot
	failed        []metastore.Op
	metaWake      chan struct{}

	// publishSem serializes active ClassBulk publishing/draining to 1 DID at a
	// time (channel-based so synctest counts waiters as durably blocked).
	publishSem chan struct{}

	// appendHook matches the last row of an in-flight ClassBulk chunk or
	// drained inbox group inside ingest.Writer's OnAppend callback.
	appendHook atomic.Pointer[appendMark]

	// idleWaiters are woken whenever len(gated) drops to 0 (useful in tests).
	idleMu      sync.Mutex
	idleWaiters []chan struct{}
}

// New constructs a Coordinator for one steady-state writer session.
// Call AttachWriter once the writer exists, Recover before starting live
// consumption or retries, and Run in the session errgroup.
func New(cfg Config) (*Coordinator, error) {
	if cfg.Store == nil {
		return nil, errors.New("repair: Store is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Stage == nil {
		cfg.Stage = NewMemStageStore()
	}
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = defaultChunkSize
	}
	if cfg.MetaFlushInterval == 0 {
		cfg.MetaFlushInterval = defaultMetaFlushInterval
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Coordinator{
		cfg:        cfg,
		logger:     cfg.Logger.With(slog.String("component", "ingest/repair")),
		stage:      cfg.Stage,
		gens:       make(map[atmos.DID]uint64),
		cutoffs:    make(map[atmos.DID]string),
		gated:      make(map[atmos.DID]*gateEntry),
		queued:     make(map[atmos.DID]bool),
		workData:   make(map[atmos.DID]*queuedWork),
		workWake:   make(chan struct{}, 1),
		metaWake:   make(chan struct{}, 1),
		publishSem: make(chan struct{}, 1),
	}, nil
}

// AttachWriter binds the steady-state ingest.Writer. Must be called before Run
// or any RetryHandle publication.
func (c *Coordinator) AttachWriter(w *ingest.Writer) {
	c.writerMu.Lock()
	c.writer = w
	c.writerMu.Unlock()
}

func (c *Coordinator) getWriter() *ingest.Writer {
	c.writerMu.RLock()
	w := c.writer
	c.writerMu.RUnlock()
	return w
}

// OnAppend must be invoked from the shared ingest.Writer's OnAppend callback
// (under the writer mutex, before any flush/cut). It commits chunk offsets,
// inbox drain offsets, and syncstate promotions atomically into the batch that
// holds the appended rows.
func (c *Coordinator) OnAppend(ev *segment.Event) error {
	m := c.appendHook.Load()
	if m == nil || m.row != ev {
		return nil
	}
	return m.onHit()
}

// SnapshotMeta captures repair metadata ops staged since the previous snapshot.
// Called from Consumer.cursorValueForDurableBatch under the writer mutex.
func (c *Coordinator) SnapshotMeta() *MetaSnapshot {
	c.metaMu.Lock()
	defer c.metaMu.Unlock()
	if len(c.unsnapped) == 0 {
		return nil
	}
	snap := &MetaSnapshot{ops: c.unsnapped}
	c.unsnapped = nil
	c.unsnappedByte = 0
	return snap
}

// StageMetaSnapshot writes snap's ops (plus any ops carried over from a prior
// aborted batch) into b and records them for CommitMetaStaged / AbortMetaStaged.
// Returns true if anything was staged (so caller knows to commit/abort).
func (c *Coordinator) StageMetaSnapshot(b metastore.Batch, snap *MetaSnapshot) bool {
	c.metaMu.Lock()
	defer c.metaMu.Unlock()
	hasFailed := len(c.failed) > 0
	hasSnap := snap != nil && len(snap.ops) > 0
	if !hasFailed && !hasSnap {
		return false
	}
	combined := make([]metastore.Op, 0, len(c.failed)+len(snapOps(snap)))
	if hasFailed {
		combined = append(combined, c.failed...)
		c.failed = nil
	}
	if hasSnap {
		combined = append(combined, snap.ops...)
	}
	for _, op := range combined {
		switch op.Kind {
		case metastore.OpSet:
			b.Set(op.Key, op.Value)
		case metastore.OpDelete:
			b.Delete(op.Key)
		case metastore.OpDeleteRange:
			b.DeleteRange(op.Key, op.End)
		}
	}
	c.staged = append(c.staged, &MetaSnapshot{ops: combined})
	return true
}

func snapOps(s *MetaSnapshot) []metastore.Op {
	if s == nil {
		return nil
	}
	return s.ops
}

// CommitMetaStaged retires the oldest staged snapshot after its durable batch
// commits.
func (c *Coordinator) CommitMetaStaged() {
	c.metaMu.Lock()
	defer c.metaMu.Unlock()
	if len(c.staged) == 0 {
		return
	}
	c.staged[0] = nil
	c.staged = c.staged[1:]
}

// AbortMetaStaged rolls all uncommitted staged snapshots into the carry-over
// queue so the next durable batch retries them.
func (c *Coordinator) AbortMetaStaged() {
	c.metaMu.Lock()
	defer c.metaMu.Unlock()
	if len(c.staged) == 0 {
		return
	}
	for _, s := range c.staged {
		c.failed = append(c.failed, s.ops...)
	}
	c.staged = nil
}

func (c *Coordinator) stageOps(ops ...metastore.Op) {
	if len(ops) == 0 {
		return
	}
	var added int64
	for _, op := range ops {
		added += int64(len(op.Key) + len(op.Value) + len(op.End))
	}
	c.metaMu.Lock()
	c.unsnapped = append(c.unsnapped, ops...)
	c.unsnappedByte += added
	shouldWake := c.unsnappedByte >= defaultMetaFlushByteThresh
	c.metaMu.Unlock()
	if shouldWake {
		select {
		case c.metaWake <- struct{}{}:
		default:
		}
	}
}

func (c *Coordinator) hasPendingMeta() bool {
	c.metaMu.Lock()
	defer c.metaMu.Unlock()
	return len(c.unsnapped) > 0 || len(c.failed) > 0
}

func setOp(key, val []byte) metastore.Op {
	return metastore.Op{Kind: metastore.OpSet, Key: key, Value: val}
}

func delOp(key []byte) metastore.Op {
	return metastore.Op{Kind: metastore.OpDelete, Key: key}
}

func delRangeOp(start, end []byte) metastore.Op {
	return metastore.Op{Kind: metastore.OpDeleteRange, Key: start, End: end}
}

// Recover restores generations, cutoffs, active per-DID gates, durable inbox
// cursors, and pending verifier checkpoints from Store. Must run BEFORE the
// live consumer / verifier and failed-repo retry loop start.
func (c *Coordinator) Recover(ctx context.Context) error {
	// 1. Load generations.
	if err := c.scanPrefix(ctx, genPrefix, func(k, v []byte) error {
		did := atmos.DID(strings.TrimPrefix(string(k), genPrefix))
		gen, err := decodeGen(v)
		if err != nil {
			return fmt.Errorf("repair: recover gen %s: %w", did, err)
		}
		c.mu.Lock()
		c.gens[did] = gen
		c.mu.Unlock()
		return nil
	}); err != nil {
		return err
	}

	// 2. Load completed repair cutoffs.
	if err := c.scanPrefix(ctx, cutoffPrefix, func(k, v []byte) error {
		did := atmos.DID(strings.TrimPrefix(string(k), cutoffPrefix))
		c.mu.Lock()
		c.cutoffs[did] = string(v)
		c.mu.Unlock()
		return nil
	}); err != nil {
		return err
	}

	// 3. Load active repair jobs.
	var jobs []*JobRecord
	if err := c.scanPrefix(ctx, jobPrefix, func(_, v []byte) error {
		j, err := decodeJob(v)
		if err != nil {
			return fmt.Errorf("repair: recover job: %w", err)
		}
		jobs = append(jobs, j)
		return nil
	}); err != nil {
		return err
	}

	// 4. For each active job, restore its gate, inbox watermarks, and pending
	// verifier checkpoints (without promoting to applied state).
	for _, j := range jobs {
		var maxPos uint64
		var maxSeq int64
		var latestChain []byte
		var latestHosting []byte

		if len(j.SnapshotChain) > 0 {
			latestChain = j.SnapshotChain
		}

		prefix := string(inboxDIDPrefix(j.DID))
		if err := c.scanRange(ctx, inboxDIDPrefix(j.DID), inboxDIDRangeEnd(j.DID), func(_, v []byte) error {
			g, err := decodeInboxGroup(v)
			if err != nil {
				return fmt.Errorf("repair: recover inbox %s: %w", prefix, err)
			}
			if g.Pos > maxPos {
				maxPos = g.Pos
			}
			if g.UpstreamSeq > maxSeq {
				maxSeq = g.UpstreamSeq
			}
			if len(g.ChainState) > 0 {
				latestChain = g.ChainState
			}
			if len(g.HostingState) > 0 {
				latestHosting = g.HostingState
			}
			return nil
		}); err != nil {
			return err
		}

		if c.cfg.SyncStateStore != nil {
			if len(latestChain) > 0 {
				if err := c.cfg.SyncStateStore.RestorePendingChain(j.DID, latestChain); err != nil {
					return fmt.Errorf("repair: restore pending chain %s: %w", j.DID, err)
				}
			}
			if len(latestHosting) > 0 {
				if err := c.cfg.SyncStateStore.RestorePendingHosting(j.DID, latestHosting); err != nil {
					return fmt.Errorf("repair: restore pending hosting %s: %w", j.DID, err)
				}
			}
		}

		nextPos := max(maxPos, j.DrainedPos) + 1
		c.mu.Lock()
		if j.Generation > c.gens[j.DID] {
			c.gens[j.DID] = j.Generation
		}
		c.gated[j.DID] = &gateEntry{
			job:          *j,
			nextInboxPos: nextPos,
			lastInboxSeq: maxSeq,
		}
		c.enqueueLocked(j.DID)
		c.cfg.Metrics.setActiveGated(len(c.gated))
		c.mu.Unlock()
		c.cfg.Metrics.incResumed()
		c.logger.InfoContext(ctx, "resumed in-progress repair job",
			"did", string(j.DID),
			"job_id", j.JobID,
			"phase", j.Phase,
			"next_row", j.NextRow,
			"total_rows", j.TotalRows,
			"drained_pos", j.DrainedPos,
			"next_inbox_pos", nextPos,
		)
	}
	return nil
}

func (c *Coordinator) scanPrefix(ctx context.Context, prefix string, fn func(k, v []byte) error) error {
	return c.scanRange(ctx, []byte(prefix), prefixUpperBound(prefix), fn)
}

func (c *Coordinator) scanRange(ctx context.Context, lower, upper []byte, fn func(k, v []byte) error) error {
	it, err := c.cfg.Store.NewIter(ctx, lower, upper)
	if err != nil {
		return fmt.Errorf("repair: iter [%s,%s): %w", lower, upper, err)
	}
	defer func() { _ = it.Close() }()
	for it.Next() {
		if err := fn(it.Key(), it.Value()); err != nil {
			return err
		}
	}
	return it.Err()
}

// IsGated reports whether did currently has an active repair gate.
func (c *Coordinator) IsGated(did atmos.DID) bool {
	c.mu.Lock()
	_, ok := c.gated[did]
	c.mu.Unlock()
	return ok
}

// ActiveCount returns the number of currently gated DIDs.
func (c *Coordinator) ActiveCount() int {
	c.mu.Lock()
	n := len(c.gated)
	c.mu.Unlock()
	return n
}

// WaitIdle blocks until all currently active repair jobs finish and reopen
// their gates, or ctx ends. Useful in tests.
func (c *Coordinator) WaitIdle(ctx context.Context) error {
	for {
		c.mu.Lock()
		active := len(c.gated)
		c.mu.Unlock()
		if active == 0 {
			return nil
		}
		ch := make(chan struct{}, 1)
		c.idleMu.Lock()
		c.mu.Lock()
		if len(c.gated) == 0 {
			c.mu.Unlock()
			c.idleMu.Unlock()
			return nil
		}
		c.idleWaiters = append(c.idleWaiters, ch)
		c.mu.Unlock()
		c.idleMu.Unlock()

		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *Coordinator) notifyIdleIfEmptyLocked() {
	if len(c.gated) != 0 {
		return
	}
	c.idleMu.Lock()
	waiters := c.idleWaiters
	c.idleWaiters = nil
	c.idleMu.Unlock()
	for _, ch := range waiters {
		close(ch)
	}
}

// InterceptLiveGroup checks whether a normal (non-resync) live event group for
// did must be captured behind an active repair gate or dropped by a completed
// repair cutoff.
//
// Returns (handled=false) on the hot path for unrelated DIDs with zero I/O and
// zero allocation. When handled=true, Consumer.processBatch must NOT append
// segEvts to ClassLive or promote syncstate; the event is either durably staged
// into the DID's inbox (committed atomically with relay/cursor) or dropped as
// stale/duplicate.
func (c *Coordinator) InterceptLiveGroup(upstreamSeq int64, segEvts []segment.Event) (handled bool, err error) {
	if len(segEvts) == 0 {
		return false, nil
	}
	did := atmos.DID(segEvts[0].DID)
	maxRev := maxGroupRev(segEvts)
	isAccountOrIdentity := isControlGroup(segEvts)

	c.mu.Lock()
	gate, gated := c.gated[did]
	if !gated {
		cutoff := c.cutoffs[did]
		c.mu.Unlock()
		if !isAccountOrIdentity && cutoff != "" && maxRev != "" && maxRev <= cutoff {
			c.cfg.Metrics.incCutoffDropped()
			return true, nil
		}
		return false, nil
	}

	// Dedupe real relay events by upstream seq so relay replay across a crash
	// cannot double-insert an inbox group.
	if upstreamSeq > 0 && upstreamSeq <= gate.lastInboxSeq {
		c.mu.Unlock()
		c.cfg.Metrics.incInboxDeduped()
		return true, nil
	}

	pos := gate.nextInboxPos
	gate.nextInboxPos++
	if upstreamSeq > gate.lastInboxSeq {
		gate.lastInboxSeq = upstreamSeq
	}
	c.mu.Unlock()

	// Peek verifier pending checkpoints without promoting to applied state.
	var chainState, hostingState []byte
	if c.cfg.SyncStateStore != nil {
		if maxRev != "" {
			if buf, ok := c.cfg.SyncStateStore.PeekPendingChain(did, maxRev); ok {
				chainState = buf
			}
		}
		if hasKind(segEvts, segment.KindAccount) && upstreamSeq > 0 {
			if buf, ok := c.cfg.SyncStateStore.PeekPendingHosting(did, upstreamSeq); ok {
				hostingState = buf
			}
		}
	}

	// Clone events into the inbox group (caller reuses segEvts after return).
	evCopy := make([]segment.Event, len(segEvts))
	for i := range segEvts {
		evCopy[i] = segEvts[i]
		if len(segEvts[i].Payload) > 0 {
			evCopy[i].Payload = append([]byte(nil), segEvts[i].Payload...)
		}
	}
	group := &InboxGroup{
		Pos:          pos,
		UpstreamSeq:  upstreamSeq,
		Rev:          maxRev,
		ChainState:   chainState,
		HostingState: hostingState,
		Events:       evCopy,
	}
	encoded := encodeInboxGroup(group)
	c.stageOps(setOp(inboxKey(did, pos), encoded))
	c.cfg.Metrics.incInboxCaptured()
	return true, nil
}

// EnqueueLiveResync moves an automatic Sync 1.1 resync (ResyncAsync or
// ResyncSyncEvent) off Consumer.Run onto the background repair worker.
//
// It activates the per-DID gate and stages repair/job/<did> into the writer's
// durable metadata batch BEFORE returning, so relay/cursor cannot advance past
// evt.Seq without the repair job being durable. It does NOT clone payloads or
// touch ClassBulk on Consumer.Run.
func (c *Coordinator) EnqueueLiveResync(evt streaming.Event, witnessedAt int64) error {
	if evt.Sync == nil {
		return errors.New("repair: resync event missing sync envelope")
	}
	did := atmos.DID(evt.Sync.DID)
	if _, err := atmos.ParseTID(evt.Sync.Rev); err != nil {
		return fmt.Errorf("%w: did=%s rev=%q: %w", ErrInvalidRev, did, evt.Sync.Rev, err)
	}

	src := SourceSyncEvent
	if evt.Resync == streaming.ResyncAsync {
		src = SourceAsyncResync
	}

	evtCopy := evt // shallow copy; verifiedOps slice header is owned by evt

	c.mu.Lock()
	if gate, exists := c.gated[did]; exists {
		switch gate.job.Phase {
		case PhaseQueued:
			if gate.job.Source == SourceFailedRetry {
				// Owned by a RetryHandle: record a durable follow-up instead of
				// racing the retry goroutine over workData/queue.
				if evt.Sync.Rev > gate.job.FollowUpRev {
					gate.job.FollowUpRev = evt.Sync.Rev
					jobBytes, err := encodeJob(&gate.job)
					if err != nil {
						c.mu.Unlock()
						return err
					}
					c.stageOps(setOp(jobKey(did), jobBytes))
				}
				break
			}
			// Still queued (not staged yet): supersede with newer event.
			if evt.Sync.Rev >= gate.job.TargetRev {
				gate.job.TargetRev = evt.Sync.Rev
				if evt.Seq > gate.job.UpstreamSeq {
					gate.job.UpstreamSeq = evt.Seq
				}
				c.workData[did] = &queuedWork{
					did:         did,
					gen:         gate.job.Generation,
					liveEvt:     &evtCopy,
					witnessedAt: witnessedAt,
				}
				jobBytes, err := encodeJob(&gate.job)
				if err != nil {
					c.mu.Unlock()
					return err
				}
				c.stageOps(setOp(jobKey(did), jobBytes))
				c.enqueueLocked(did)
			}
		default:
			// Already publishing or draining: if this resync is strictly newer
			// than the active snapshot, record a durable follow-up so we re-run
			// after the current job finishes without stuffing a whole repo into
			// the metastore inbox.
			if gate.job.SnapshotRev == "" || evt.Sync.Rev > gate.job.SnapshotRev {
				if evt.Sync.Rev > gate.job.FollowUpRev {
					gate.job.FollowUpRev = evt.Sync.Rev
					jobBytes, err := encodeJob(&gate.job)
					if err != nil {
						c.mu.Unlock()
						return err
					}
					c.stageOps(setOp(jobKey(did), jobBytes))
				}
			}
		}
		c.mu.Unlock()
		return nil
	}

	var initSnapChain []byte
	if c.cfg.SyncStateStore != nil {
		if buf, ok := c.cfg.SyncStateStore.PeekPendingChain(did, evt.Sync.Rev); ok {
			initSnapChain = buf
		}
	}

	gen := c.gens[did] + 1
	c.gens[did] = gen
	job := JobRecord{
		Version:       jobRecordVersion,
		DID:           did,
		JobID:         newJobID(),
		Generation:    gen,
		Source:        src,
		Phase:         PhaseQueued,
		UpstreamSeq:   evt.Seq,
		TargetRev:     evt.Sync.Rev,
		SnapshotChain: initSnapChain,
		CreatedAt:     c.cfg.Now().UTC(),
	}
	jobBytes, err := encodeJob(&job)
	if err != nil {
		c.mu.Unlock()
		return err
	}
	c.gated[did] = &gateEntry{
		job:          job,
		nextInboxPos: 1,
		lastInboxSeq: evt.Seq,
	}
	c.workData[did] = &queuedWork{
		did:         did,
		gen:         gen,
		liveEvt:     &evtCopy,
		witnessedAt: witnessedAt,
	}
	c.stageOps(
		setOp(genKey(did), encodeGen(gen)),
		setOp(jobKey(did), jobBytes),
	)
	c.enqueueLocked(did)
	c.cfg.Metrics.setActiveGated(len(c.gated))
	c.mu.Unlock()

	c.cfg.Metrics.incEnqueued(src)
	return nil
}

// RetryHandle owns a per-DID gate reservation for one steady-state
// RunFailedRepoRetry attempt. Caller must finish with either PublishRepo or
// Abort.
type RetryHandle struct {
	c          *Coordinator
	did        atmos.DID
	gen        uint64
	appliedRev string
	done       bool
}

// BeginFailedRetry activates the per-DID gate for did before RunFailedRepoRetry
// starts downloading the repo CAR, so any live event arriving during download
// or between ClassBulk chunks routes to the durable inbox instead of racing
// ClassBulk on the same DID.
func (c *Coordinator) BeginFailedRetry(ctx context.Context, did atmos.DID) (*RetryHandle, error) {
	var appliedRev string
	if c.cfg.SyncStateStore != nil {
		st, err := c.cfg.SyncStateStore.LoadAppliedChain(ctx, did)
		if err != nil {
			return nil, fmt.Errorf("repair: load applied chain %s: %w", did, err)
		}
		if st != nil {
			appliedRev = st.Rev
		}
	}

	c.mu.Lock()
	if _, exists := c.gated[did]; exists {
		c.mu.Unlock()
		return nil, ErrAlreadyGated
	}
	gen := c.gens[did] + 1
	c.gens[did] = gen
	job := JobRecord{
		Version:    jobRecordVersion,
		DID:        did,
		JobID:      newJobID(),
		Generation: gen,
		Source:     SourceFailedRetry,
		Phase:      PhaseQueued,
		AppliedRev: appliedRev,
		CreatedAt:  c.cfg.Now().UTC(),
	}
	jobBytes, err := encodeJob(&job)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	c.gated[did] = &gateEntry{
		job:          job,
		nextInboxPos: 1,
	}
	c.stageOps(
		setOp(genKey(did), encodeGen(gen)),
		setOp(jobKey(did), jobBytes),
	)
	c.cfg.Metrics.setActiveGated(len(c.gated))
	c.mu.Unlock()

	c.cfg.Metrics.incEnqueued(SourceFailedRetry)
	return &RetryHandle{
		c:          c,
		did:        did,
		gen:        gen,
		appliedRev: appliedRev,
	}, nil
}

// PublishRepo validates, stages, and publishes a downloaded failed-repo retry
// behind the per-DID gate via ClassBulk, then drains the DID's durable inbox in
// order and reopens the gate.
func (h *RetryHandle) PublishRepo(ctx context.Context, rp *repo.Repo, commit *repo.Commit) error {
	if h.done {
		return errors.New("repair: retry handle already finished")
	}
	h.done = true

	if _, err := atmos.ParseTID(commit.Rev); err != nil {
		if h.c.cfg.DropMetrics != nil {
			h.c.cfg.DropMetrics.IncDropped(ingest.DropSourceBackfill, ingest.DropReasonInvalidRev)
		}
		_ = h.abortInternal(ctx)
		return fmt.Errorf("%w: backfill did=%s: %w", ErrInvalidRev, h.did, err)
	}

	// Refuse a stale snapshot older than the archive head applied before the
	// gate opened: drain inbox without emitting KindSync, reopen gate, and
	// report ErrStaleSnapshot so the retry runner does not mark StatusComplete.
	if h.appliedRev != "" && commit.Rev < h.appliedRev {
		h.c.logger.WarnContext(ctx, "refusing stale failed-repo retry snapshot",
			"did", string(h.did),
			"snapshot_rev", commit.Rev,
			"applied_rev", h.appliedRev,
		)
		if err := h.abortInternal(ctx); err != nil {
			return err
		}
		return fmt.Errorf("%w: did=%s snapshot=%s applied=%s", ErrStaleSnapshot, h.did, commit.Rev, h.appliedRev)
	}

	// Verify generation is still current before staging.
	h.c.mu.Lock()
	gate, ok := h.c.gated[h.did]
	if !ok || gate.job.Generation != h.gen {
		h.c.mu.Unlock()
		return ErrStaleGeneration
	}
	jobID := gate.job.JobID
	h.c.mu.Unlock()

	stageKey := jobID + ".stage"
	meta, err := h.c.stageRepo(ctx, h.did, rp, commit, stageKey)
	if err != nil {
		_ = h.abortInternal(ctx)
		return err
	}

	// Acquire the single active ClassBulk publisher slot.
	if err := h.c.acquirePublish(ctx); err != nil {
		_ = h.c.stage.Delete(context.Background(), stageKey)
		return err
	}
	defer h.c.releasePublish()

	// Re-check generation after acquiring slot.
	h.c.mu.Lock()
	gate, ok = h.c.gated[h.did]
	if !ok || gate.job.Generation != h.gen {
		h.c.mu.Unlock()
		_ = h.c.stage.Delete(context.Background(), stageKey)
		return ErrStaleGeneration
	}
	gate.job.Phase = PhasePublishing
	gate.job.SnapshotRev = commit.Rev
	gate.job.StageKey = meta.Key
	gate.job.StageSHA256 = meta.SHA256
	gate.job.TotalRows = meta.TotalRows
	gate.job.NextRow = 0
	jobBytes, err := encodeJob(&gate.job)
	if err != nil {
		h.c.mu.Unlock()
		_ = h.c.stage.Delete(context.Background(), stageKey)
		return err
	}
	jobSnap := gate.job
	h.c.stageOps(setOp(jobKey(h.did), jobBytes))
	h.c.mu.Unlock()

	if err := h.c.publishAndDrain(ctx, jobSnap); err != nil {
		return err
	}
	return nil
}

// Abort cancels a failed-repo retry before reset, draining any buffered live
// inbox events without emitting KindSync and reopening the gate.
func (h *RetryHandle) Abort(ctx context.Context) error {
	if h.done {
		return nil
	}
	h.done = true
	return h.abortInternal(ctx)
}

func (h *RetryHandle) abortInternal(ctx context.Context) error {
	if err := h.c.acquirePublish(ctx); err != nil {
		return err
	}
	defer h.c.releasePublish()

	h.c.mu.Lock()
	gate, ok := h.c.gated[h.did]
	if !ok || gate.job.Generation != h.gen {
		h.c.mu.Unlock()
		return nil
	}
	gate.job.Phase = PhaseDraining
	gate.job.SnapshotRev = ""
	jobBytes, err := encodeJob(&gate.job)
	if err != nil {
		h.c.mu.Unlock()
		return err
	}
	jobSnap := gate.job
	h.c.stageOps(setOp(jobKey(h.did), jobBytes))
	h.c.mu.Unlock()

	if err := h.c.drainAndClose(ctx, jobSnap, "aborted"); err != nil {
		return err
	}
	return nil
}

// Run executes the background repair worker loop and periodic idle metadata
// flusher until ctx is canceled or a fatal writer/storage error occurs.
func (c *Coordinator) Run(ctx context.Context) error {
	if c.getWriter() == nil {
		return errors.New("repair: AttachWriter must be called before Run")
	}

	// Optional background metadata flusher so durable inbox entries and queued
	// jobs commit even when no unrelated live events are arriving.
	flushDone := make(chan error, 1)
	flushCtx, cancelFlush := context.WithCancel(ctx)
	defer cancelFlush()
	if c.cfg.MetaFlushInterval > 0 {
		go func() {
			flushDone <- c.runMetaFlusher(flushCtx)
		}()
	} else {
		close(flushDone)
	}

	var runErr error
	for {
		did, ok := c.popQueued()
		if !ok {
			select {
			case <-ctx.Done():
				runErr = ctx.Err()
			case <-c.workWake:
				continue
			}
			break
		}
		if err := c.processQueuedDID(ctx, did); err != nil {
			if ctx.Err() != nil {
				runErr = ctx.Err()
			} else {
				runErr = err
			}
			break
		}
	}

	cancelFlush()
	if ferr := <-flushDone; ferr != nil && runErr == nil && ctx.Err() == nil {
		runErr = ferr
	}
	return runErr
}

func (c *Coordinator) runMetaFlusher(ctx context.Context) error {
	ticker := time.NewTicker(c.cfg.MetaFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-c.metaWake:
		}
		if !c.hasPendingMeta() {
			continue
		}
		w := c.getWriter()
		if w == nil {
			continue
		}
		if err := w.DrainDurability(ctx); err != nil {
			if ctx.Err() != nil || errors.Is(err, ingest.ErrClosed) {
				return nil
			}
			return fmt.Errorf("repair: flush pending metadata: %w", err)
		}
	}
}

func (c *Coordinator) enqueueLocked(did atmos.DID) {
	if !c.queued[did] {
		c.queued[did] = true
		c.queue = append(c.queue, did)
	}
	select {
	case c.workWake <- struct{}{}:
	default:
	}
}

func (c *Coordinator) popQueued() (atmos.DID, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.queue) == 0 {
		return "", false
	}
	did := c.queue[0]
	c.queue = c.queue[1:]
	delete(c.queued, did)
	return did, true
}

func (c *Coordinator) acquirePublish(ctx context.Context) error {
	select {
	case c.publishSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Coordinator) releasePublish() {
	<-c.publishSem
}

func (c *Coordinator) processQueuedDID(ctx context.Context, did atmos.DID) error {
	if err := c.acquirePublish(ctx); err != nil {
		return err
	}
	defer c.releasePublish()

	c.mu.Lock()
	gate, ok := c.gated[did]
	if !ok {
		delete(c.workData, did)
		c.mu.Unlock()
		return nil
	}
	job := gate.job
	work := c.workData[did]
	delete(c.workData, did)
	c.mu.Unlock()

	switch job.Phase {
	case PhaseQueued:
		stageKey := job.JobID + ".stage"
		var meta StageMeta
		var snapRev string
		var snapChain []byte
		var stageErr error

		switch {
		case work != nil && work.liveEvt != nil:
			meta, snapRev, snapChain, stageErr = c.stageLiveResync(ctx, did, *work.liveEvt, stageKey)
			// Release reference to atmos event/verifiedOps immediately once staged.
			work.liveEvt = nil
		case c.cfg.FetchRepo != nil:
			rp, commit, ferr := c.cfg.FetchRepo(ctx, did)
			if ferr != nil {
				stageErr = ferr
			} else {
				meta, stageErr = c.stageRepo(ctx, did, rp, commit, stageKey)
				if stageErr == nil {
					snapRev = commit.Rev
				}
			}
		default:
			stageErr = errors.New("repair: no in-memory resync event or FetchRepo available to resume queued job")
		}

		if stageErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Staging failed before KindSync was ever emitted: abort reset and
			// drain inbox without losing any live events.
			c.logger.WarnContext(ctx, "repair staging failed before reset; draining inbox without reset",
				"did", string(did),
				"job_id", job.JobID,
				"err", stageErr,
			)
			c.mu.Lock()
			if g, exists := c.gated[did]; exists && g.job.Generation == job.Generation {
				g.job.Phase = PhaseDraining
				g.job.SnapshotRev = ""
				if jb, err := encodeJob(&g.job); err == nil {
					c.stageOps(setOp(jobKey(did), jb))
				}
				job = g.job
			}
			c.mu.Unlock()
			return c.drainAndClose(ctx, job, "failed")
		}

		c.mu.Lock()
		gate, ok = c.gated[did]
		if !ok || gate.job.Generation != job.Generation {
			c.mu.Unlock()
			_ = c.stage.Delete(context.Background(), stageKey)
			return nil
		}
		gate.job.Phase = PhasePublishing
		gate.job.SnapshotRev = snapRev
		if len(snapChain) > 0 {
			gate.job.SnapshotChain = snapChain
		}
		gate.job.StageKey = meta.Key
		gate.job.StageSHA256 = meta.SHA256
		gate.job.TotalRows = meta.TotalRows
		gate.job.NextRow = 0
		jobBytes, err := encodeJob(&gate.job)
		if err != nil {
			c.mu.Unlock()
			return err
		}
		job = gate.job
		c.stageOps(setOp(jobKey(did), jobBytes))
		c.mu.Unlock()

		return c.publishAndDrain(ctx, job)

	case PhasePublishing:
		return c.publishAndDrain(ctx, job)

	case PhaseDraining:
		return c.drainAndClose(ctx, job, "succeeded")
	default:
		return fmt.Errorf("repair: unknown job phase %q for %s", job.Phase, did)
	}
}

func (c *Coordinator) publishAndDrain(ctx context.Context, job JobRecord) error {
	w := c.getWriter()
	if w == nil {
		return errors.New("repair: writer not attached")
	}

	// Ensure PhasePublishing + StageKey + StageSHA256 are durable BEFORE
	// emitting any KindSync row.
	if job.NextRow == 0 {
		if err := w.DrainDurability(ctx); err != nil {
			return fmt.Errorf("repair: persist publishing phase %s: %w", job.DID, err)
		}
	}

	sr, err := OpenStageReader(ctx, c.stage, job.StageKey, job.StageSHA256, job.NextRow)
	if err != nil {
		return fmt.Errorf("repair: open staged replacement %s: %w", job.DID, err)
	}
	defer func() { _ = sr.Close() }()

	bulkCtx := ingest.WithClass(ctx, ingest.ClassBulk)
	chunk := make([]segment.Event, 0, c.cfg.ChunkSize)
	rowIdx := job.NextRow

	for rowIdx < job.TotalRows {
		chunk = chunk[:0]
		witnessedAt := c.cfg.Now().UTC().UnixMicro()
		for len(chunk) < c.cfg.ChunkSize && rowIdx+int64(len(chunk)) < job.TotalRows {
			var ev segment.Event
			ok, err := sr.Next(&ev)
			if err != nil {
				return fmt.Errorf("repair: read staged row %d for %s: %w", rowIdx+int64(len(chunk)), job.DID, err)
			}
			if !ok {
				return fmt.Errorf("repair: staged replacement %s truncated at row %d of %d", job.DID, rowIdx+int64(len(chunk)), job.TotalRows)
			}
			ev.WitnessedAt = witnessedAt
			chunk = append(chunk, ev)
		}
		if len(chunk) == 0 {
			break
		}

		endRow := rowIdx + int64(len(chunk))
		isFinal := endRow >= job.TotalRows

		// Record NextRow (and PhaseDraining + SnapshotChain promotion on the
		// final chunk) inside OnAppend under the writer mutex so it commits in
		// the exact durable batch that holds this chunk's rows.
		c.appendHook.Store(&appendMark{
			row: &chunk[len(chunk)-1],
			onHit: func() error {
				c.mu.Lock()
				gate, ok := c.gated[job.DID]
				if !ok || gate.job.Generation != job.Generation {
					c.mu.Unlock()
					return nil
				}
				gate.job.NextRow = endRow
				if isFinal {
					gate.job.Phase = PhaseDraining
				}
				jb, err := encodeJob(&gate.job)
				if err != nil {
					c.mu.Unlock()
					return err
				}
				c.stageOps(setOp(jobKey(job.DID), jb))
				c.mu.Unlock()

				if isFinal && c.cfg.SyncStateStore != nil && len(job.SnapshotChain) > 0 {
					if err := c.cfg.SyncStateStore.PromoteChainBytes(job.DID, job.SnapshotChain); err != nil {
						return err
					}
				}
				return nil
			},
		})
		appendErr := w.AppendBatch(bulkCtx, chunk)
		c.appendHook.Store(nil)
		if appendErr != nil {
			return fmt.Errorf("repair: append bulk chunk [%d,%d) for %s: %w", rowIdx, endRow, job.DID, appendErr)
		}

		c.cfg.Metrics.incBulkChunk(len(chunk))
		if c.cfg.OnEvent != nil {
			for i := range chunk {
				c.cfg.OnEvent(&chunk[i])
			}
		}

		rowIdx = endRow
		job.NextRow = endRow
		if isFinal {
			job.Phase = PhaseDraining
		}

		if c.cfg.OnAfterChunkCommit != nil {
			if err := w.DrainDurability(ctx); err != nil {
				return err
			}
			if err := c.cfg.OnAfterChunkCommit(ctx, job.DID, rowIdx, job.TotalRows); err != nil {
				return err
			}
		}
	}

	return c.drainAndClose(ctx, job, "succeeded")
}

func (c *Coordinator) drainAndClose(ctx context.Context, job JobRecord, outcome string) error {
	w := c.getWriter()
	if w == nil {
		return errors.New("repair: writer not attached")
	}
	bulkCtx := ingest.WithClass(ctx, ingest.ClassBulk)

	for {
		// Make sure every inbox group staged up to now (plus PhaseDraining) is
		// durable in Store before scanning.
		if err := w.DrainDurability(ctx); err != nil {
			return fmt.Errorf("repair: drain durability before inbox scan %s: %w", job.DID, err)
		}

		var groups []*InboxGroup
		if err := c.scanRange(ctx, inboxDIDPrefix(job.DID), inboxDIDRangeEnd(job.DID), func(_, v []byte) error {
			g, err := decodeInboxGroup(v)
			if err != nil {
				return err
			}
			if g.Pos > job.DrainedPos {
				groups = append(groups, g)
			}
			return nil
		}); err != nil {
			return fmt.Errorf("repair: scan inbox %s: %w", job.DID, err)
		}

		for _, g := range groups {
			skip := false
			if !isControlGroup(g.Events) && job.SnapshotRev != "" && g.Rev != "" && g.Rev <= job.SnapshotRev {
				skip = true
			}

			if skip || len(g.Events) == 0 {
				c.mu.Lock()
				if gate, ok := c.gated[job.DID]; ok && gate.job.Generation == job.Generation {
					gate.job.DrainedPos = g.Pos
					jb, err := encodeJob(&gate.job)
					if err != nil {
						c.mu.Unlock()
						return err
					}
					c.stageOps(
						delOp(inboxKey(job.DID, g.Pos)),
						setOp(jobKey(job.DID), jb),
					)
				}
				c.mu.Unlock()
				job.DrainedPos = g.Pos
				c.cfg.Metrics.incInboxSkipped()
				continue
			}

			witnessedAt := c.cfg.Now().UTC().UnixMicro()
			rows := make([]segment.Event, len(g.Events))
			for i := range g.Events {
				rows[i] = g.Events[i]
				rows[i].WitnessedAt = witnessedAt
			}

			grp := g
			c.appendHook.Store(&appendMark{
				row: &rows[len(rows)-1],
				onHit: func() error {
					c.mu.Lock()
					if gate, ok := c.gated[job.DID]; ok && gate.job.Generation == job.Generation {
						gate.job.DrainedPos = grp.Pos
						jb, err := encodeJob(&gate.job)
						if err != nil {
							c.mu.Unlock()
							return err
						}
						c.stageOps(
							delOp(inboxKey(job.DID, grp.Pos)),
							setOp(jobKey(job.DID), jb),
						)
					}
					c.mu.Unlock()

					if c.cfg.SyncStateStore != nil {
						if len(grp.ChainState) > 0 {
							if err := c.cfg.SyncStateStore.PromoteChainBytes(job.DID, grp.ChainState); err != nil {
								return err
							}
						}
						if len(grp.HostingState) > 0 {
							if err := c.cfg.SyncStateStore.PromoteHostingBytes(job.DID, grp.HostingState); err != nil {
								return err
							}
						}
					}
					return nil
				},
			})
			appendErr := w.AppendBatch(bulkCtx, rows)
			c.appendHook.Store(nil)
			if appendErr != nil {
				return fmt.Errorf("repair: drain inbox pos=%d for %s: %w", g.Pos, job.DID, appendErr)
			}
			job.DrainedPos = g.Pos
			c.cfg.Metrics.incInboxDrained()

			if c.cfg.OnEvent != nil {
				for i := range rows {
					c.cfg.OnEvent(&rows[i])
				}
			}
		}

		// Atomic handoff check: if no new inbox entries arrived while we were
		// draining, close the job and reopen the per-DID gate under c.mu.
		c.mu.Lock()
		gate, ok := c.gated[job.DID]
		if !ok || gate.job.Generation != job.Generation {
			c.mu.Unlock()
			return nil
		}
		if gate.nextInboxPos-1 > job.DrainedPos {
			c.mu.Unlock()
			continue
		}

		var ops []metastore.Op
		if job.SnapshotRev != "" && job.SnapshotRev > c.cutoffs[job.DID] {
			c.cutoffs[job.DID] = job.SnapshotRev
			ops = append(ops, setOp(cutoffKey(job.DID), []byte(job.SnapshotRev)))
		}
		ops = append(ops,
			delOp(jobKey(job.DID)),
			delRangeOp(inboxDIDPrefix(job.DID), inboxDIDRangeEnd(job.DID)),
		)
		followUpRev := gate.job.FollowUpRev
		stageKey := job.StageKey
		delete(c.gated, job.DID)
		c.stageOps(ops...)
		c.cfg.Metrics.setActiveGated(len(c.gated))
		c.notifyIdleIfEmptyLocked()
		c.mu.Unlock()

		// Persist job deletion BEFORE removing the staged artifact so a crash
		// before deletion commits still finds the staged bytes on restart.
		if err := w.DrainDurability(ctx); err != nil {
			return fmt.Errorf("repair: commit job completion %s: %w", job.DID, err)
		}
		if stageKey != "" {
			if derr := c.stage.Delete(context.Background(), stageKey); derr != nil {
				c.logger.WarnContext(ctx, "delete completed repair stage", "did", string(job.DID), "stage_key", stageKey, "err", derr)
			}
		}
		c.cfg.Metrics.incCompleted(outcome)

		// If a strictly newer resync arrived while we were gated (or while a
		// failed-repo retry ran and aborted/published an older rev), kick off
		// a follow-up repair job when FetchRepo is configured.
		if followUpRev != "" && (job.SnapshotRev == "" || followUpRev > job.SnapshotRev) && c.cfg.FetchRepo != nil {
			c.enqueueFollowUp(job.DID, followUpRev)
		}
		return nil
	}
}

func (c *Coordinator) enqueueFollowUp(did atmos.DID, targetRev string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.gated[did]; exists {
		return
	}
	gen := c.gens[did] + 1
	c.gens[did] = gen
	job := JobRecord{
		Version:    jobRecordVersion,
		DID:        did,
		JobID:      newJobID(),
		Generation: gen,
		Source:     SourceSyncEvent,
		Phase:      PhaseQueued,
		TargetRev:  targetRev,
		CreatedAt:  c.cfg.Now().UTC(),
	}
	jb, err := encodeJob(&job)
	if err != nil {
		return
	}
	c.gated[did] = &gateEntry{
		job:          job,
		nextInboxPos: 1,
	}
	c.stageOps(
		setOp(genKey(did), encodeGen(gen)),
		setOp(jobKey(did), jb),
	)
	c.enqueueLocked(did)
	c.cfg.Metrics.setActiveGated(len(c.gated))
	c.cfg.Metrics.incEnqueued(SourceSyncEvent)
}

// stageLiveResync streams an atmos resync event directly into a checksummed
// staged artifact without allocating a whole-repo []segment.Event slice.
func (c *Coordinator) stageLiveResync(ctx context.Context, did atmos.DID, evt streaming.Event, stageKey string) (meta StageMeta, maxRev string, snapChain []byte, retErr error) {
	retErr = obs.Span(ctx, func(ctx context.Context) error {
		trace.SpanFromContext(ctx).SetAttributes(attribute.String("did", string(did)))
		if evt.Sync == nil {
			return errors.New("repair: missing sync envelope")
		}
		if _, err := atmos.ParseTID(evt.Sync.Rev); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidRev, err)
		}
		syncPayload, err := evt.Sync.MarshalCBOR()
		if err != nil {
			return fmt.Errorf("repair: marshal sync: %w", err)
		}

		sw, err := NewStageWriter(ctx, c.stage, stageKey)
		if err != nil {
			return err
		}
		committed := false
		defer func() {
			if !committed {
				_ = sw.Abort()
			}
		}()

		// Row 0 is always KindSync (DID tombstone).
		syncRow := segment.Event{
			UpstreamRelayCursor: evt.Seq,
			Kind:                segment.KindSync,
			DID:                 evt.Sync.DID,
			Rev:                 evt.Sync.Rev,
			Payload:             syncPayload,
		}
		if err := segment.ValidateEvent(syncRow); err != nil {
			return fmt.Errorf("repair: invalid sync row: %w", err)
		}
		if err := sw.WriteEvent(&syncRow); err != nil {
			return err
		}

		maxRev = evt.Sync.Rev
		missingBlocks := 0
		var ev segment.Event
		for op, opErr := range evt.Operations() {
			if err := ctx.Err(); err != nil {
				return err
			}
			if opErr != nil {
				return fmt.Errorf("repair: decode resync ops for %s: %w", did, opErr)
			}
			kind, kerr := actionKind(op.Action)
			if kerr != nil {
				return fmt.Errorf("repair: did=%s: %w", did, kerr)
			}
			if reason := validateLiveOp(&op); reason != "" {
				c.cfg.DropMetrics.IncDropped(ingest.DropSourceLive, reason)
				continue
			}

			var payload []byte
			if kind != segment.KindDelete {
				block := op.BlockData()
				if block == nil {
					c.cfg.DropMetrics.IncDropped(ingest.DropSourceLive, ingest.DropReasonMissingBlock)
					missingBlocks++
					continue
				}
				if datamodel.CheckRecord(block) != nil {
					c.cfg.DropMetrics.IncDropped(ingest.DropSourceLive, ingest.DropReasonInvalidDataModel)
					continue
				}
				payload = block
			}

			ev = segment.Event{
				UpstreamRelayCursor: evt.Seq,
				Kind:                kind,
				DID:                 string(op.Repo),
				Collection:          string(op.Collection),
				Rkey:                string(op.RKey),
				Rev:                 string(op.Rev),
				Payload:             payload,
			}
			if err := segment.ValidateEvent(ev); err != nil {
				if errors.Is(err, segment.ErrFieldTooLong) {
					c.cfg.DropMetrics.IncDropped(ingest.DropSourceLive, ingest.DropReasonFieldTooLong)
					c.logger.WarnContext(ctx, "dropped unarchivable resync record",
						"did", string(did),
						"collection_len", len(ev.Collection),
						"rkey_len", len(ev.Rkey),
						"err", err,
					)
					continue
				}
				return fmt.Errorf("repair: invalid resync row: %w", err)
			}
			if err := sw.WriteEvent(&ev); err != nil {
				return err
			}
			if ev.Rev > maxRev {
				maxRev = ev.Rev
			}
		}
		if missingBlocks > 0 {
			c.logger.WarnContext(ctx, "dropped resync ops with missing record blocks",
				"did", string(did),
				"seq", evt.Seq,
				"count", missingBlocks,
			)
		}

		var commitErr error
		meta, commitErr = sw.Commit(ctx)
		if commitErr != nil {
			return commitErr
		}
		committed = true
		c.cfg.Metrics.addStagedBytes(meta.SizeBytes)

		if c.cfg.SyncStateStore != nil {
			if buf, ok := c.cfg.SyncStateStore.PeekPendingChain(did, maxRev); ok {
				snapChain = buf
			}
		}
		return nil
	})
	return meta, maxRev, snapChain, retErr
}

// stageRepo streams a downloaded repo.Repo MST walk directly into a checksummed
// staged artifact without materializing a whole-repo []segment.Event slice.
func (c *Coordinator) stageRepo(ctx context.Context, did atmos.DID, rp *repo.Repo, commit *repo.Commit, stageKey string) (StageMeta, error) {
	if _, err := atmos.ParseTID(commit.Rev); err != nil {
		return StageMeta{}, fmt.Errorf("%w: %w", ErrInvalidRev, err)
	}
	syncPayload, err := (&comatproto.SyncSubscribeRepos_Sync{
		DID:  string(did),
		Rev:  commit.Rev,
		Time: c.cfg.Now().UTC().Format(time.RFC3339Nano),
	}).MarshalCBOR()
	if err != nil {
		return StageMeta{}, fmt.Errorf("repair: marshal repo sync payload: %w", err)
	}

	sw, err := NewStageWriter(ctx, c.stage, stageKey)
	if err != nil {
		return StageMeta{}, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = sw.Abort()
		}
	}()

	syncRow := segment.Event{
		Kind:    segment.KindSync,
		DID:     string(did),
		Rev:     commit.Rev,
		Payload: syncPayload,
	}
	if err := segment.ValidateEvent(syncRow); err != nil {
		return StageMeta{}, fmt.Errorf("repair: invalid repo sync row: %w", err)
	}
	if err := sw.WriteEvent(&syncRow); err != nil {
		return StageMeta{}, err
	}

	var ev segment.Event
	if err := rp.Tree.Walk(func(key string, cid cbor.CID) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		col, rkey, reason := splitRecordPath(key)
		if reason != "" {
			c.cfg.DropMetrics.IncDropped(ingest.DropSourceBackfill, reason)
			return nil
		}
		payload, err := rp.Store.GetBlock(cid)
		if err != nil {
			return fmt.Errorf("repair: did=%s get block %s/%s: %w", did, col, rkey, err)
		}
		if checkErr := datamodel.CheckRecord(payload); checkErr != nil {
			c.cfg.DropMetrics.IncDropped(ingest.DropSourceBackfill, ingest.DropReasonInvalidDataModel)
			return nil //nolint:nilerr // invalid record is dropped, not a failed repo
		}
		ev = segment.Event{
			Kind:       segment.KindCreateResync,
			DID:        string(did),
			Collection: col,
			Rkey:       rkey,
			Rev:        commit.Rev,
			Payload:    payload,
		}
		if err := segment.ValidateEvent(ev); err != nil {
			if errors.Is(err, segment.ErrFieldTooLong) {
				c.cfg.DropMetrics.IncDropped(ingest.DropSourceBackfill, ingest.DropReasonFieldTooLong)
				c.logger.WarnContext(ctx, "dropped unarchivable repo record",
					"did", string(did),
					"collection_len", len(col),
					"rkey_len", len(rkey),
					"err", err,
				)
				return nil
			}
			return fmt.Errorf("repair: did=%s invalid event %s/%s: %w", did, col, rkey, err)
		}
		return sw.WriteEvent(&ev)
	}); err != nil {
		return StageMeta{}, err
	}

	meta, err := sw.Commit(ctx)
	if err != nil {
		return StageMeta{}, err
	}
	committed = true
	c.cfg.Metrics.addStagedBytes(meta.SizeBytes)
	return meta, nil
}

func actionKind(a atmos.Action) (segment.Kind, error) {
	switch a {
	case atmos.ActionCreate:
		return segment.KindCreate, nil
	case atmos.ActionUpdate:
		return segment.KindUpdate, nil
	case atmos.ActionDelete:
		return segment.KindDelete, nil
	case atmos.ActionResync:
		return segment.KindCreateResync, nil
	default:
		return 0, fmt.Errorf("unknown action %q", a)
	}
}

func validateLiveOp(op *streaming.Operation) ingest.DropReason {
	if op.Rev.Validate() != nil {
		return ingest.DropReasonInvalidRev
	}
	if op.Collection.Validate() != nil {
		return ingest.DropReasonInvalidCollection
	}
	if op.RKey.Validate() != nil {
		return ingest.DropReasonInvalidRkey
	}
	return ""
}

func splitRecordPath(key string) (collection, rkey string, reason ingest.DropReason) {
	col, rk, ok := strings.Cut(key, "/")
	if !ok {
		return "", "", ingest.DropReasonInvalidCollection
	}
	if atmos.NSID(col).Validate() != nil {
		return "", "", ingest.DropReasonInvalidCollection
	}
	if atmos.RecordKey(rk).Validate() != nil {
		return "", "", ingest.DropReasonInvalidRkey
	}
	return col, rk, ""
}

func maxGroupRev(evts []segment.Event) string {
	var maxRev string
	for i := range evts {
		if evts[i].Rev > maxRev {
			maxRev = evts[i].Rev
		}
	}
	return maxRev
}

func isControlGroup(evts []segment.Event) bool {
	if len(evts) == 0 {
		return false
	}
	for i := range evts {
		if evts[i].Kind != segment.KindAccount && evts[i].Kind != segment.KindIdentity {
			return false
		}
	}
	return true
}

func hasKind(evts []segment.Event, k segment.Kind) bool {
	for i := range evts {
		if evts[i].Kind == k {
			return true
		}
	}
	return false
}

func newJobID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

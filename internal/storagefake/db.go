// Package storagefake is the in-memory PostgreSQL catalog used by unit tests
// and the layer 3 oracle (plan D3: the only PostgreSQL fake). It implements
// catalog.DB over tables shaped like design §8, with the PostgreSQL behaviour
// the transaction scripts rely on:
//
//   - the archive row lock: the fence (and the lease statements) take it and
//     hold it until commit or rollback, so leader transactions serialize and
//     revisions increase in commit order;
//   - READ COMMITTED leader transactions and REPEATABLE READ readers;
//   - unique, check, and foreign-key constraints, including the partial
//     unique indexes objects_sha256_available and segments_one_active;
//   - non-transactional sequences for object and generation IDs;
//   - NOTIFY delivered on commit.
//
// It deliberately re-implements none of the catalog scripts' checks (plan
// D1): an unfenced write by a stale leader really commits here, and
// CheckInvariants, which runs after every commit, reports the damage.
//
// It also provides the lease (leader.Locker), a metastore.Store over
// metadata_kv, fault injection, and a scheduler hook for deterministic
// interleaving (plan D4).
package storagefake

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
)

// Compiled schema constants, as pgstore records them. pgstore's tests check
// SchemaVersion against pgstore.SchemaVersion.
const (
	FormatVersion = catalog.FormatVersion
	SchemaVersion = 1
)

// Errors a backend can return that the scripts treat as session-ending.
var (
	// ErrTxDone is returned by any call on a finished transaction.
	ErrTxDone = errors.New("storagefake: transaction is finished")
	// ErrTxAborted is PostgreSQL's "current transaction is aborted": a
	// statement failed earlier in the transaction.
	ErrTxAborted = errors.New("storagefake: current transaction is aborted")
	// ErrConnLost is an injected connection loss.
	ErrConnLost = errors.New("storagefake: connection lost")
	// ErrCommitLost is an injected COMMIT whose response never arrived.
	ErrCommitLost = errors.New("storagefake: commit result lost")
)

// ConstraintError is a violated schema constraint.
type ConstraintError struct {
	Constraint string
	Detail     string
}

func (e *ConstraintError) Error() string {
	return fmt.Sprintf("storagefake: constraint %s violated: %s", e.Constraint, e.Detail)
}

func violation(constraint, format string, args ...any) error {
	return &ConstraintError{Constraint: constraint, Detail: fmt.Sprintf(format, args...)}
}

// Config configures a DB. The zero value is usable.
type Config struct {
	// ArchiveID is archive.archive_id. Zero picks a random one.
	ArchiveID [16]byte
	// Now is the database clock (now()). Nil means time.Now, which inside a
	// synctest bubble is the bubble's fake clock.
	Now func() time.Time
	// Invariants tunes the CheckInvariants run after every commit.
	Invariants catalog.InvariantOptions
	// RelayWatch, when set, is Invariants.RelayCursor (invariant 7).
	RelayWatch *RelayWatch
	// OnViolation is called, after the commit is published, with every
	// invariant violation. The DB also records the first one (Violation).
	OnViolation func(rev uint64, err error)
	// Scheduler, when set, is called at every storage call (plan D4).
	Scheduler Scheduler
}

// DB is an in-memory catalog database.
type DB struct {
	cfg Config

	// lock is the archive row lock: a one-slot semaphore so waiters can
	// honor their contexts.
	lock chan struct{}

	mu        sync.Mutex
	state     *state
	nextObj   uint64 // objects_object_id_seq
	nextGen   uint64 // segment_generations_generation_id_seq
	listeners map[*listener]struct{}
	violation error
	faults    []*Fault
	commits   uint64
}

var _ catalog.DB = (*DB)(nil)
var _ catalog.Listener = (*DB)(nil)

// New returns a DB holding an initialized archive row (what `jetstream
// storage init` creates) and no other rows.
func New(cfg Config) *DB {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.ArchiveID == ([16]byte{}) {
		_, _ = rand.Read(cfg.ArchiveID[:])
	}
	db := &DB{
		cfg:       cfg,
		lock:      make(chan struct{}, 1),
		nextObj:   1,
		nextGen:   1,
		listeners: map[*listener]struct{}{},
	}
	db.state = &state{
		archive: catalog.ArchiveRow{
			ArchiveID:     cfg.ArchiveID,
			FormatVersion: FormatVersion,
			SchemaVersion: SchemaVersion,
			CreatedAt:     cfg.Now(),
		},
		meta:         newLayer[string, []byte](),
		objects:      newLayer[uint64, catalog.ObjectRow](),
		objectKeys:   newLayer[string, uint64](),
		objectsBySHA: newLayer[string, uint64](),
		segments:     newLayer[string, catalog.SegmentRow](),
		oneActive:    newLayer[string, uint64](),
		generations:  newLayer[uint64, catalog.GenerationRow](),
		genBlocks:    newLayer[string, catalog.GenerationBlockRow](),
		activeBlocks: newLayer[string, catalog.ActiveBlockRow](),
		hotBatches:   newLayer[uint64, catalog.HotBatchRow](),
	}
	return db
}

// state is one committed database state. Every field is immutable once
// published; a write transaction builds the next one.
type state struct {
	archive catalog.ArchiveRow

	meta         *layer[string, []byte]
	objects      *layer[uint64, catalog.ObjectRow]
	objectKeys   *layer[string, uint64] // objects.key UNIQUE
	objectsBySHA *layer[string, uint64] // objects_sha256_available
	segments     *layer[string, catalog.SegmentRow]
	oneActive    *layer[string, uint64] // segments_one_active: namespace -> index
	generations  *layer[uint64, catalog.GenerationRow]
	genBlocks    *layer[string, catalog.GenerationBlockRow]
	activeBlocks *layer[string, catalog.ActiveBlockRow]
	hotBatches   *layer[uint64, catalog.HotBatchRow]
}

// child returns a mutable state over s.
func (s *state) child() *state {
	return &state{
		archive:      s.archive,
		meta:         s.meta.child(),
		objects:      s.objects.child(),
		objectKeys:   s.objectKeys.child(),
		objectsBySHA: s.objectsBySHA.child(),
		segments:     s.segments.child(),
		oneActive:    s.oneActive.child(),
		generations:  s.generations.child(),
		genBlocks:    s.genBlocks.child(),
		activeBlocks: s.activeBlocks.child(),
		hotBatches:   s.hotBatches.child(),
	}
}

func (s *state) freeze() *state {
	return &state{
		archive:      s.archive,
		meta:         s.meta.freeze(),
		objects:      s.objects.freeze(),
		objectKeys:   s.objectKeys.freeze(),
		objectsBySHA: s.objectsBySHA.freeze(),
		segments:     s.segments.freeze(),
		oneActive:    s.oneActive.freeze(),
		generations:  s.generations.freeze(),
		genBlocks:    s.genBlocks.freeze(),
		activeBlocks: s.activeBlocks.freeze(),
		hotBatches:   s.hotBatches.freeze(),
	}
}

// Composite primary keys encode as strings that sort like the SQL tuples.

func segKey(ns catalog.Namespace, idx uint64) string {
	return fmt.Sprintf("%s\x00%020d", ns, idx)
}

func blockKey(ns catalog.Namespace, idx uint64, ordinal int) string {
	return fmt.Sprintf("%s\x00%020d\x00%010d", ns, idx, ordinal)
}

func blockPrefix(ns catalog.Namespace, idx uint64) string {
	return fmt.Sprintf("%s\x00%020d\x00", ns, idx)
}

func genBlockKey(gen uint64, ordinal int) string {
	return fmt.Sprintf("%020d\x00%010d", gen, ordinal)
}

func genBlockPrefix(gen uint64) string {
	return fmt.Sprintf("%020d\x00", gen)
}

func (db *DB) now() time.Time { return db.cfg.Now() }

func (db *DB) current() *state {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.state
}

// lockArchive takes the archive row lock, waiting like a blocked UPDATE.
func (db *DB) lockArchive(ctx context.Context) error {
	select {
	case db.lock <- struct{}{}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("storagefake: canceling statement waiting for the archive row lock: %w", ctx.Err())
	}
}

func (db *DB) unlockArchive() { <-db.lock }

// publish installs next as the committed state and runs the post-commit
// work: notifications and the invariant check. The caller holds the archive
// row lock, so states are published in lock order.
func (db *DB) publish(next *state, notify []uint64, dropNotify bool) {
	next = next.freeze()
	db.mu.Lock()
	db.state = next
	db.commits++
	if !dropNotify {
		// Sends never block, and holding mu keeps a listener from closing
		// mid-send.
		for l := range db.listeners {
			for _, rev := range notify {
				l.deliver(rev)
			}
		}
	}
	db.mu.Unlock()
	db.checkInvariants(next)
}

func (db *DB) checkInvariants(s *state) {
	opts, framesFrom := db.cfg.Invariants, ^uint64(0)
	if w := db.cfg.RelayWatch; w != nil {
		opts.RelayCursor, framesFrom = w.Check, w.from()
	}
	snap, err := catalog.LoadSnapshotFrames(context.Background(), &readTx{db: db, s: s}, framesFrom)
	if err == nil {
		err = catalog.CheckInvariants(snap, opts)
	}
	if err == nil {
		return
	}
	db.mu.Lock()
	if db.violation == nil {
		db.violation = fmt.Errorf("after revision %d: %w\n%s", s.archive.CatalogRevision, err, snap)
	}
	db.mu.Unlock()
	if db.cfg.OnViolation != nil {
		db.cfg.OnViolation(s.archive.CatalogRevision, err)
	}
}

// Violation returns the first invariant violation any commit produced, or
// nil.
func (db *DB) Violation() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.violation
}

// Commits returns how many transactions have committed, including lease
// statements.
func (db *DB) Commits() uint64 {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.commits
}

// Archive returns the committed archive row.
func (db *DB) Archive() catalog.ArchiveRow { return db.current().archive }

// Snapshot loads the committed catalog without the reader fault and
// scheduler seams, for test assertions.
func (db *DB) Snapshot() (*catalog.Snapshot, error) {
	return catalog.LoadSnapshot(context.Background(), &readTx{db: db, s: db.current()})
}

func (db *DB) yield(ctx context.Context, point string) error {
	if db.cfg.Scheduler == nil {
		return nil
	}
	return db.cfg.Scheduler.Yield(ctx, point)
}

// Begin implements catalog.DB.
func (db *DB) Begin(ctx context.Context, kind catalog.TxKind) (catalog.Tx, error) {
	if err := db.yield(ctx, "begin/"+string(kind)); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &tx{db: db, kind: kind, connLost: db.armConnLost(kind)}, nil
}

// BeginRead implements catalog.DB.
func (db *DB) BeginRead(ctx context.Context) (catalog.ReadTx, error) {
	if err := db.yield(ctx, "begin_read"); err != nil {
		return nil, err
	}
	if d := db.slowRead(); d > 0 {
		t := time.NewTimer(d)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return nil, ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &readTx{db: db, s: db.current(), seam: true}, nil
}

// Listen implements catalog.Listener. Notifications to a listener that is
// not keeping up are dropped, which LISTEN clients must tolerate anyway.
func (db *DB) Listen(ctx context.Context) (<-chan uint64, error) {
	l := &listener{ch: make(chan uint64, 64)}
	db.mu.Lock()
	db.listeners[l] = struct{}{}
	db.mu.Unlock()
	go func() {
		<-ctx.Done()
		db.mu.Lock()
		delete(db.listeners, l)
		close(l.ch)
		db.mu.Unlock()
	}()
	return l.ch, nil
}

type listener struct {
	ch chan uint64
}

func (l *listener) deliver(rev uint64) {
	select {
	case l.ch <- rev:
	default:
	}
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	return bytes.Clone(b)
}

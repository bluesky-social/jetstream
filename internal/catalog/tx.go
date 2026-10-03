package catalog

import (
	"context"
	"time"

	"github.com/bluesky-social/jetstream/internal/metastore"
)

// FormatVersion is archive.format_version, the storage layout version this
// binary reads and writes. A pod refuses an archive with any other value
// (design §8).
const FormatVersion = 1

// DB opens catalog transactions (plan D1). Implementations: pgstore (SQL
// against PostgreSQL) and storagefake (in-memory tables with PostgreSQL's
// locking and visibility). The transaction scripts in this package run over
// it, so layer 3 exercises the same checks production runs.
type DB interface {
	// Begin opens a leader write transaction at READ COMMITTED. The first
	// statement a script runs in it is always Tx.FenceBump.
	Begin(ctx context.Context, kind TxKind) (Tx, error)
	// BeginRead opens a REPEATABLE READ READ ONLY transaction: every read
	// through it sees one snapshot.
	BeginRead(ctx context.Context) (ReadTx, error)
}

// Listener delivers the revisions leader transactions NOTIFY on commit
// (LISTEN jetstream_catalog). Delivery is best effort: a notification can be
// lost or coalesced, so a follower also polls. The channel closes when ctx
// ends.
type Listener interface {
	Listen(ctx context.Context) (<-chan uint64, error)
}

// TxKind labels a leader transaction for metrics and tracing
// (jetstream_pg_txn_duration_seconds{kind}).
type TxKind string

const (
	TxHotBatch   TxKind = "hot_batch"
	TxBlock      TxKind = "block"
	TxFold       TxKind = "fold"
	TxSeal       TxKind = "seal"
	TxObjects    TxKind = "objects"
	TxMetadata   TxKind = "metadata"
	TxNamespace  TxKind = "namespace"
	TxCompaction TxKind = "compaction"
	TxGC         TxKind = "gc"
)

// Tx is one leader write transaction. Each method, and each Read, is one
// statement against the design §8 schema. The primitives do no checking of
// their own beyond what the schema's constraints enforce: every correctness
// check lives in the scripts (scripts.go), so a backend cannot hide a bug in
// one.
//
// A Tx is not safe for concurrent use. After any method returns an error the
// only valid call is Rollback.
//
// A method that returns only an error and writes (ApplyMeta, InsertObjects,
// SetObjectsAvailable, ClearUnreferenced, InsertHotBatch, InsertActiveBlock,
// InsertGenerationBlocks, InsertSegment, DeleteNamespace, Notify) may be
// queued and sent with a later statement, to save a round trip. Its failure
// is then reported by the first later call, at the latest Commit, and the
// transaction aborts as it would have anyway. Scripts treat every error
// alike, so none depends on which call reports it.
//
// Reads are batched the same way: FenceBump carries a script's reads in the
// fence's round trip, and Read sends several in one. Over a WAN link the
// archive row lock is held for as many round trips as a script makes after
// its fence, so a script that knows its reads up front makes one.
type Tx interface {
	// FenceBump runs the §6.4 fence, then reads, in one round trip. The
	// reads run after the fence, so under its row lock: they see every
	// transaction that committed before this one took the lock. ok=false
	// means no row matched: the epoch is stale, the reads did not run and
	// locked nothing, and the transaction is aborted, so Rollback is the
	// only call left.
	FenceBump(ctx context.Context, epoch uint64, reads ...Read) (revision uint64, ok bool, err error)
	// Read runs reads in one round trip, after every queued statement. With
	// no reads it does nothing.
	Read(ctx context.Context, reads ...Read) error

	// ApplyMeta applies ops in order, with metastore.Batch semantics.
	ApplyMeta(ctx context.Context, ops []metastore.Op) error

	// InsertObjects inserts uploading rows. ids, of len(objs), receives
	// their object IDs in argument order when the statement's round trip
	// returns: they are valid once a later call that is not queued (Read, a
	// method that returns more than an error, or Commit) has returned nil.
	InsertObjects(ctx context.Context, objs []NewObject, ids []uint64) error
	// SetObjectsAvailable moves the uploading rows among ids to available
	// (§7.3 step 6). A script reads each row, row-locked, before queuing
	// this, so it knows what the statement will change.
	SetObjectsAvailable(ctx context.Context, ids []uint64) error
	// ClearUnreferenced is the §7.4 statement's write: it sets
	// unreferenced_at NULL on the available rows among ids. A script reads
	// each row, row-locked, to check it before queuing this.
	ClearUnreferenced(ctx context.Context, ids []uint64) error

	InsertHotBatch(ctx context.Context, row HotBatchRow) error
	// DeleteHotBatches deletes the hot batches with first_seq in [lo, hi]
	// and returns them in first_seq order.
	DeleteHotBatches(ctx context.Context, lo, hi uint64) ([]HotBatchSpan, error)

	// ActiveBlocksForUpdate returns a segment's active blocks in ordinal
	// order, row-locked.
	ActiveBlocksForUpdate(ctx context.Context, ns Namespace, idx uint64) ([]ActiveBlockRow, error)
	InsertActiveBlock(ctx context.Context, row ActiveBlockRow) error
	// DeleteActiveBlocks deletes every active block of a segment and
	// returns how many it deleted.
	DeleteActiveBlocks(ctx context.Context, ns Namespace, idx uint64) (int, error)

	// InsertGeneration inserts a segment_generations row and returns its ID.
	// row.ID and row.CreatedAt are ignored.
	InsertGeneration(ctx context.Context, row GenerationRow) (uint64, error)
	InsertGenerationBlocks(ctx context.Context, rows []GenerationBlockRow) error
	// SealSegment marks an active segment sealed at generation gen. ok=false
	// when the segment is not active.
	SealSegment(ctx context.Context, ns Namespace, idx, gen, revision uint64) (ok bool, err error)
	InsertSegment(ctx context.Context, row SegmentRow) error
	// DeleteNamespace deletes every segment, generation, generation block,
	// and active block row of ns.
	DeleteNamespace(ctx context.Context, ns Namespace) error

	// SegmentForUpdate returns one segments row, row-locked.
	SegmentForUpdate(ctx context.Context, ns Namespace, idx uint64) (SegmentRow, bool, error)
	// Generation returns one segment_generations row.
	Generation(ctx context.Context, id uint64) (GenerationRow, bool, error)
	// BlocksOfGeneration returns a generation's blocks in ordinal order.
	BlocksOfGeneration(ctx context.Context, id uint64) ([]GenerationBlockRow, error)
	// SetSegmentGeneration points a sealed segment at generation gen.
	// ok=false when the segment is not sealed.
	SetSegmentGeneration(ctx context.Context, ns Namespace, idx, gen, revision uint64) (ok bool, err error)
	// DeleteGeneration deletes a segment_generations row and, by cascade,
	// its generation_blocks. ok=false when no row has that ID.
	DeleteGeneration(ctx context.Context, id uint64) (ok bool, err error)

	// MarkUnreferenced is one page of the §13 mark: of the available
	// objects with unreferenced_at NULL and object_id > after, it takes the
	// first limit by object_id and sets unreferenced_at = now() on the ones
	// no row references.
	MarkUnreferenced(ctx context.Context, after uint64, limit int) (MarkPage, error)
	// ClaimObjects is the §13 claim: up to limit objects that are available
	// with unreferenced_at older than gcDelay, or uploading with created_at
	// older than orphanAge, move to deleting. The returned rows carry the
	// state they had before the claim.
	ClaimObjects(ctx context.Context, gcDelay, orphanAge time.Duration, limit int) ([]ObjectRow, error)
	// DeletingObjects returns up to limit deleting objects in object_id
	// order: the claims of a GC run that did not finish.
	DeletingObjects(ctx context.Context, limit int) ([]ObjectRow, error)
	// ReferencedObjects runs the §13 NOT EXISTS checks for every ID and
	// returns, sorted and deduplicated, the ones some row references.
	ReferencedObjects(ctx context.Context, ids []uint64) ([]uint64, error)
	// ForgetObjects deletes the deleting rows among ids and returns how many
	// it deleted.
	ForgetObjects(ctx context.Context, ids []uint64) (int, error)

	// Notify queues pg_notify('jetstream_catalog', revision) for commit.
	Notify(ctx context.Context, revision uint64) error

	// Commit commits. An error means the result is unknown: the
	// transaction may or may not have applied.
	Commit(ctx context.Context) error
	// Rollback abandons the transaction. It is safe after a failed method
	// or Commit.
	Rollback(ctx context.Context) error
}

// ReadTx is one REPEATABLE READ READ ONLY transaction (design §9.1). The
// follower runs one per tick; on-demand lookups run their own.
type ReadTx interface {
	Archive(ctx context.Context) (ArchiveRow, error)
	// SegmentsSince returns segment rows with revision > rev, ordered by
	// (namespace, segment_index).
	SegmentsSince(ctx context.Context, rev uint64) ([]SegmentRow, error)
	// Generations returns the generation rows with the given IDs.
	Generations(ctx context.Context, ids []uint64) ([]GenerationRow, error)
	// GenerationBlocks returns the blocks of the given generations, ordered
	// by (generation_id, ordinal).
	GenerationBlocks(ctx context.Context, ids []uint64) ([]GenerationBlockRow, error)
	// ActiveBlocksSince returns active block rows with revision > rev,
	// ordered by (namespace, segment_index, ordinal).
	ActiveBlocksSince(ctx context.Context, rev uint64) ([]ActiveBlockRow, error)
	// ActiveBlockKeys returns the key of every active block, ordered by
	// (namespace, segment_index, ordinal).
	ActiveBlockKeys(ctx context.Context) ([]ActiveBlockKey, error)
	// HotBatches returns every hot batch in first_seq order. Frame is set
	// only for inline batches with first_seq >= framesFrom.
	HotBatches(ctx context.Context, framesFrom uint64) ([]HotBatchRow, error)
	// Objects returns the object rows with the given IDs.
	Objects(ctx context.Context, ids []uint64) ([]ObjectRow, error)
	// MetaGet returns the values of the keys that exist.
	MetaGet(ctx context.Context, keys [][]byte) (map[string][]byte, error)
	// ObjectStates counts object rows by state
	// (jetstream_objects{state}).
	ObjectStates(ctx context.Context) (map[ObjectState]int64, error)
	// Close ends the transaction.
	Close(ctx context.Context) error
}

// ArchiveRow is the single archive row.
type ArchiveRow struct {
	ArchiveID       [16]byte
	FormatVersion   int
	SchemaVersion   int
	WriterEpoch     uint64
	HolderID        [16]byte // zero when NULL
	LeaseExpiresAt  time.Time
	CatalogRevision uint64
	CreatedAt       time.Time
}

// ObjectState is objects.state.
type ObjectState string

const (
	ObjectUploading ObjectState = "uploading"
	ObjectAvailable ObjectState = "available"
	ObjectDeleting  ObjectState = "deleting"
)

// ObjectRow is one objects row.
type ObjectRow struct {
	ID        uint64
	Key       [16]byte
	SHA256    [32]byte
	Length    int64
	State     ObjectState
	CreatedAt time.Time
	// UnreferencedAt is zero when NULL.
	UnreferencedAt time.Time
}

// MarkPage is the result of one Tx.MarkUnreferenced page.
type MarkPage struct {
	// Scanned is how many candidate rows the page covered; fewer than the
	// limit means the mark is done.
	Scanned int
	// Last is the highest object_id the page covered, the next page's
	// after. Zero when Scanned is.
	Last uint64
	// Marked is how many rows the page set unreferenced_at on.
	Marked int
}

// NewObject is an uploading row to insert.
type NewObject struct {
	Key    [16]byte
	SHA256 [32]byte
	Length int64
}

// HotBatchRow is one hot_batches row. Exactly one of Frame and ObjectID is
// set, except that ReadTx.HotBatches leaves Frame nil below framesFrom.
type HotBatchRow struct {
	FirstSeq, LastSeq              uint64
	EventCount                     uint32
	MinWitnessedUS, MaxWitnessedUS int64
	Epoch                          uint64
	Revision                       uint64
	CommittedAt                    time.Time
	Frame                          []byte
	ObjectID                       uint64
	// Inline reports an inline row, whether or not Frame was loaded. Reads
	// always set it; InsertHotBatch derives it from Frame instead.
	Inline bool
}

// HotBatchSpan is what DeleteHotBatches returns per deleted row.
type HotBatchSpan struct {
	FirstSeq, LastSeq uint64
	EventCount        uint32
	ObjectID          uint64
}

// SegmentRow is one segments row. GenerationID is zero while active.
type SegmentRow struct {
	Namespace    Namespace
	Index        uint64
	State        SegmentState
	GenerationID uint64
	Revision     uint64
}

// ActiveBlockKey is an active_segment_blocks primary key.
type ActiveBlockKey struct {
	Namespace Namespace
	Segment   uint64
	Ordinal   int
}

// ActiveBlockRow is one active_segment_blocks row.
type ActiveBlockRow struct {
	Namespace                      Namespace
	Segment                        uint64
	Ordinal                        int
	ObjectID                       uint64
	EventCount                     uint32
	MinSeq, MaxSeq                 uint64
	MinWitnessedUS, MaxWitnessedUS int64
	CompressedLength               int64
	UncompressedLength             int64
	Revision                       uint64
}

// Key returns the row's primary key.
func (r ActiveBlockRow) Key() ActiveBlockKey {
	return ActiveBlockKey{Namespace: r.Namespace, Segment: r.Segment, Ordinal: r.Ordinal}
}

// GenerationRow is one segment_generations row.
type GenerationRow struct {
	ID             uint64
	Namespace      Namespace
	Segment        uint64
	Header         []byte
	FooterObjectID uint64
	CreatedAt      time.Time
	Revision       uint64
}

// GenerationBlockRow is one generation_blocks row.
type GenerationBlockRow struct {
	GenerationID     uint64
	Ordinal          int
	ObjectID         uint64
	CompressedLength int64
}

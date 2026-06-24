package jetstream

import (
	"context"
	"fmt"
	"iter"

	iclient "github.com/bluesky-social/jetstream/internal/client"
)

// TypedEvent pairs a delivered Event with its record decoded into the caller's
// type T. It is produced by TypedEvents.
//
//   - For a create/update commit whose Collection matches the requested NSID and
//     whose record decoded cleanly, Record is non-nil and DecodeErr is nil.
//   - For a commit whose record failed to decode into T, Record is nil and
//     DecodeErr is set (the Event is still delivered; nothing is dropped).
//   - For a delete, a non-commit event (identity/account/sync), or a commit of a
//     different collection, Record and DecodeErr are both nil — inspect Event.
//
// Event always carries the full envelope (DID/Seq/TimeUS/Kind and, for commits,
// Operation/Collection/Rkey/Rev/CID/RecordCBOR). In the raw-record modes that
// TypedEvents requires, Event.Commit.Record (the generic map) is nil by design.
type TypedEvent[T any] struct {
	Event     Event
	Record    *T
	DecodeErr error
}

// TypedBatch is a batch of TypedEvents, mirroring Batch.
type TypedBatch[T any] struct {
	events []TypedEvent[T]
}

// Events returns the typed events in this batch. The slice — and any non-nil
// Record in it — is owned by the caller for the lifetime of the loop iteration;
// do not retain past the next iteration without copying (see the aliasing note
// on TypedEvents).
func (b *TypedBatch[T]) Events() []TypedEvent[T] { return b.events }

// LastCursor returns the highest Seq in the batch, suitable for persisting as a
// resume point. Returns 0 for an empty batch.
func (b *TypedBatch[T]) LastCursor() uint64 {
	var maxSeq uint64
	for i := range b.events {
		if b.events[i].Event.Seq > maxSeq {
			maxSeq = b.events[i].Event.Seq
		}
	}
	return maxSeq
}

// TypedEvents adapts a Client's event stream into records decoded as type T,
// avoiding the generic map[string]any decode that dominates the client's CPU
// and allocations at scale (#142). The client MUST have been built with
// WithRawRecords or WithRawRecordsCopied so the expensive map build is skipped
// on the parallel decode workers; TypedEvents then decodes each matching record
// straight into a *T via PT.UnmarshalCBOR.
//
// T is a lexicon record type and PT its pointer, constrained to implement
// UnmarshalCBOR — exactly the shape atmos's generated record types satisfy, so
// the call site is just:
//
//	for tb, err := range jetstream.TypedEvents[bsky.FeedLike](ctx, c, "app.bsky.feed.like") {
//		for _, te := range tb.Events() {
//			if te.Record != nil { /* te.Record is *bsky.FeedLike */ }
//		}
//	}
//
// collection is the NSID whose commits are decoded into T; it MUST be non-empty
// and MUST match the type T. Only create/update commits of that collection are
// decoded — every other event (deletes, other collections, identity/account/
// sync) passes through with a nil Record (see TypedEvent). Requiring the
// collection is a safety measure: feeding one record type's bytes to another's
// UnmarshalCBOR can silently produce a garbage struct rather than an error, so
// TypedEvents never guesses. Pair it with WithCollections([]string{collection})
// on the Client so the server/engine only deliver that collection.
//
// Errors from the underlying stream are forwarded verbatim (with a nil batch),
// preserving the ErrFatal contract. Per-record decode failures are surfaced as
// TypedEvent.DecodeErr, not as stream errors, so one bad record does not abort
// iteration.
//
// Aliasing/lifetime: with WithRawRecords (the zero-copy default), a decoded *T
// may alias the client's internal buffer through Event.Commit.RecordCBOR — its
// string/byte fields are valid only for the current loop iteration. Copy what
// you need to retain, or build the Client with WithRawRecordsCopied for records
// that are safe to keep.
func TypedEvents[T any, PT interface {
	*T
	UnmarshalCBOR([]byte) error
}](ctx context.Context, c *Client, collection string) iter.Seq2[*TypedBatch[T], error] {
	return func(yield func(*TypedBatch[T], error) bool) {
		if collection == "" {
			yield(nil, fmt.Errorf("jetstream: TypedEvents requires a non-empty collection matching the record type"))
			return
		}
		if c == nil || c.engine == nil {
			yield(nil, errClientNotInitialized)
			return
		}
		if c.closed.Load() {
			yield(nil, fmt.Errorf("jetstream: client is closed"))
			return
		}
		// Fast path: when the client is backed by the real engine, run the typed
		// record decode ON the parallel decode workers (via the engine's backfill
		// transform seam) instead of in this single consumer goroutine. Decoding
		// the whole archive's records into T on one goroutine is the bottleneck at
		// high core counts (#146); fanning it across the worker pool is the point.
		if re, ok := c.engine.(*realEngine); ok {
			typedRun[T, PT](ctx, re, collection, yield)
			return
		}
		// Fallback (test fakes / non-real engines): assemble typed batches in this
		// goroutine over the public Events stream. Identical output to the fast
		// path — both call assembleTyped — just not parallel.
		for batch, err := range c.Events(ctx) {
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			if !yield(assembleTyped[T, PT](batch.Events(), collection), nil) {
				return
			}
		}
	}
}

// typedRun drives the real engine with a TYPED backfill sink: the per-block
// transform runs on the decode-worker pool, converting each block's events to
// public events and decoding matching records into *T (so the expensive typed
// UnmarshalCBOR is parallel, not serial in the consumer). The reassembler then
// hands the ready *TypedBatch[T] to Emit in seq order. The live-tail/error
// closures assemble typed batches in the (single) emit goroutine — fine because
// the live path is low-volume — so the consumer sees one uniform typed stream
// across the backfill→live cutover.
func typedRun[T any, PT interface {
	*T
	UnmarshalCBOR([]byte) error
}](ctx context.Context, re *realEngine, collection string, yield func(*TypedBatch[T], error) bool) {
	stopped := false
	bf := iclient.BackfillSink{
		Transform: func(_ int, evs []iclient.Event) any {
			if len(evs) == 0 {
				return nil
			}
			// Convert to public events (slab-allocated) then decode records into T,
			// all on this worker goroutine. One *TypedBatch[T] per block, boxed as
			// any so internal/client never names T.
			return assembleTyped[T, PT](toPublicEvents(evs), collection)
		},
		Emit: func(res iclient.EntryResult) bool {
			if stopped {
				return false
			}
			tb, _ := res.Payload.(*TypedBatch[T])
			if tb == nil {
				return true // empty/filtered block
			}
			if !yield(tb, nil) {
				stopped = true
				return false
			}
			return true
		},
	}
	emitBatch := func(batch []iclient.Event) bool {
		if stopped {
			return false
		}
		if !yield(assembleTyped[T, PT](toPublicEvents(batch), collection), nil) {
			stopped = true
			return false
		}
		return true
	}
	emitErr := func(err error) bool {
		if stopped {
			return false
		}
		if !yield(nil, err) {
			stopped = true
			return false
		}
		return true
	}
	re.driveRun(ctx, emitBatch, emitErr, bf)
}

// assembleTyped turns a slice of public events into a TypedBatch[T], decoding
// each create/update commit of collection into a *T drawn from a per-batch slab.
// It is the single shared assembler used by both the worker-parallel fast path
// and the serial fallback, so their output is identical by construction.
func assembleTyped[T any, PT interface {
	*T
	UnmarshalCBOR([]byte) error
}](src []Event, collection string) *TypedBatch[T] {
	out := make([]TypedEvent[T], len(src))
	// One records slab per batch, presized and indexed by a counter so the
	// &records[i] handed to each TypedEvent stays stable (never grown). It is
	// dropped to GC with the batch and never pooled — same lifetime rule as the
	// rest of the client's slabs.
	records := make([]T, len(src))
	ri := 0
	for i := range src {
		ev := src[i]
		te := TypedEvent[T]{Event: ev}
		if decodable(ev, collection) {
			rec := &records[ri]
			ri++
			if derr := PT(rec).UnmarshalCBOR(ev.Commit.RecordCBOR); derr != nil {
				te.DecodeErr = fmt.Errorf("jetstream: decode %s record (rkey=%s seq=%d): %w",
					collection, ev.Commit.Rkey, ev.Seq, derr)
			} else {
				te.Record = rec
			}
		}
		out[i] = te
	}
	return &TypedBatch[T]{events: out}
}

// decodable reports whether ev is a create/update commit of the requested
// collection carrying record bytes — i.e. a row TypedEvents should decode into
// T. Deletes (no record), other collections, and non-commit events are not.
func decodable(ev Event, collection string) bool {
	return ev.Kind == KindCommit &&
		ev.Commit != nil &&
		(ev.Commit.Operation == OpCreate || ev.Commit.Operation == OpUpdate) &&
		ev.Commit.Collection == collection &&
		len(ev.Commit.RecordCBOR) > 0
}

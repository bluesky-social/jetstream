// handler.go provides SegmentHandler, the atmos
// backfill.Handler that walks each downloaded repo's MST and emits
// one segment.KindCreate event per record into the shared
// ingest.Writer.

package backfill

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/repo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// SegmentHandler walks each downloaded repo's MST and emits one
// KindCreate event per record into the writer. atmos guarantees no
// two HandleRepo calls overlap for the same DID; ingest.Writer is
// safe across DIDs.
type SegmentHandler struct {
	writer        *ingest.Writer
	logger        *slog.Logger
	now           func() time.Time
	metrics       *Metrics
	dropMetrics   *ingest.DropMetrics
	onWriterError func(error)
	completions   *completionBatcher
}

const segmentHandlerAppendBatchSize = 1024

// Compile-time assertion.
var _ atmosbackfill.Handler = (*SegmentHandler)(nil)

// NewSegmentHandler returns a handler that writes events into writer.
// nil logger uses slog.Default(); writer is required.
func NewSegmentHandler(writer *ingest.Writer, logger *slog.Logger, m *Metrics) *SegmentHandler {
	if writer == nil {
		panic("backfill: NewSegmentHandler: writer is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &SegmentHandler{
		writer:  writer,
		logger:  logger.With(slog.String("component", "backfill/handler")),
		now:     time.Now,
		metrics: m,
	}
}

// SetDropMetrics wires the shared ingest drop counter family.
// Construction-time wiring, like SetCompletionBatcher.
func (h *SegmentHandler) SetDropMetrics(m *ingest.DropMetrics) {
	h.dropMetrics = m
}

// SetCompletionBatcher queues repo completion watermarks for durable batch
// metadata. It is intended for construction-time wiring before HandleRepo runs.
func (h *SegmentHandler) SetCompletionBatcher(b *completionBatcher) {
	h.completions = b
}

// HandleRepo emits one segment event per record in r.Tree. The
// WitnessedAt timestamp is the same for every event in this repo: it
// is the wall-clock instant at which jetstream observed this repo.
// Per-record timestamps would imply a false ordering.
func (h *SegmentHandler) HandleRepo(ctx context.Context, did atmos.DID, r *repo.Repo, commit *repo.Commit) error {
	return h.handleRepo(ctx, did, r, commit, segment.KindCreate, false)
}

// HandleRepoResync emits a whole-repo replacement stream for a previously
// failed repo retry: one KindSync DID tombstone followed by KindCreateResync
// rows for the downloaded repo's current records. It validates every
// replacement row before appending anything so a malformed CAR cannot leave a
// durable tombstone without its replacement rows.
func (h *SegmentHandler) HandleRepoResync(ctx context.Context, did atmos.DID, r *repo.Repo, commit *repo.Commit) error {
	return h.handleRepo(ctx, did, r, commit, segment.KindCreateResync, true)
}

func (h *SegmentHandler) handleRepo(ctx context.Context, did atmos.DID, r *repo.Repo, commit *repo.Commit, materializedKind segment.Kind, prependSync bool) error {
	return obs.Span(ctx, func(ctx context.Context) (retErr error) {
		trace.SpanFromContext(ctx).SetAttributes(attribute.String("did", string(did)))
		start := time.Now()
		defer func() { h.metrics.observeHandleRepo(start, retErr) }()

		// #197 rev gate: every archived row shares commit.Rev, and rev
		// ordering drives merge/compaction decisions, so an invalid rev
		// fails the whole repo — visible in failed-repo diagnostics and
		// retried by the retry loop — rather than silently archiving
		// unorderable rows. atmos's repo loader already rejects invalid
		// non-empty revs before HandleRepo runs; the empty rev is the
		// reachable case here.
		if _, err := atmos.ParseTID(commit.Rev); err != nil {
			h.dropMetrics.IncDropped(ingest.DropSourceBackfill, ingest.DropReasonInvalidRev)
			return fmt.Errorf("backfill: did=%s: commit rev is not a valid TID: %w", did, err)
		}

		now := h.now().UTC()
		witnessedAt := now.UnixMicro()
		appended := false
		lastSeq := uint64(0)
		batch := make([]segment.Event, 0, segmentHandlerAppendBatchSize)

		if prependSync {
			ev, err := syncTombstoneEvent(did, commit, witnessedAt, now)
			if err != nil {
				return err
			}
			batch = append(batch, ev)
			if err := h.validateRepoMaterializations(ctx, did, r, commit, materializedKind, witnessedAt); err != nil {
				return err
			}
		}

		flushBatch := func() error {
			if len(batch) == 0 {
				return nil
			}
			if err := h.writer.AppendBatch(ctx, batch); err != nil {
				err = fmt.Errorf("backfill: did=%s append batch: %w", did, err)
				h.abortOnWriterError(err)
				return err
			}
			lastSeq = batch[len(batch)-1].Seq
			appended = true
			batch = batch[:0]
			return nil
		}

		if err := r.Tree.Walk(func(key string, cid cbor.CID) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			collection, rkey, ok := h.validRecordPath(ctx, did, key)
			if !ok {
				return nil
			}
			payload, err := r.Store.GetBlock(cid)
			if err != nil {
				return fmt.Errorf("backfill: did=%s get block %s/%s: %w", did, collection, rkey, err)
			}

			ev := segment.Event{
				WitnessedAt: witnessedAt,
				Kind:        materializedKind,
				DID:         string(did),
				Collection:  collection,
				Rkey:        rkey,
				Rev:         commit.Rev,
				Payload:     payload,
			}
			if err := segment.ValidateEvent(ev); err != nil {
				if errors.Is(err, segment.ErrFieldTooLong) {
					h.dropMetrics.IncDropped(ingest.DropSourceBackfill, ingest.DropReasonFieldTooLong)
					h.logger.WarnContext(ctx, "dropped unarchivable upstream record",
						"did", string(did),
						"did_len", len(string(did)),
						"collection_len", len(collection),
						"rkey_len", len(rkey),
						"rev_len", len(commit.Rev),
						"payload_len", len(payload),
						"err", err,
					)
					return nil
				}
				return fmt.Errorf("backfill: did=%s invalid segment event %s/%s: %w", did, collection, rkey, err)
			}
			batch = append(batch, ev)
			if len(batch) >= segmentHandlerAppendBatchSize {
				return flushBatch()
			}
			return nil
		}); err != nil {
			return err
		}
		if err := flushBatch(); err != nil {
			return err
		}
		if h.completions != nil {
			h.completions.RecordWatermark(did, lastSeq, appended)
		}
		return nil
	})
}

// validRecordPath applies the #197 per-record path gate on the
// appending walk: a spec-invalid MST key drops that record — counted
// on the shared drop family, no log line (hostile CARs must not
// drive log volume; the labeled counter is the operator signal) —
// while siblings archive normally. Returns ok=false for a drop.
func (h *SegmentHandler) validRecordPath(ctx context.Context, did atmos.DID, key string) (collection, rkey string, ok bool) {
	collection, rkey, reason := splitRecordPath(key)
	if reason == "" {
		return collection, rkey, true
	}
	h.dropMetrics.IncDropped(ingest.DropSourceBackfill, reason)
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.AddEvent("dropped_invalid_record", attributeReasonDID(reason, did))
	}
	return "", "", false
}

// splitRecordPath splits an MST key and validates both halves against
// the atproto specs (NSID + record key — the same checks as
// atmos.ParseRepoPath). The MST layer's own key charset is broader
// than the specs, and mst.LoadTree decodes keys from a downloaded CAR
// without spec validation, so a hostile PDS can put arbitrary
// MST-legal keys in front of this walk. reason == "" means valid.
func splitRecordPath(key string) (collection, rkey string, reason ingest.DropReason) {
	collection, rkey, found := strings.Cut(key, "/")
	if !found {
		return "", "", ingest.DropReasonInvalidCollection
	}
	if _, err := atmos.ParseNSID(collection); err != nil {
		return collection, rkey, ingest.DropReasonInvalidCollection
	}
	if _, err := atmos.ParseRecordKey(rkey); err != nil {
		return collection, rkey, ingest.DropReasonInvalidRkey
	}
	return collection, rkey, ""
}

func attributeReasonDID(reason ingest.DropReason, did atmos.DID) trace.SpanStartEventOption {
	return trace.WithAttributes(
		attribute.String("reason", string(reason)),
		attribute.String("did", string(did)),
	)
}

// validateRepoMaterializations pre-checks every replacement row a
// resync will append, so a malformed CAR cannot leave a durable
// KindSync tombstone without its replacement rows. Droppable records
// (spec-invalid paths, over-wide fields) are SKIPPED here without
// counting — the appending walk makes the same classification and
// owns the metric, keeping each drop counted exactly once.
func (h *SegmentHandler) validateRepoMaterializations(ctx context.Context, did atmos.DID, r *repo.Repo, commit *repo.Commit, materializedKind segment.Kind, witnessedAt int64) error {
	return r.Tree.Walk(func(key string, cid cbor.CID) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		collection, rkey, reason := splitRecordPath(key)
		if reason != "" {
			return nil
		}
		payload, err := r.Store.GetBlock(cid)
		if err != nil {
			return fmt.Errorf("backfill: did=%s get block %s/%s: %w", did, collection, rkey, err)
		}
		ev := segment.Event{
			WitnessedAt: witnessedAt,
			Kind:        materializedKind,
			DID:         string(did),
			Collection:  collection,
			Rkey:        rkey,
			Rev:         commit.Rev,
			Payload:     payload,
		}
		if err := segment.ValidateEvent(ev); err != nil && !errors.Is(err, segment.ErrFieldTooLong) {
			return fmt.Errorf("backfill: did=%s invalid segment event %s/%s: %w", did, collection, rkey, err)
		}
		return nil
	})
}

func syncTombstoneEvent(did atmos.DID, commit *repo.Commit, witnessedAt int64, now time.Time) (segment.Event, error) {
	payload, err := (&comatproto.SyncSubscribeRepos_Sync{
		DID:  string(did),
		Rev:  commit.Rev,
		Time: now.Format(time.RFC3339Nano),
	}).MarshalCBOR()
	if err != nil {
		return segment.Event{}, fmt.Errorf("backfill: did=%s marshal synthetic sync: %w", did, err)
	}
	ev := segment.Event{
		WitnessedAt: witnessedAt,
		Kind:        segment.KindSync,
		DID:         string(did),
		Rev:         commit.Rev,
		Payload:     payload,
	}
	if err := segment.ValidateEvent(ev); err != nil {
		return segment.Event{}, fmt.Errorf("backfill: did=%s invalid synthetic sync: %w", did, err)
	}
	return ev, nil
}

func (h *SegmentHandler) abortOnWriterError(err error) {
	if h.onWriterError != nil {
		h.onWriterError(err)
	}
}

// Package backfill: handler.go provides SegmentHandler, the atmos
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
			collection, rkey, err := splitMSTKey(key)
			if err != nil {
				return fmt.Errorf("backfill: did=%s: %w", did, err)
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
					h.metrics.incDroppedRecords()
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

func (h *SegmentHandler) validateRepoMaterializations(ctx context.Context, did atmos.DID, r *repo.Repo, commit *repo.Commit, materializedKind segment.Kind, witnessedAt int64) error {
	return r.Tree.Walk(func(key string, cid cbor.CID) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		collection, rkey, err := splitMSTKey(key)
		if err != nil {
			return fmt.Errorf("backfill: did=%s: %w", did, err)
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

// splitMSTKey splits "collection/rkey" into its parts. The MST
// validates the key shape on insert (atmos/mst.IsValidMstKey), so a
// malformed key here is a data-integrity violation we surface
// rather than swallow.
func splitMSTKey(key string) (collection, rkey string, err error) {
	idx := strings.IndexByte(key, '/')
	if idx <= 0 || idx == len(key)-1 {
		return "", "", fmt.Errorf("malformed MST key %q (expected collection/rkey)", key)
	}
	if strings.Contains(key[idx+1:], "/") {
		return "", "", fmt.Errorf("MST key %q has more than one slash", key)
	}
	return key[:idx], key[idx+1:], nil
}

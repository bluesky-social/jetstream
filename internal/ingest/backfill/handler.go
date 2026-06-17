// Package backfill: handler.go provides SegmentHandler, the atmos
// backfill.Handler that walks each downloaded repo's MST and emits
// one segment.KindCreate event per record into the shared
// ingest.Writer.
package backfill

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/obs"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/atmos"
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
// IndexedAt timestamp is the same for every event in this repo: it
// is the wall-clock instant at which jetstream observed this repo.
// Per-record timestamps would imply a false ordering.
func (h *SegmentHandler) HandleRepo(ctx context.Context, did atmos.DID, r *repo.Repo, commit *repo.Commit) error {
	return obs.Span(ctx, func(ctx context.Context) (retErr error) {
		trace.SpanFromContext(ctx).SetAttributes(attribute.String("did", string(did)))
		start := time.Now()
		defer func() { h.metrics.observeHandleRepo(start, retErr) }()

		indexedAt := h.now().UnixMicro()
		appended := false
		lastSeq := uint64(0)
		batch := make([]segment.Event, 0, segmentHandlerAppendBatchSize)

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
				return fmt.Errorf("backfill: did=%s get block %s/%s: %w: %w", did, collection, rkey, io.ErrUnexpectedEOF, err)
			}

			ev := segment.Event{
				IndexedAt:  indexedAt,
				Kind:       segment.KindCreate,
				DID:        string(did),
				Collection: collection,
				Rkey:       rkey,
				Rev:        commit.Rev,
				Payload:    payload,
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

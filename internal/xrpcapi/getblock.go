package xrpcapi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/bluesky-social/jetstream/api/jetstream"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/atmos/xrpcserver"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// getBlockHandler serves one sealed-segment block as its raw stored zstd frame.
// Like getSegmentHandler it implements xrpcserver.Handler directly so it can use
// http.ServeContent for conditional/Range handling. The block bytes, block
// count, and ETag are all derived from a single SegmentFile — never the
// manifest — so a concurrent compaction rewrite cannot splice generations.
type getBlockHandler struct {
	opener               SegmentOpener
	logger               *slog.Logger
	maxDuration          time.Duration
	compactionCacheGrace time.Duration
	compactionDeadline   CompactionDeadline
	metrics              *Metrics
	tracer               trace.Tracer
}

func (h *getBlockHandler) ServeXRPC(ctx context.Context, w http.ResponseWriter, r *xrpcserver.Request) error {
	start := time.Now()
	var span trace.Span
	if h.tracer != nil {
		_, span = h.tracer.Start(ctx, "getBlock")
		defer span.End()
	}
	result := resultError
	served := 0
	defer func() { h.metrics.observeServe(result, served, time.Since(start).Seconds()) }()

	fail := func(res string, err error) error {
		result = res
		if span != nil {
			span.SetAttributes(attribute.String("result", res))
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return err
	}

	name, err := r.Params.String("segment")
	if err != nil {
		return fail(resultBadRequest, err)
	}
	idx, ok := ingest.ParseSegmentIndex(name)
	if !ok {
		return fail(resultBadRequest, xrpcserver.InvalidRequest("malformed segment name"))
	}
	blockIdx64, err := r.Params.Int64("blockIndex")
	if err != nil {
		return fail(resultBadRequest, err)
	}
	if blockIdx64 < 0 {
		return fail(resultBadRequest, xrpcserver.InvalidRequest("blockIndex must be >= 0"))
	}
	blockIdx := int(blockIdx64)
	if span != nil {
		span.SetAttributes(attribute.Int64("segment.idx", int64(idx)),
			attribute.Int("block.index", blockIdx))
	}

	ctx, cancel := responseDeadline(ctx, w, h.maxDuration)
	defer cancel()

	// The block bytes, block count, and ETag MUST all come from this single
	// SegmentFile. Never take the offset or checksum from the in-memory
	// manifest: during a compaction rename→refresh window it can be stale, and
	// mixing manifest metadata with file reads would splice two file
	// generations together.
	f, err := h.opener.OpenSegment(ctx, idx)
	if errors.Is(err, ErrSegmentNotFound) {
		return fail(resultNotFound, &xrpc.Error{
			StatusCode: http.StatusNotFound, Name: jetstream.ErrJetstreamGetBlock_SegmentNotFound, Message: "segment not found",
		})
	}
	if err != nil {
		h.logger.Error("getBlock: open sealed file failed",
			slog.String("name", name), slog.Any("err", err))
		return fail(resultError, xrpcserver.InternalError("failed to open segment"))
	}
	defer func() { _ = f.Close() }()

	hdr := f.Header()
	if hdr.FooterOffset > uint64(f.Size()) {
		h.logger.Error("getBlock: sealed header extends past file",
			slog.String("name", name),
			slog.Uint64("footer_offset", hdr.FooterOffset), slog.Int64("file_size", f.Size()))
		return fail(resultError, xrpcserver.InternalError("failed to read segment footer"))
	}
	if blockIdx >= int(hdr.BlockCount) {
		return fail(resultNotFound, &xrpc.Error{
			StatusCode: http.StatusNotFound, Name: jetstream.ErrJetstreamGetBlock_BlockNotFound, Message: "block index out of range",
		})
	}

	var content io.ReadSeeker
	var contentSize int64
	if bf, ok := f.(blockFramer); ok {
		// Each block is its own object: HEAD reads nothing, and GET reads
		// the whole verified object rather than a footer entry and a range.
		if isHeadRequest(r.HTTPReq) {
			section := bf.BlockFrameSection(blockIdx)
			content, contentSize = section, section.Size()
		} else {
			frame, err := bf.BlockFrame(blockIdx)
			if err != nil {
				h.logger.Error("getBlock: read block object failed",
					slog.String("name", name), slog.Int("block", blockIdx), slog.Any("err", err))
				return fail(resultError, xrpcserver.InternalError("failed to read block"))
			}
			content, contentSize = bytes.NewReader(frame), int64(len(frame))
		}
	} else if isHeadRequest(r.HTTPReq) {
		// HEAD needs only the validated footer entry and virtual frame range;
		// do not allocate a buffer proportional to the compressed frame.
		section, err := segment.BlockFrameSection(f, hdr, blockIdx)
		if err != nil {
			h.logger.Error("getBlock: validate block frame failed",
				slog.String("name", name), slog.Int("block", blockIdx), slog.Any("err", err))
			return fail(resultError, xrpcserver.InternalError("failed to read block"))
		}
		content = section
		contentSize = section.Size()
	} else {
		frame, err := segment.ReadBlockFrame(f, hdr, blockIdx)
		if err != nil {
			h.logger.Error("getBlock: read block frame failed",
				slog.String("name", name), slog.Int("block", blockIdx), slog.Any("err", err))
			return fail(resultError, xrpcserver.InternalError("failed to read block"))
		}
		content = bytes.NewReader(frame)
		contentSize = int64(len(frame))
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", fmt.Sprintf("%q", checksumHex(hdr.Checksum)+":"+fmt.Sprint(blockIdx)))
	w.Header().Set("Cache-Control", cacheControlHeader(cacheLifetime(
		time.Now(), h.compactionCacheGrace, h.compactionDeadline,
	)))

	if span != nil {
		span.SetAttributes(attribute.Int64("block.compressed_size", contentSize))
	}

	// ServeContent handles If-None-Match->304, Range, and Content-Length. After
	// this point the response may be partially written, so per the Handler
	// contract we return nil.
	rec := &blockResponseRecorder{ResponseWriter: w}
	http.ServeContent(rec, contentRequest(r.HTTPReq), name, f.ModTime(), content)
	result = resultOK
	if rec.status >= http.StatusBadRequest {
		result = resultError
	}
	if rec.status == http.StatusOK {
		served = rec.bytes
	}
	if span != nil {
		span.SetAttributes(attribute.String("result", result))
		if result == resultOK {
			span.SetStatus(codes.Ok, "")
		} else {
			span.SetStatus(codes.Error, fmt.Sprintf("http status %d", rec.status))
		}
	}
	return nil
}

type blockResponseRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *blockResponseRecorder) WriteHeader(code int) {
	if r.status != 0 {
		return
	}
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *blockResponseRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

func (r *blockResponseRecorder) ReadFrom(src io.Reader) (int64, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	if rf, ok := r.ResponseWriter.(io.ReaderFrom); ok {
		n, err := rf.ReadFrom(src)
		r.bytes += int(n)
		return n, err
	}
	n, err := io.Copy(r.ResponseWriter, src)
	r.bytes += int(n)
	return n, err
}

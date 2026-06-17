package xrpcapi

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/bluesky-social/jetstream-v2/api/jetstream"
	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/atmos/xrpcserver"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// getBlockHandler serves one sealed-segment block as its raw stored zstd frame.
// Like getSegmentHandler it implements xrpcserver.Handler directly so it can use
// http.ServeContent for conditional/Range handling. The block bytes, block
// count, and ETag are all derived from a single freshly-opened fd — never the
// manifest — so a concurrent compaction rewrite cannot splice generations.
type getBlockHandler struct {
	src         SegmentSource
	logger      *slog.Logger
	cacheMaxAge time.Duration
	metrics     *Metrics
	tracer      trace.Tracer
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

	ref, ok := h.src.SegmentByIdx(idx)
	if !ok {
		return fail(resultNotFound, &xrpc.Error{
			StatusCode: http.StatusNotFound, Name: jetstream.ErrJetstreamGetBlock_SegmentNotFound, Message: "segment not found",
		})
	}

	// The block bytes, block count, and ETag MUST all come from this single
	// freshly-opened fd. Never take the offset or checksum from the in-memory
	// manifest: during a compaction rename→refresh window it can be stale, and
	// mixing manifest metadata with on-disk reads would splice two file
	// generations together.
	f, err := os.Open(ref.Path)
	if err != nil {
		h.logger.Error("getBlock: open sealed file failed",
			slog.String("name", name), slog.String("path", ref.Path), slog.Any("err", err))
		return fail(resultError, xrpcserver.InternalError("failed to open segment"))
	}
	defer func() { _ = f.Close() }()

	hdr, err := segment.ReadSealedHeader(f)
	if err != nil {
		h.logger.Error("getBlock: read sealed header failed",
			slog.String("name", name), slog.String("path", ref.Path), slog.Any("err", err))
		return fail(resultError, xrpcserver.InternalError("failed to read segment header"))
	}
	if blockIdx >= int(hdr.BlockCount) {
		return fail(resultNotFound, &xrpc.Error{
			StatusCode: http.StatusNotFound, Name: jetstream.ErrJetstreamGetBlock_BlockNotFound, Message: "block index out of range",
		})
	}

	frame, err := segment.ReadBlockFrame(f, hdr, blockIdx)
	if err != nil {
		h.logger.Error("getBlock: read block frame failed",
			slog.String("name", name), slog.Int("block", blockIdx), slog.Any("err", err))
		return fail(resultError, xrpcserver.InternalError("failed to read block"))
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", fmt.Sprintf("%q", checksumHex(hdr.Checksum)+":"+fmt.Sprint(blockIdx)))
	w.Header().Set("Cache-Control", cacheControlHeader(h.cacheMaxAge))

	if span != nil {
		span.SetAttributes(attribute.Int("block.compressed_size", len(frame)))
	}

	// ServeContent handles If-None-Match->304, Range, and Content-Length. After
	// this point the response may be partially written, so per the Handler
	// contract we return nil.
	rec := &blockResponseRecorder{ResponseWriter: w}
	http.ServeContent(rec, r.HTTPReq, name, ref.ModTime, bytes.NewReader(frame))
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

package xrpcapi

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/atmos/xrpcserver"
)

// getSegmentHandler serves a whole sealed segment file. It implements
// xrpcserver.Handler directly (rather than xrpcserver.RawQuery) because it
// needs the underlying *http.Request to drive http.ServeContent's Range and
// conditional-request handling.
type getSegmentHandler struct {
	src         SegmentSource
	logger      *slog.Logger
	cacheMaxAge time.Duration
}

func (h *getSegmentHandler) ServeXRPC(ctx context.Context, w http.ResponseWriter, r *xrpcserver.Request) error {
	name, err := r.Params.String("name")
	if err != nil {
		return err // InvalidRequest (400) for missing param
	}

	idx, ok := ingest.ParseSegmentIndex(name)
	if !ok {
		return xrpcserver.InvalidRequest("malformed segment name")
	}

	ref, ok := h.src.SegmentByIdx(idx)
	if !ok {
		// Error name must match the lexicon's declared SegmentNotFound, not
		// the generic NotFound, so clients matching on the published name work.
		return &xrpc.Error{StatusCode: http.StatusNotFound, Name: "SegmentNotFound", Message: "segment not found"}
	}

	// Open and stat BEFORE writing anything, so failures become XRPC error
	// envelopes rather than a corrupt partial 200 response.
	f, err := os.Open(ref.Path)
	if err != nil {
		// Manifest believes this segment exists but we cannot open it: a
		// real inconsistency (rotation/deletion race). Surface it loudly.
		h.logger.Error("getSegment: open sealed file failed",
			slog.String("name", name), slog.String("path", ref.Path),
			slog.Any("err", err))
		return xrpcserver.InternalError("failed to open segment")
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		h.logger.Error("getSegment: stat sealed file failed",
			slog.String("name", name), slog.String("path", ref.Path),
			slog.Any("err", err))
		return xrpcserver.InternalError("failed to stat segment")
	}
	var header [256]byte
	if _, err := f.ReadAt(header[:], 0); err != nil {
		h.logger.Error("getSegment: read sealed header failed",
			slog.String("name", name), slog.String("path", ref.Path),
			slog.Any("err", err))
		return xrpcserver.InternalError("failed to read segment header")
	}
	checksum := binary.LittleEndian.Uint64(header[4:12])
	if checksum == 0 {
		return xrpcserver.InternalError("segment is not sealed")
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	// A strong ETag is the value wrapped in double quotes per RFC 9110.
	w.Header().Set("ETag", fmt.Sprintf("%q", checksumHex(checksum)))
	w.Header().Set("Cache-Control", cacheControlHeader(h.cacheMaxAge))

	// ServeContent handles Range, Accept-Ranges, Content-Length,
	// If-None-Match->304, and If-Range, and triggers sendfile(2) via the
	// statusRecorder.ReadFrom delegation. Per the xrpcserver.Handler contract
	// we MUST return nil after this point: the response may already be
	// partially written, so an error envelope is no longer possible.
	http.ServeContent(w, r.HTTPReq, name, info.ModTime(), f)
	return nil
}

func cacheControlHeader(maxAge time.Duration) string {
	if maxAge <= 0 {
		return "public, no-cache"
	}
	seconds := int64(maxAge / time.Second)
	if maxAge%time.Second != 0 {
		seconds++
	}
	return "public, max-age=" + strconv.FormatInt(seconds, 10)
}

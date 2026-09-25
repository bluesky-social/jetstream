package xrpcapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/atmos/xrpcserver"
)

// getSegmentHandler serves a whole sealed segment file. It implements
// xrpcserver.Handler directly (rather than xrpcserver.RawQuery) because it
// needs the underlying *http.Request to drive http.ServeContent's Range and
// conditional-request handling.
type getSegmentHandler struct {
	opener               SegmentOpener
	logger               *slog.Logger
	compactionCacheGrace time.Duration
	compactionDeadline   CompactionDeadline
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

	// Open BEFORE writing anything, so failures become XRPC error envelopes
	// rather than a corrupt partial 200 response. Download validators come
	// from the file we actually serve, never the manifest: during a
	// compaction rename→refresh window the manifest ETag is stale, and an
	// If-Range match against it would let a resuming client splice two file
	// generations together.
	f, err := h.opener.OpenSegment(ctx, idx)
	if errors.Is(err, ErrSegmentNotFound) {
		// Error name must match the lexicon's declared SegmentNotFound, not
		// the generic NotFound, so clients matching on the published name work.
		return &xrpc.Error{StatusCode: http.StatusNotFound, Name: "SegmentNotFound", Message: "segment not found"}
	}
	if err != nil {
		// The segment exists but cannot be opened: a real inconsistency
		// (rotation/deletion race, corrupt header). Surface it loudly.
		h.logger.Error("getSegment: open sealed file failed",
			slog.String("name", name), slog.Any("err", err))
		return xrpcserver.InternalError("failed to open segment")
	}
	defer func() { _ = f.Close() }()
	hdr := f.Header()

	w.Header().Set("Content-Type", "application/octet-stream")
	// A strong ETag is the value wrapped in double quotes per RFC 9110.
	w.Header().Set("ETag", fmt.Sprintf("%q", checksumHex(hdr.Checksum)))
	w.Header().Set("Cache-Control", cacheControlHeader(cacheLifetime(
		time.Now(), h.compactionCacheGrace, h.compactionDeadline,
	)))

	// ServeContent handles Range, Accept-Ranges, Content-Length,
	// If-None-Match->304, and If-Range, and triggers sendfile(2) via the
	// statusRecorder.ReadFrom delegation. Per the xrpcserver.Handler contract
	// we MUST return nil after this point: the response may already be
	// partially written, so an error envelope is no longer possible.
	http.ServeContent(w, contentRequest(r.HTTPReq), name, f.ModTime(), f.Content())
	return nil
}

func cacheControlHeader(maxAge time.Duration) string {
	seconds := int64(maxAge / time.Second)
	if seconds <= 0 {
		return "public, no-cache"
	}
	return "public, max-age=" + strconv.FormatInt(seconds, 10)
}

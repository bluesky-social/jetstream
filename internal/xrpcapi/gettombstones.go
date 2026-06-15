package xrpcapi

import (
	"context"
	"net/http"
	"strconv"

	"github.com/bluesky-social/jetstream-v2/internal/overlay"
	"github.com/jcalabro/atmos/xrpcserver"
)

// OverlaySource is the read-only surface the getTombstones handler needs.
// *overlay.Cache satisfies it; tests pass a fake.
type OverlaySource interface {
	Current() *overlay.Blob
	ObserveServe(n int)
}

func newGetTombstonesHandler(src OverlaySource) xrpcserver.Handler {
	return xrpcserver.RawQuery(func(ctx context.Context, p xrpcserver.Params, w http.ResponseWriter) error {
		blob := src.Current()
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("ETag", blob.ETag)
		// Not CDN-cacheable in practice (the overlay changes continuously);
		// no-cache lets the occasional If-None-Match revalidation still 304.
		w.Header().Set("Cache-Control", "no-cache")
		// Surface W/M as headers so the future query plan can read the
		// coverage envelope without decompressing the body.
		w.Header().Set("Jetstream-Overlay-Watermark", strconv.FormatUint(blob.Watermark, 10))
		w.Header().Set("Jetstream-Overlay-Max-Seq", strconv.FormatUint(blob.MaxSeq, 10))
		_, _ = w.Write(blob.Bytes)
		src.ObserveServe(len(blob.Bytes))
		return nil
	})
}

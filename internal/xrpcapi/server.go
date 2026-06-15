// Package xrpcapi exposes jetstream's sealed segment files over XRPC
// (atproto's HTTP RPC framework). It is the only package that depends on
// the atmos xrpcserver; the manifest and segment packages stay
// transport-agnostic.
package xrpcapi

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/manifest"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/atmos/xrpcserver"
)

// SegmentSource is the read-only manifest surface xrpcapi needs. The
// concrete *manifest.Manifest satisfies it; tests can pass a fake.
type SegmentSource interface {
	SegmentByIdx(idx uint64) (manifest.SegmentFileRef, bool)
	ListFrom(startIdx uint64, limit int) ([]manifest.SegmentListEntry, uint64, bool)
}

// ReadyFunc is called at the start of every XRPC request. Return an error
// when the archive is not safe to expose yet, for example during bootstrap
// or manifest startup.
type ReadyFunc func(context.Context) error

// Server builds the XRPC handler tree for the jetstream lexicons.
type Server struct {
	src     SegmentSource
	logger  *slog.Logger
	xrpc    *xrpcserver.Server
	overlay OverlaySource
}

// New constructs the XRPC server and registers all jetstream NSIDs.
func New(src SegmentSource, logger *slog.Logger) *Server {
	return NewWithReadyAndCacheAndOverlay(src, logger, nil, 0, nil)
}

// NewWithReady constructs the XRPC server and registers all jetstream NSIDs,
// guarding each request with ready when ready is non-nil.
func NewWithReady(src SegmentSource, logger *slog.Logger, ready ReadyFunc) *Server {
	return NewWithReadyAndCacheAndOverlay(src, logger, ready, 0, nil)
}

// NewWithReadyAndCache constructs the XRPC server with a readiness gate and
// a getSegment Cache-Control max-age. cacheMaxAge <= 0 disables caching.
func NewWithReadyAndCache(src SegmentSource, logger *slog.Logger, ready ReadyFunc, cacheMaxAge time.Duration) *Server {
	return NewWithReadyAndCacheAndOverlay(src, logger, ready, cacheMaxAge, nil)
}

// NewWithReadyAndCacheAndOverlay constructs the XRPC server with a readiness
// gate, a getSegment Cache-Control max-age, and an optional overlay source.
// When ov is non-nil, the getTombstones NSID is registered behind the same
// readiness gate; when nil, getTombstones is not exposed.
func NewWithReadyAndCacheAndOverlay(src SegmentSource, logger *slog.Logger, ready ReadyFunc, cacheMaxAge time.Duration, ov OverlaySource) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{src: src, logger: logger, xrpc: &xrpcserver.Server{}, overlay: ov}
	s.xrpc.HandleQuery("network.bsky.jetstream.getSegment", withReady(ready, &getSegmentHandler{
		src:         src,
		logger:      logger,
		cacheMaxAge: cacheMaxAge,
	}))
	s.xrpc.HandleQuery("network.bsky.jetstream.listSegments", withReady(ready, newListSegmentsHandler(src)))
	if ov != nil {
		s.xrpc.HandleQuery("network.bsky.jetstream.getTombstones", withReady(ready, newGetTombstonesHandler(ov)))
	}
	return s
}

// Handler returns the http.Handler that routes /xrpc/{nsid} requests.
// Mount it at "/xrpc/" on the public mux.
func (s *Server) Handler() http.Handler {
	return s.xrpc
}

func withReady(ready ReadyFunc, h xrpcserver.Handler) xrpcserver.Handler {
	if ready == nil {
		return h
	}
	return xrpcserver.HandlerFunc(func(ctx context.Context, w http.ResponseWriter, r *xrpcserver.Request) error {
		if err := ready(ctx); err != nil {
			return &xrpc.Error{
				StatusCode: http.StatusServiceUnavailable,
				Name:       "ServiceUnavailable",
				Message:    fmt.Sprintf("service not ready: %s", err.Error()),
			}
		}
		return h.ServeXRPC(ctx, w, r)
	})
}

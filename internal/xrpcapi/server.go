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

	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/atmos/xrpcserver"
	"go.opentelemetry.io/otel/trace"
)

// SegmentSource is the read-only manifest surface xrpcapi needs. The
// concrete *manifest.Manifest satisfies it; tests can pass a fake.
type SegmentSource interface {
	SegmentByIdx(idx uint64) (manifest.SegmentFileRef, bool)
	ListFrom(startIdx uint64, limit int) ([]manifest.SegmentListEntry, uint64, bool)
	PlanBackfill(manifest.PlanBackfillRequest) (manifest.PlanBackfillResult, error)
}

// ReadyFunc is called at the start of every XRPC request. Return an error
// when the archive is not safe to expose yet, for example during bootstrap
// or manifest startup.
type ReadyFunc func(context.Context) error

// Server builds the XRPC handler tree for the jetstream lexicons.
type Server struct {
	src    SegmentSource
	logger *slog.Logger
	xrpc   *xrpcserver.Server
}

// Config holds the dependencies for the XRPC server. Zero values are valid:
// a nil Logger defaults to slog.Default(); a nil Ready disables the readiness
// gate; a zero CacheMaxAge disables segment/block caching; nil Metrics/Tracer
// make getBlock observability no-ops. Plan must be populated for planBackfill
// to accept non-empty filters.
type Config struct {
	Src         SegmentSource
	Logger      *slog.Logger
	Ready       ReadyFunc
	CacheMaxAge time.Duration
	Plan        PlanConfig
	Metrics     *Metrics
	Tracer      trace.Tracer
}

// New constructs the XRPC server and registers all jetstream NSIDs.
func New(cfg Config) *Server {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{src: cfg.Src, logger: logger, xrpc: &xrpcserver.Server{}}
	s.xrpc.HandleQuery("network.bsky.jetstream.getSegment", withReady(cfg.Ready, &getSegmentHandler{
		src: cfg.Src, logger: logger, cacheMaxAge: cfg.CacheMaxAge,
	}))
	s.xrpc.HandleQuery("network.bsky.jetstream.getBlock", withReady(cfg.Ready, &getBlockHandler{
		src: cfg.Src, logger: logger, cacheMaxAge: cfg.CacheMaxAge,
		metrics: cfg.Metrics, tracer: cfg.Tracer,
	}))
	s.xrpc.HandleQuery("network.bsky.jetstream.listSegments", withReady(cfg.Ready, newListSegmentsHandler(cfg.Src)))
	s.xrpc.HandleProcedure("network.bsky.jetstream.planBackfill", withReady(cfg.Ready, newPlanBackfillHandler(cfg.Src, cfg.Plan)))
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

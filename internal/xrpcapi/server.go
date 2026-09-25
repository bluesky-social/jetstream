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
	PlanSnapshot(manifest.PlanSnapshotRequest) (manifest.PlanSnapshotResult, error)
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
// gate; an unknown or disabled CompactionDeadline disables caching; nil
// Metrics/Tracer make getBlock observability no-ops. Plan must be populated for
// planSnapshot to accept non-empty filters.
type Config struct {
	Src                  SegmentSource
	Logger               *slog.Logger
	Ready                ReadyFunc
	CompactionCacheGrace time.Duration
	CompactionDeadline   CompactionDeadline
	Plan                 PlanConfig
	Metrics              *Metrics
	Tracer               trace.Tracer

	// Dictionary is the v2 subscribe compression dictionary served by
	// getZstdDictionary. Empty Bytes leaves the endpoint unregistered.
	Dictionary DictionaryConfig
}

// New constructs the XRPC server and registers all jetstream NSIDs.
func New(cfg Config) *Server {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{src: cfg.Src, logger: logger, xrpc: &xrpcserver.Server{}}
	s.xrpc.HandleQuery(getSegmentNSID, withReady(cfg.Ready, &getSegmentHandler{
		src: cfg.Src, logger: logger,
		compactionCacheGrace: cfg.CompactionCacheGrace, compactionDeadline: cfg.CompactionDeadline,
	}))
	s.xrpc.HandleQuery(getBlockNSID, withReady(cfg.Ready, &getBlockHandler{
		src: cfg.Src, logger: logger,
		compactionCacheGrace: cfg.CompactionCacheGrace, compactionDeadline: cfg.CompactionDeadline,
		metrics: cfg.Metrics, tracer: cfg.Tracer,
	}))
	s.xrpc.HandleQuery("network.bsky.jetstream.listSegments", withReady(cfg.Ready, newListSegmentsHandler(cfg.Src)))
	s.xrpc.HandleProcedure("network.bsky.jetstream.planSnapshot", withReady(cfg.Ready, newPlanSnapshotHandler(cfg.Src, cfg.Plan)))

	// The v2 subscribe compression dictionary. Deliberately NOT behind the
	// readiness gate: the artifact is compiled in and immutable, and a
	// client warming up during bootstrap should be able to prefetch it.
	if len(cfg.Dictionary.Bytes) > 0 {
		s.xrpc.HandleQuery("network.bsky.jetstream.getZstdDictionary",
			newGetZstdDictionaryHandler(cfg.Dictionary))
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

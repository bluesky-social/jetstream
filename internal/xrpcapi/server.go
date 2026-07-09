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

// ImportConfig wires the bearer-gated timestamp-import endpoints. It is
// optional: a zero ImportConfig (nil Manager) leaves importTimestamps /
// getImportStatus unregistered, so a server built without import support
// returns the framework's default 404 for those NSIDs.
type ImportConfig struct {
	// Manager runs and reports import jobs. nil disables the endpoints.
	Manager ImportManager
	// Token is the bearer secret. Empty means the endpoints are registered but
	// every request is rejected 401 (secure-by-default), matching the design's
	// "disabled -> 401" rule and keeping the wire surface identical whether or
	// not a token is set.
	Token string
	// RunCtx roots submitted jobs' background runs; cancel it on shutdown.
	RunCtx context.Context
}

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
	Import      ImportConfig

	// Dictionary is the /subscribe-v2 compression dictionary served by
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
	s.xrpc.HandleQuery("network.bsky.jetstream.getSegment", withReady(cfg.Ready, &getSegmentHandler{
		src: cfg.Src, logger: logger, cacheMaxAge: cfg.CacheMaxAge,
	}))
	s.xrpc.HandleQuery("network.bsky.jetstream.getBlock", withReady(cfg.Ready, &getBlockHandler{
		src: cfg.Src, logger: logger, cacheMaxAge: cfg.CacheMaxAge,
		metrics: cfg.Metrics, tracer: cfg.Tracer,
	}))
	s.xrpc.HandleQuery("network.bsky.jetstream.listSegments", withReady(cfg.Ready, newListSegmentsHandler(cfg.Src)))
	s.xrpc.HandleProcedure("network.bsky.jetstream.planBackfill", withReady(cfg.Ready, newPlanBackfillHandler(cfg.Src, cfg.Plan)))

	// The /subscribe-v2 compression dictionary. Deliberately NOT behind the
	// readiness gate: the artifact is compiled in and immutable, and a
	// client warming up during bootstrap should be able to prefetch it.
	if len(cfg.Dictionary.Bytes) > 0 {
		s.xrpc.HandleQuery("network.bsky.jetstream.getZstdDictionary",
			newGetZstdDictionaryHandler(cfg.Dictionary))
	}

	// Timestamp-import endpoints (design §8 M6). Registered only when a manager
	// is wired; bearer-gated (401-by-default when no token) and NOT behind the
	// readiness gate — an operator must be able to submit/monitor an import
	// during any phase, and the manager itself refuses work the archive can't
	// take yet.
	if cfg.Import.Manager != nil {
		runCtx := cfg.Import.RunCtx
		if runCtx == nil {
			runCtx = context.Background()
		}
		s.xrpc.HandleProcedure("network.bsky.jetstream.importTimestamps",
			withBearer(cfg.Import.Token, newImportTimestampsHandler(cfg.Import.Manager, runCtx)))
		s.xrpc.HandleQuery("network.bsky.jetstream.getImportStatus",
			withBearer(cfg.Import.Token, newGetImportStatusHandler(cfg.Import.Manager)))
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

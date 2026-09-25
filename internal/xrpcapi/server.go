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

	"github.com/bluesky-social/jetstream/internal/lifecycle"
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

// SeqSyncer brings a pod's view of the archive up to seq before a request
// is answered from it. The disaggregated catalog follower implements it with
// a synchronous tick when seq is past its mirror (design §11.6), so a client
// that learned of seq on a fresher pod is not planned short of it.
type SeqSyncer interface {
	SyncSeq(ctx context.Context, seq uint64) error
}

// DefaultMaxArchiveResponseDuration bounds one getSegment or getBlock
// response in disaggregated mode (JETSTREAM_MAX_ARCHIVE_RESPONSE_DURATION).
const DefaultMaxArchiveResponseDuration = time.Hour

// CheckGCDelay reports whether GC_DELAY outlives every reader of a replaced
// generation's objects (design §11.5): a pod may serve a view up to
// maxViewAge old, and an open response may read from it for up to
// maxResponse more, so objects must survive both plus a margin for clock
// skew and scheduling.
func CheckGCDelay(gcDelay, maxViewAge, maxResponse time.Duration) error {
	const margin = 10 * time.Minute
	if need := maxViewAge + maxResponse + margin; gcDelay <= need {
		return fmt.Errorf("GC delay %s must exceed max view age %s + max archive response duration %s + %s",
			gcDelay, maxViewAge, maxResponse, margin)
	}
	return nil
}

// Server builds the XRPC handler tree for the jetstream lexicons.
type Server struct {
	src    SegmentSource
	logger *slog.Logger
	xrpc   *xrpcserver.Server
}

// Config holds the dependencies for the XRPC server. Zero values are valid:
// a nil Logger defaults to slog.Default(); a nil Opener serves segment files
// from the paths Src reports; a nil Ready disables the readiness gate; an
// unknown or disabled CompactionDeadline disables caching; nil Metrics/Tracer
// make getBlock observability no-ops. Plan must be populated for planSnapshot
// to accept non-empty filters.
//
// Ready runs at the start of every archive request and turns an error into a
// 503, for example during bootstrap or manifest startup.
//
// MaxResponseDuration, when positive, cuts off a getSegment or getBlock
// response that runs longer; zero leaves responses unbounded. Sync, when set,
// runs before planSnapshot plans against a seq the request names.
type Config struct {
	Src                  SegmentSource
	Opener               SegmentOpener
	Logger               *slog.Logger
	Ready                lifecycle.Readiness
	CompactionCacheGrace time.Duration
	CompactionDeadline   CompactionDeadline
	Plan                 PlanConfig
	Metrics              *Metrics
	Tracer               trace.Tracer
	MaxResponseDuration  time.Duration
	Sync                 SeqSyncer

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
	opener := cfg.Opener
	if opener == nil {
		opener = FileOpener{Src: cfg.Src}
	}
	s := &Server{src: cfg.Src, logger: logger, xrpc: &xrpcserver.Server{}}
	s.xrpc.HandleQuery(getSegmentNSID, withReady(cfg.Ready, &getSegmentHandler{
		opener: opener, logger: logger, maxDuration: cfg.MaxResponseDuration,
		compactionCacheGrace: cfg.CompactionCacheGrace, compactionDeadline: cfg.CompactionDeadline,
	}))
	s.xrpc.HandleQuery(getBlockNSID, withReady(cfg.Ready, &getBlockHandler{
		opener: opener, logger: logger, maxDuration: cfg.MaxResponseDuration,
		compactionCacheGrace: cfg.CompactionCacheGrace, compactionDeadline: cfg.CompactionDeadline,
		metrics: cfg.Metrics, tracer: cfg.Tracer,
	}))
	s.xrpc.HandleQuery("network.bsky.jetstream.listSegments", withReady(cfg.Ready, newListSegmentsHandler(cfg.Src)))
	s.xrpc.HandleProcedure("network.bsky.jetstream.planSnapshot", withReady(cfg.Ready, newPlanSnapshotHandler(cfg.Src, cfg.Plan, cfg.Sync)))

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

// responseDeadline bounds a response to d when d is positive: ctx ends, so
// object reads stop, and the connection's write deadline is set, so a
// stalled client cannot hold the response open either. Pinning a generation
// is only safe for GC_DELAY, so no response may outlive it (design §11.5).
// The write deadline is best-effort: a wrapping ResponseWriter that cannot
// reach the connection leaves only the ctx cutoff.
func responseDeadline(ctx context.Context, w http.ResponseWriter, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return ctx, func() {}
	}
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(d))
	return context.WithTimeout(ctx, d)
}

func withReady(ready lifecycle.Readiness, h xrpcserver.Handler) xrpcserver.Handler {
	if ready == nil {
		return h
	}
	return xrpcserver.HandlerFunc(func(ctx context.Context, w http.ResponseWriter, r *xrpcserver.Request) error {
		if err := ready.Ready(ctx); err != nil {
			return &xrpc.Error{
				StatusCode: http.StatusServiceUnavailable,
				Name:       "ServiceUnavailable",
				Message:    fmt.Sprintf("service not ready: %s", err.Error()),
			}
		}
		return h.ServeXRPC(ctx, w, r)
	})
}

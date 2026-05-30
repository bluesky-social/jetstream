package subscribe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
	"github.com/bluesky-social/jetstream-v2/internal/manifest"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/coder/websocket"
)

const (
	// pingInterval keeps idle connections alive through proxy / load
	// balancer idle timeouts.
	pingInterval = 30 * time.Second

	// frameWriteTimeout bounds how long a single websocket frame can
	// take to flush. A wedged client triggers handler exit and unsubscribe.
	frameWriteTimeout = 5 * time.Second
)

// HandlerDeps bundles the dependencies of the /subscribe handler.
// Required fields are validated in NewHandler with a panic — this is
// wired exactly once at process startup, so a panic at construction
// time is the right granularity.
type HandlerDeps struct {
	Broadcaster *Broadcaster
	Store       *store.Store
	Manifest    *manifest.Manifest // optional; required for cursor replay
	Writer      *ingest.Writer     // optional; required for cursor replay
	// WriterRef, when non-nil, supersedes Writer. Resolved at request
	// time; supports cmd/jetstream's deferred-writer-publication
	// pattern where the orchestrator publishes the writer pointer
	// after steady-state begins.
	WriterRef *atomic.Pointer[ingest.Writer]
	Logger    *slog.Logger
	Metrics   *Metrics

	// Lookback is the cursor-replay clamp duration. Zero disables
	// cursor replay entirely (cursors are silently dropped to live).
	Lookback time.Duration

	// LookbackRingSz is the per-subscriber ring size for the lookback
	// handoff. Zero defaults to DefaultLookbackRingSize.
	LookbackRingSz int

	// MaxIters bounds the ring-overflow restart loop. Zero defaults
	// to DefaultMaxLookbackIterations.
	MaxIters int
}

func (d HandlerDeps) writer() *ingest.Writer {
	if d.WriterRef != nil {
		return d.WriterRef.Load()
	}
	return d.Writer
}

func NewHandler(deps HandlerDeps) http.Handler {
	if deps.Logger == nil {
		panic("subscribe: HandlerDeps.Logger is required")
	}
	if deps.Broadcaster == nil {
		panic("subscribe: HandlerDeps.Broadcaster is required")
	}
	if deps.Store == nil {
		panic("subscribe: HandlerDeps.Store is required")
	}
	logger := deps.Logger.With(slog.String("component", "subscribe/handler"))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serve(w, r, deps, logger)
	})
}

func serve(w http.ResponseWriter, r *http.Request, deps HandlerDeps, logger *slog.Logger) {
	if !lifecycle.IsSteadyState(deps.Store) {
		http.Error(w, "service not ready: bootstrap in progress", http.StatusServiceUnavailable)
		return
	}

	values, qerr := url.ParseQuery(r.URL.RawQuery)
	if qerr != nil {
		http.Error(w, fmt.Sprintf("%s: %s", ErrInvalidOptions.Error(), qerr.Error()), http.StatusBadRequest)
		return
	}

	if values.Get("compress") == "true" || strings.Contains(r.Header.Get("Socket-Encoding"), "zstd") {
		http.Error(w, "compression not supported: jetstream v2 does not implement the v1 zstd-with-custom-dictionary scheme; remove ?compress=true and the Socket-Encoding header", http.StatusBadRequest)
		return
	}

	initialFilter, perr := ParseQuery(values)
	if perr != nil {
		http.Error(w, perr.Error(), http.StatusBadRequest)
		return
	}

	requireHello := parseRequireHello(values)

	// Resolve cursor BEFORE upgrade so a bad cursor returns HTTP 400.
	rawCursor := values.Get("cursor")
	var cursorPlan CursorPlan
	switch {
	case deps.Lookback <= 0:
		// Cursor lookback is disabled by configuration. Ignore the
		// parameter and serve live tip; this is a documented operator
		// choice, not a silent gap.
		cursorPlan = CursorPlan{Mode: ModeLive}
	case deps.Manifest == nil || deps.writer() == nil:
		// Cursor lookback is enabled, but the replay dependencies aren't
		// available yet. The only window this happens in is steady-state
		// warmup: the phase marker is durable (we passed IsSteadyState
		// above) but the live consumer hasn't published its writer
		// pointer yet. Silently serving live tip here would hand a
		// resuming client the live tip while it believes it resumed at
		// its cursor — a silent gap of every event between the cursor
		// and the tip. Refuse with a retryable 503 instead so the
		// client reconnects once warmup completes. A request with no
		// cursor has nothing to resume, so it's safe to serve live.
		if rawCursor != "" {
			deps.Metrics.incCursorRequests("unavailable")
			http.Error(w, "service not ready: cursor replay warming up", http.StatusServiceUnavailable)
			return
		}
		cursorPlan = CursorPlan{Mode: ModeLive}
	default:
		resolveStart := time.Now()
		plan, err := ResolveCursor(rawCursor, CursorEnv{
			Manifest: deps.Manifest,
			NextSeq:  deps.writer().NextSeq(),
			Lookback: deps.Lookback,
		})
		deps.Metrics.observeCursorResolveSeconds(time.Since(resolveStart).Seconds())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		cursorPlan = plan
	}

	// Classify the cursor mode for metrics.
	mode := "live"
	switch cursorPlan.Mode {
	case ModeReplaySeq:
		mode = "seq"
	case ModeReplayTimeUS:
		mode = "time_us"
	}
	if cursorPlan.Clamped {
		mode = "clamped"
	}
	if deps.Lookback == 0 && rawCursor != "" {
		mode = "disabled"
	}
	deps.Metrics.incCursorRequests(mode)

	// Negotiate RFC 7692 permessage-deflate when the client offers it.
	// coder/websocket reads the client's Sec-WebSocket-Extensions during
	// Accept and only enables compression if the peer advertises support,
	// falling back to uncompressed otherwise — so non-offering clients
	// (Safari, bare consumers) are unaffected. ContextTakeover reuses a
	// 32 KB sliding window across messages for the best ratio on this
	// repetitive JSON firehose; its ~1.2 MB flate.Writer per connection is
	// affordable at our connection scale. This is orthogonal to the v1
	// zstd-with-custom-dictionary scheme rejected above.
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns:  []string{"*"},
		CompressionMode: websocket.CompressionContextTakeover,
	})
	if err != nil {
		return
	}
	defer func() { _ = conn.CloseNow() }()
	conn.SetReadLimit(int64(MaxSubscriberMessageBytes))

	var filterPtr atomic.Pointer[Filter]
	filterPtr.Store(initialFilter)

	helloCh := make(chan struct{})
	var helloOnce sync.Once
	signalHello := func() { helloOnce.Do(func() { close(helloCh) }) }

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// Enroll this connection in the broadcaster's graceful-shutdown
	// registry. On shutdown the broadcaster invokes closeConn (below),
	// which sends a clean StatusGoingAway close frame and unwinds the
	// serve loops. If RegisterConn reports the broadcaster is already
	// draining, we missed the closer snapshot — send our own goodbye and
	// bail rather than serve a connection nobody will ever close.
	closeConn := func() {
		// Send the close frame FIRST, before cancelling. Cancelling the
		// read context trips coder/websocket's AfterFunc, which force-
		// closes the socket (rwc.Close) with no close frame — exactly the
		// abrupt teardown we're trying to avoid. conn.Close writes the
		// goodbye over the independent write path; the blocked reader
		// unwinds on the peer's echo (or on Close's own 5s+5s internal
		// timeout for a silent peer). Only then do we cancel, to
		// guarantee the serve loops exit even if they were mid-select.
		// The whole call is bounded by the caller's Shutdown deadline.
		_ = conn.Close(websocket.StatusGoingAway, "server shutting down")
		cancel()
	}
	connID, ok := deps.Broadcaster.RegisterConn(closeConn)
	if !ok {
		_ = conn.Close(websocket.StatusGoingAway, "server shutting down")
		return
	}
	defer deps.Broadcaster.DeregisterConn(connID)

	go runReader(ctx, cancel, conn, deps, &filterPtr, signalHello, logger)

	if requireHello {
		select {
		case <-helloCh:
		case <-ctx.Done():
			return
		}
	}

	if cursorPlan.Mode == ModeLive {
		runLiveLoop(ctx, conn, deps, &filterPtr, logger)
		return
	}

	runReplayLoop(ctx, conn, deps, &filterPtr, cursorPlan, logger)
}

func runReader(
	ctx context.Context, cancel context.CancelFunc,
	conn *websocket.Conn,
	deps HandlerDeps,
	filterPtr *atomic.Pointer[Filter],
	signalHello func(),
	logger *slog.Logger,
) {
	defer cancel()
	for {
		msgType, payload, rerr := conn.Read(ctx)
		if rerr != nil {
			if websocket.CloseStatus(rerr) == websocket.StatusMessageTooBig {
				deps.Metrics.incOptionsUpdateError(optionsUpdateErrorReasonOversize)
			}
			return
		}
		if msgType != websocket.MessageText {
			continue
		}
		var msg SubscriberSourcedMessage
		if err := json.Unmarshal(payload, &msg); err != nil {
			deps.Metrics.incOptionsUpdateError(optionsUpdateErrorReasonBadEnvelopeJSON)
			_ = conn.Close(websocket.StatusInvalidFramePayloadData, "bad SubscriberSourcedMessage envelope")
			return
		}
		switch msg.Type {
		case SubMessageTypeOptionsUpdate:
			var update UpdatePayload
			if err := json.Unmarshal(msg.Payload, &update); err != nil {
				deps.Metrics.incOptionsUpdateError(optionsUpdateErrorReasonBadPayloadJSON)
				_ = conn.Close(websocket.StatusInvalidFramePayloadData, "bad options_update payload")
				return
			}
			newFilter, err := ParseUpdatePayload(update)
			if err != nil {
				deps.Metrics.incOptionsUpdateError(optionsUpdateErrorReasonInvalidOptions)
				_ = conn.Close(websocket.StatusPolicyViolation, truncateCloseReason(err.Error()))
				return
			}
			filterPtr.Store(newFilter)
			deps.Metrics.incOptionsUpdates()
			signalHello()
		default:
			logger.Warn("unknown subscriber message type", "type", msg.Type)
		}
	}
}

func runLiveLoop(
	ctx context.Context, conn *websocket.Conn,
	deps HandlerDeps, filterPtr *atomic.Pointer[Filter],
	logger *slog.Logger,
) {
	subCh, doneCh, unsubscribe := deps.Broadcaster.Subscribe()
	defer unsubscribe()

	pingTicker := time.NewTicker(pingInterval)
	defer pingTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-doneCh:
			return
		case <-pingTicker.C:
			pingCtx, pcancel := context.WithTimeout(ctx, frameWriteTimeout)
			perr := conn.Ping(pingCtx)
			pcancel()
			if perr != nil {
				return
			}
		case evt := <-subCh:
			if !deliverEvent(ctx, conn, deps, filterPtr.Load(), evt, logger) {
				return
			}
		}
	}
}

func runReplayLoop(
	ctx context.Context, conn *websocket.Conn,
	deps HandlerDeps, filterPtr *atomic.Pointer[Filter],
	plan CursorPlan, logger *slog.Logger,
) {
	deps.Metrics.incLookbackSubscribers()
	defer deps.Metrics.decLookbackSubscribers()

	r := NewReplayer(ReplayerInput{
		Broadcaster: deps.Broadcaster,
		Manifest:    deps.Manifest,
		Writer:      deps.writer(),
		StartSeq:    plan.StartSeq,
		RingSize:    deps.LookbackRingSz,
		MaxIters:    deps.MaxIters,
		Metrics:     deps.Metrics,
	})
	err := r.Run(ctx, func(ev *segment.Event) error {
		if !deliverEvent(ctx, conn, deps, filterPtr.Load(), ev, logger) {
			return context.Canceled
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		// ErrLookbackTooSlow is the iteration-cap exhaustion case;
		// any other non-context error is an unexpected replay failure
		// (disk I/O, segment decode, etc.). Distinguish them on the
		// wire so clients can react appropriately.
		errCode := "replay_error"
		if errors.Is(err, ErrLookbackTooSlow) {
			errCode = "lookback_too_slow"
		}
		body, _ := json.Marshal(struct {
			Error   string `json:"error"`
			Message string `json:"message"`
		}{Error: errCode, Message: err.Error()})
		writeCtx, wcancel := context.WithTimeout(ctx, frameWriteTimeout)
		_ = conn.Write(writeCtx, websocket.MessageText, body)
		wcancel()
	}
}

// deliverEvent is the per-event filter+encode+write step shared by
// the live and replay paths. Returns false if the connection should
// terminate (write error). Skipped or oversize events are reported
// via metrics; the connection stays alive.
func deliverEvent(
	ctx context.Context, conn *websocket.Conn,
	deps HandlerDeps, f *Filter, evt *segment.Event, logger *slog.Logger,
) bool {
	if !f.Wants(evt) {
		deps.Metrics.incEventsFiltered()
		return true
	}
	body, eerr := Encode(evt)
	if errors.Is(eerr, errSkipEvent) {
		deps.Metrics.incEventsSkippedSync()
		return true
	}
	if eerr != nil {
		deps.Metrics.incEncodeErrors()
		logger.Warn("encode error", "err", eerr, "kind", int(evt.Kind), "did", evt.DID)
		return true
	}
	if max := f.MaxMessageSizeBytes(); max > 0 && uint32(len(body)) > max {
		deps.Metrics.incEventsOversize()
		return true
	}
	writeCtx, wcancel := context.WithTimeout(ctx, frameWriteTimeout)
	werr := conn.Write(writeCtx, websocket.MessageText, body)
	wcancel()
	if werr != nil {
		return false
	}
	deps.Metrics.incEventsSent()
	return true
}

// truncateCloseReason fits a reason string into the 123-byte limit
// imposed on websocket close-frame reason text (RFC 6455 §5.5.1). The
// cut is rune-aligned: callers (e.g. ParseQuery error messages) echo
// user-supplied input that may contain multi-byte UTF-8 sequences, and
// coder/websocket validates close-frame reasons as valid UTF-8. Any
// truncation appends "..." to make the cut visible to clients.
func truncateCloseReason(s string) string {
	const max = 123
	if len(s) <= max {
		return s
	}
	const suffix = "..."
	budget := max - len(suffix)
	// Walk back from `budget` to a rune boundary. utf8.RuneStart is
	// true for the first byte of any (single- or multi-byte) sequence,
	// so the first index i ≤ budget where RuneStart(s[i]) holds is
	// the largest valid cut.
	cut := budget
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + suffix
}

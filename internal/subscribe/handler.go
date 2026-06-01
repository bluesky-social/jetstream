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

// Subscription bundles the dependencies of the /subscribe handler.
// Required fields are validated in NewHandler with a panic — this is
// wired exactly once at process startup, so a panic at construction
// time is the right granularity.
type Subscription struct {
	Tail     *Tail
	Store    *store.Store
	Manifest *manifest.Manifest // optional; required for cursor replay
	Writer   *ingest.Writer     // optional; required for cursor replay
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
}

func (d Subscription) writer() *ingest.Writer {
	if d.WriterRef != nil {
		return d.WriterRef.Load()
	}
	return d.Writer
}

func NewHandler(deps Subscription) http.Handler {
	if deps.Logger == nil {
		panic("subscribe: HandlerDeps.Logger is required")
	}
	if deps.Tail == nil {
		panic("subscribe: HandlerDeps.Tail is required")
	}
	if deps.Store == nil {
		panic("subscribe: HandlerDeps.Store is required")
	}
	logger := deps.Logger.With(slog.String("component", "subscribe/handler"))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serve(w, r, deps, logger)
	})
}

func serve(w http.ResponseWriter, r *http.Request, deps Subscription, logger *slog.Logger) {
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
	extended := values.Get("extended") == "true"

	// Resolve cursor BEFORE upgrade so a bad cursor returns HTTP 400.
	rawCursor := values.Get("cursor")

	var cursorPlan CursorPlan
	switch {
	case deps.Lookback <= 0:
		// Cursor lookback is disabled by configuration. The service runs
		// pure-live: seqs start at 0 and the live tip comes from the ring,
		// so there's no warmup window and no writer to wait on. Ignore the
		// cursor parameter and serve live tip; a documented operator
		// choice, not a silent gap.
		cursorPlan = CursorPlan{Mode: ModeLive}
	case deps.Manifest == nil || deps.writer() == nil:
		// Cursor lookback is enabled but the replay dependencies aren't
		// available. The dominant case is the steady-state warmup window:
		// the phase marker is durable (we passed IsSteadyState above) but
		// the live consumer hasn't published its writer pointer yet, so
		// the Tail's live tip is not yet meaningful — Tip() reports 0.
		// Serving ANY subscriber now is wrong:
		//
		//   - A cursor client would be handed the live tip while believing
		//     it resumed at its cursor — a silent gap of every event
		//     between the cursor and the tip.
		//   - A live (no-cursor) client would anchor at the bogus tip 0;
		//     once real events arrive at a high seq, that client sits below
		//     the hot ring's base and dives the ENTIRE archive cold — the
		//     replay storm this fan-out path exists to avoid.
		//
		// Refuse both with a retryable 503 until the writer is published;
		// the client reconnects in seconds. Earlier this exempted no-cursor
		// requests as "safe to serve live" — that was the source of the
		// full-archive replay, since the live tip is unknowable here.
		if rawCursor != "" {
			deps.Metrics.incCursorRequests("unavailable")
		}
		http.Error(w, "service not ready: cursor replay warming up", http.StatusServiceUnavailable)
		return
	default:
		if err := deps.Manifest.Wait(r.Context()); err != nil {
			if rawCursor != "" {
				deps.Metrics.incCursorRequests("unavailable")
			}
			http.Error(w, fmt.Sprintf("service not ready: manifest warming up: %s", err.Error()), http.StatusServiceUnavailable)
			return
		}
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

	// Enroll this connection in the Tail's graceful-shutdown
	// registry. On shutdown the Tail invokes closeConn (below),
	// which sends a clean StatusGoingAway close frame and unwinds the
	// serve loops. If RegisterConn reports the Tail is already
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

	connID, ok := deps.Tail.RegisterConn(closeConn)
	if !ok {
		_ = conn.Close(websocket.StatusGoingAway, "server shutting down")
		return
	}
	defer deps.Tail.DeregisterConn(connID)

	go runReader(ctx, cancel, conn, deps, &filterPtr, signalHello, logger)

	if requireHello {
		select {
		case <-helloCh:
		case <-ctx.Done():
			return
		}
	}

	startSeq := cursorPlan.StartSeq
	if cursorPlan.Mode == ModeLive {
		startSeq = deps.Tail.Tip()
	}

	runSubscriberLoop(ctx, conn, deps, &filterPtr, startSeq, extended, logger)
}

func runReader(
	ctx context.Context, cancel context.CancelFunc,
	conn *websocket.Conn,
	deps Subscription,
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

// runSubscriberLoop is the single pull loop for every subscriber, live or
// cursor. It reads batches from the Tail starting at startSeq, delivers each
// event (filter -> memoized encode -> write), and drops the client only when
// the adversarial-rate detector fires. ReadFrom blocks at the tip, so an idle
// stream costs nothing; a ping ticker keeps idle connections alive.
func runSubscriberLoop(
	ctx context.Context,
	conn *websocket.Conn,
	deps Subscription,
	filterPtr *atomic.Pointer[Filter],
	startSeq uint64,
	extended bool,
	logger *slog.Logger,
) {
	deps.Metrics.incSubscribers()
	defer deps.Metrics.decSubscribers()

	slowDetector := newSlowDetector(deps.Tail.SlowConfig())
	batchMax := deps.Tail.ReadBatch()
	cursor := startSeq

	pingTicker := time.NewTicker(pingInterval)
	defer pingTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			deps.Metrics.incCleanDisconnects()
			return
		case <-pingTicker.C:
			pingCtx, pcancel := context.WithTimeout(ctx, frameWriteTimeout)
			perr := conn.Ping(pingCtx)
			pcancel()
			if perr != nil {
				return
			}
		default:
		}

		// Bound the read so the loop wakes periodically to send keepalive
		// pings even when the stream is idle (ReadFrom blocks at the tip).
		readCtx, rcancel := context.WithTimeout(ctx, pingInterval)
		batch, next, err := deps.Tail.ReadFrom(readCtx, cursor, batchMax)
		rcancel()
		if err != nil {
			if ctx.Err() != nil {
				deps.Metrics.incCleanDisconnects()
				return // connection closing
			}
			if errors.Is(err, context.DeadlineExceeded) {
				continue // idle at tip: loop to send a keepalive ping
			}
			if errors.Is(err, errColdUnavailable) {
				return
			}
			logger.Warn("read error", "err", err)
			return
		}

		// Forward-progress invariant: a non-error ReadFrom must advance the
		// cursor. Blocking at the live tip surfaces as DeadlineExceeded
		// (handled above); a hot hit or cold batch always advances past what
		// it returned. A non-advancing non-error return (e.g. an empty cold
		// batch with next == cursor) would spin this loop hot, so treat it as
		// a contract violation and disconnect.
		if next <= cursor {
			logger.Error("tail ReadFrom returned non-advancing cursor",
				"cursor", cursor, "next", next, "batch", len(batch))
			return
		}

		for _, e := range batch {
			f := filterPtr.Load()
			if !f.Wants(e.Event) {
				deps.Metrics.incEventsFiltered()
				continue
			}

			var body []byte
			var eerr error
			if extended {
				body, eerr = e.EncodedExtended()
			} else {
				body, eerr = e.Encoded()
			}
			if errors.Is(eerr, errSkipEvent) {
				deps.Metrics.incEventsSkippedSync()
				continue
			}
			if eerr != nil {
				deps.Metrics.incEncodeErrors()
				logger.Warn("encode error", "err", eerr, "kind", int(e.Event.Kind), "did", e.Event.DID)
				continue
			}

			if max := f.MaxMessageSizeBytes(); max > 0 && uint32(len(body)) > max {
				deps.Metrics.incEventsOversize()
				continue
			}

			writeCtx, wcancel := context.WithTimeout(ctx, frameWriteTimeout)
			werr := conn.Write(writeCtx, websocket.MessageText, body)
			wcancel()
			if werr != nil {
				return
			}

			deps.Metrics.incEventsSent()
		}

		cursor = next

		// The detector keys on cursor (log-scan progress), not frames
		// delivered: a selective-filter client that scans fast but emits
		// little is keeping up and must not be dropped.
		lag := uint64(0)
		if tip := deps.Tail.Tip(); tip > cursor {
			lag = tip - cursor
		}
		if slowDetector.observe(cursor, lag) {
			deps.Metrics.incAdversarialDrops()
			logger.Warn("dropped adversarially slow subscriber", "cursor", cursor, "lag", lag)
			return
		}
	}
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

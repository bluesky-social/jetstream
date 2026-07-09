package subscribe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/cockroachdb/pebble/vfs"
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
	FS       vfs.FS
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

	// V2 selects the /subscribe-v2 presentation policy as a bundle:
	//
	//   - the v2 wire shape (seq + commit.record_cbor on every event, and
	//     archived #sync events emitted rather than skipped),
	//   - Sync 1.1 resync replacement rows are emitted (v1 advances over
	//     them silently for wire parity),
	//   - a seq cursor below the lookback floor is REJECTED with a
	//     pre-upgrade HTTP 400 carrying the floor seq (v1 silently clamps).
	//
	// The default false preserves Jetstream v1 behavior on /subscribe.
	// These are deliberately one flag: the policies describe one endpoint
	// contract and must not be mixed and matched.
	V2 bool
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

	// Compression negotiation. The two endpoints have deliberately
	// different contracts (#294):
	//
	//   - /subscribe (v1, wire-frozen): the legacy custom-zstd-dictionary
	//     opt-in (compress=true / Socket-Encoding: zstd) and auto-negotiated
	//     RFC 7692 permessage-deflate, exactly as v1 shipped them. A client
	//     must pick ONE: zstd output is already entropy-coded, so double-
	//     compressing under deflate is rejected loudly rather than silently
	//     disabling one.
	//
	//   - /subscribe-v2: dict-zstd is the ONLY compression scheme, opted
	//     into with zstdDictionary=<id> where <id> names the dictionary the
	//     client fetched via getZstdDictionary. permessage-deflate is never
	//     negotiated (per-connection deflate is the dominant server cost at
	//     fanout scale — measured 2.3x the CPU of shared dict-zstd at 200
	//     subscribers) and the v1 opt-ins are rejected: we never serve
	//     frames a client can't decode, and there are no legacy v2 clients
	//     to stay compatible with.
	var wantZstd bool
	if deps.V2 {
		rawDict := values.Get("zstdDictionary")
		if values.Get("compress") == "true" || strings.Contains(r.Header.Get("Socket-Encoding"), "zstd") {
			http.Error(w, "compress=true / Socket-Encoding: zstd is the /subscribe (v1) opt-in; /subscribe-v2 uses zstdDictionary=<id> with the dictionary from getZstdDictionary", http.StatusBadRequest)
			return
		}
		if rawDict != "" {
			id, perr := strconv.ParseUint(rawDict, 10, 32)
			if perr != nil || id == 0 {
				http.Error(w, "zstdDictionary must be a positive integer zstd dictionary ID", http.StatusBadRequest)
				return
			}
			if uint32(id) != DictionaryV2ID {
				// Never serve frames the client can't decode: an unknown or
				// retired dictionary ID is a hard 400 carrying the current
				// ID, so the client re-fetches and reconnects.
				http.Error(w, fmt.Sprintf("unknown zstd dictionary id %d; current dictionary id is %d (fetch it via getZstdDictionary and reconnect)", id, DictionaryV2ID), http.StatusBadRequest)
				return
			}
			wantZstd = true
		}
	} else {
		wantZstd = values.Get("compress") == "true" ||
			strings.Contains(r.Header.Get("Socket-Encoding"), "zstd")
		if wantZstd && strings.Contains(r.Header.Get("Sec-WebSocket-Extensions"), "permessage-deflate") {
			http.Error(w, "choose one compression scheme: custom zstd (compress=true / Socket-Encoding: zstd) or RFC 7692 permessage-deflate, not both", http.StatusBadRequest)
			return
		}
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
		//     the readable-log floor and dives the ENTIRE archive cold — the
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
			Manifest:         deps.Manifest,
			FS:               deps.FS,
			NextSeq:          deps.writer().NextSeq(),
			Lookback:         deps.Lookback,
			RejectBelowFloor: deps.V2,
		})
		deps.Metrics.observeCursorResolveSeconds(time.Since(resolveStart).Seconds())
		if err != nil {
			switch {
			case errors.Is(err, ErrCursorResolveFailed):
				// A SERVER-side fault while resolving a well-formed cursor (a
				// segment read/decode/index-load failure during timestamp
				// translation). This is 5xx-class, not a client bad-request: the
				// client should retry, operators must see it on the 5xx signal,
				// and the wrapped internal segment path must NOT leak to the
				// client. Log the detail server-side; return a generic 503.
				logger.Error("cursor resolution failed", "err", err, "raw_cursor", rawCursor)
				deps.Metrics.incCursorRequests("resolve_failed")
				http.Error(w, "service not ready: cursor resolution failed", http.StatusServiceUnavailable)
				return
			case errors.Is(err, ErrCursorTooOld):
				// A too-old v2 seq cursor is a distinct, expected signal (the
				// client re-backfills), not a malformed request; label it
				// separately so it stays visible apart from parse-error 400s.
				deps.Metrics.incCursorRequests("too_old")
			}
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

	// Compression at the websocket layer:
	//
	//   - /subscribe (v1): negotiate RFC 7692 permessage-deflate when the
	//     client offers it, exactly as v1 shipped. coder/websocket only
	//     enables it if the peer advertises support, so non-offering
	//     clients are unaffected. ContextTakeover reuses a 32 KB sliding
	//     window across messages; its ~1.2 MB flate.Writer per connection
	//     is tolerated on the legacy endpoint for wire parity.
	//   - /subscribe-v2: NEVER negotiated. Per-connection deflate is the
	//     dominant server cost at fanout scale and is client-triggerable;
	//     v2's only compression is the shared dict-zstd scheme (#294). A
	//     deflate offer from a v2 client is silently not accepted (that is
	//     the RFC 7692 fallback: the extension is simply absent from the
	//     handshake response, and the client proceeds uncompressed).
	//   - zstd clients (either endpoint) do their own framing, so deflate
	//     must not also run; disable explicitly so an Accept default can't
	//     re-enable it.
	compressionMode := websocket.CompressionContextTakeover
	if wantZstd || deps.V2 {
		compressionMode = websocket.CompressionDisabled
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns:  []string{"*"},
		CompressionMode: compressionMode,
	})
	if err != nil {
		return
	}

	// Classify the connection's negotiated compression scheme for metrics.
	// zstd is an explicit opt-in; deflate is negotiated iff we allowed it
	// AND the client offered the extension (mirrors coder/websocket's
	// selectDeflate accept condition). The library does not export the
	// negotiated state, so this echoes its decision rather than observing
	// it; exotic extension params a client could send that make the
	// library refuse deflate would be mislabeled, which is acceptable for
	// a metrics label.
	scheme := compressionSchemeNone
	switch {
	case wantZstd:
		scheme = compressionSchemeZstd
	case compressionMode != websocket.CompressionDisabled &&
		strings.Contains(r.Header.Get("Sec-WebSocket-Extensions"), "permessage-deflate"):
		scheme = compressionSchemeDeflate
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

	runSubscriberLoop(ctx, conn, deps, &filterPtr, startSeq, scheme, logger)
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
//
// Each subscriber drives its own pull loop and advances its cursor at its own
// pace, so backpressure is implicit: a slow client simply pulls slower. There
// is no central broadcaster fanning out writes and no per-subscriber buffer to
// bound or overflow.
func runSubscriberLoop(
	ctx context.Context,
	conn *websocket.Conn,
	deps Subscription,
	filterPtr *atomic.Pointer[Filter],
	startSeq uint64,
	scheme string,
	logger *slog.Logger,
) {
	compress := scheme == compressionSchemeZstd
	deps.Metrics.incSubscribers(scheme)
	defer deps.Metrics.decSubscribers(scheme)

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
			if e.Event.Kind.IsResyncReplacement() && !deps.V2 {
				deps.Metrics.incEventsSkippedResync()
				continue
			}
			if !f.Wants(e.Event) {
				deps.Metrics.incEventsFiltered()
				continue
			}

			// Size cap is enforced on the UNCOMPRESSED JSON length even for
			// zstd clients: the cap bounds the logical record size a client
			// will accept, and comparing against unpredictable compressed
			// size (v1's behavior) would let a large record slip a small cap.
			// A deliberate, documented divergence from v1.
			var body []byte
			var eerr error
			if deps.V2 {
				body, eerr = e.EncodedV2()
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

			// Pick the wire payload + frame type by the connection's fixed
			// compression preference. The compressed accessors derive from
			// the same memoized JSON above, so the size cap (checked on the
			// uncompressed body) and the skip/encode-error branches already
			// hold; the only remaining failure is the compress step itself.
			msgType := websocket.MessageText
			payload := body
			if compress {
				var cerr error
				if deps.V2 {
					payload, cerr = e.CompressedV2()
				} else {
					payload, cerr = e.Compressed()
				}
				if cerr != nil {
					deps.Metrics.incEncodeErrors()
					logger.Warn("compress error", "err", cerr, "kind", int(e.Event.Kind), "did", e.Event.DID)
					continue
				}
				msgType = websocket.MessageBinary
			}

			writeCtx, wcancel := context.WithTimeout(ctx, frameWriteTimeout)
			werr := conn.Write(writeCtx, msgType, payload)
			wcancel()
			if werr != nil {
				return
			}

			deps.Metrics.incEventsSent(scheme, len(payload), len(body))
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

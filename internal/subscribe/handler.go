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

	"github.com/bluesky-social/jetstream/api/jetstream"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
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

	// V2 selects the network.bsky.jetstream.subscribe endpoint contract
	// (atproto proposal 0015) as a bundle:
	//
	//   - the xrpc.v1.json wire framing (one self-describing message or
	//     error frame per event, built from the published lexicon), with
	//     archived #sync events emitted rather than skipped,
	//   - Sec-WebSocket-Protocol negotiation (xrpc.v1.json is both the
	//     only supported token and the lexicon default),
	//   - server-push only: any client data frame closes the connection
	//     (no options_update / requireHello — those are v1-only),
	//   - the three-axis kinds/dids/collections filter (ParseQueryV2),
	//   - pre-upgrade errors as XRPC JSON envelopes ({"error","message"}),
	//   - a seq cursor below the lookback floor is REJECTED with a
	//     pre-upgrade 400 CursorTooOld carrying the floor seq (v1
	//     silently clamps); a clamped timestamp cursor emits an #info
	//     OutdatedCursor frame before the first event,
	//   - Sync 1.1 resync replacement rows are emitted (v1 advances over
	//     them silently for wire parity).
	//
	// The default false preserves Jetstream v1 behavior on /subscribe.
	// These are deliberately one flag: the policies describe one endpoint
	// contract and must not be mixed and matched.
	V2 bool
}

// eventFilter is the per-delivery predicate surface shared by the v1
// Filter (mutable via options_update) and the v2 FilterV2 (immutable).
type eventFilter interface {
	Wants(*segment.Event) bool
	MaxMessageSizeBytes() uint32
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

// httpError writes a pre-upgrade error in the endpoint's contract shape:
// plain text on /subscribe (v1 wire parity), the standard XRPC JSON error
// envelope ({"error": name, "message": msg}) on the v2 endpoint, where
// clients match the structured error name instead of substring-scraping
// the body.
func httpError(w http.ResponseWriter, deps Subscription, status int, name, msg string) {
	if !deps.V2 {
		http.Error(w, msg, status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	body, _ := json.Marshal(struct {
		Error   string `json:"error"`
		Message string `json:"message,omitempty"`
	}{Error: name, Message: msg})
	_, _ = w.Write(body)
}

// v2Subprotocol is the one wire subprotocol the v2 endpoint speaks; it is
// also the lexicon-declared default, so negotiation degenerates to a
// header echo — every connection receives identical framing. The constant
// comes from the generated lexicon code so the wire, the schema, and the
// handler cannot drift.
const v2Subprotocol = jetstream.JetstreamSubscribe_Subprotocol

// negotiateSubprotocol intersects the client's Sec-WebSocket-Protocol
// offer with the supported set {xrpc.v1.json}, matching case-sensitively
// (RFC 7936: tokens are case-sensitive). websocket.Accept matches with
// EqualFold and echoes the CLIENT's casing, so handing it the raw
// supported set could put a non-canonical token (e.g. "XRPC.V1.JSON") on
// the wire; with the exact-match intersection Accept can only echo the
// canonical token, and a case-variant offer falls back to the lexicon
// default like any other unrecognized token (proposal 0015).
func negotiateSubprotocol(r *http.Request) []string {
	for _, header := range r.Header.Values("Sec-WebSocket-Protocol") {
		for token := range strings.SplitSeq(header, ",") {
			if strings.TrimSpace(token) == v2Subprotocol {
				return []string{v2Subprotocol}
			}
		}
	}
	return nil
}

func serve(w http.ResponseWriter, r *http.Request, deps Subscription, logger *slog.Logger) {
	if !lifecycle.IsSteadyState(deps.Store) {
		httpError(w, deps, http.StatusServiceUnavailable, "ServiceUnavailable", "service not ready: bootstrap in progress")
		return
	}

	values, qerr := url.ParseQuery(r.URL.RawQuery)
	if qerr != nil {
		httpError(w, deps, http.StatusBadRequest, "InvalidRequest", fmt.Sprintf("%s: %s", ErrInvalidOptions.Error(), qerr.Error()))
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
			httpError(w, deps, http.StatusBadRequest, "InvalidRequest",
				"compress=true / Socket-Encoding: zstd is the /subscribe (v1) opt-in; this endpoint uses zstdDictionary=<id> with the dictionary from getZstdDictionary")
			return
		}
		if rawDict != "" {
			id, perr := strconv.ParseUint(rawDict, 10, 32)
			if perr != nil || id == 0 {
				httpError(w, deps, http.StatusBadRequest, "InvalidRequest",
					"zstdDictionary must be a positive integer zstd dictionary ID")
				return
			}
			if uint32(id) != DictionaryV2ID {
				// Never serve frames the client can't decode: an unknown or
				// retired dictionary ID is a hard 400 carrying the current
				// ID, so the client re-fetches and reconnects.
				httpError(w, deps, http.StatusBadRequest, "UnknownZstdDictionary",
					fmt.Sprintf("unknown zstd dictionary id %d; current dictionary id is %d (fetch it via getZstdDictionary and reconnect)", id, DictionaryV2ID))
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

	// Parse the endpoint's filter dialect: three orthogonal axes
	// (kinds/dids/collections) on v2, the legacy wantedDids/
	// wantedCollections pair on v1.
	var initialV2Filter *FilterV2
	var filterPtr atomic.Pointer[Filter] // v1 only; swapped by options_update
	if deps.V2 {
		f, perr := ParseQueryV2(values)
		if perr != nil {
			httpError(w, deps, http.StatusBadRequest, "InvalidRequest", perr.Error())
			return
		}
		initialV2Filter = f
	} else {
		f, perr := ParseQuery(values)
		if perr != nil {
			http.Error(w, perr.Error(), http.StatusBadRequest)
			return
		}
		filterPtr.Store(f)
	}

	requireHello := !deps.V2 && parseRequireHello(values)

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
		httpError(w, deps, http.StatusServiceUnavailable, "ServiceUnavailable", "service not ready: cursor replay warming up")
		return
	default:
		if err := deps.Manifest.Wait(r.Context()); err != nil {
			if rawCursor != "" {
				deps.Metrics.incCursorRequests("unavailable")
			}
			httpError(w, deps, http.StatusServiceUnavailable, "ServiceUnavailable", fmt.Sprintf("service not ready: manifest warming up: %s", err.Error()))
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
				httpError(w, deps, http.StatusServiceUnavailable, "ServiceUnavailable", "service not ready: cursor resolution failed")
				return
			case errors.Is(err, ErrCursorTooOld):
				// A too-old v2 seq cursor is a distinct, expected signal (the
				// client re-backfills), not a malformed request; label it
				// separately so it stays visible apart from parse-error 400s.
				deps.Metrics.incCursorRequests("too_old")
				httpError(w, deps, http.StatusBadRequest, "CursorTooOld", err.Error())
				return
			}
			httpError(w, deps, http.StatusBadRequest, "InvalidRequest", err.Error())
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

	// Sec-WebSocket-Protocol negotiation (proposal 0015), v2 only. The
	// supported set is exactly {xrpc.v1.json}, which is also the lexicon
	// default, so negotiation is a header echo: an offering client gets
	// the token echoed, everyone else falls back to the default — the
	// framing is identical either way. v1 predates the proposal and never
	// negotiates.
	var acceptSubprotocols []string
	if deps.V2 {
		acceptSubprotocols = negotiateSubprotocol(r)
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns:  []string{"*"},
		CompressionMode: compressionMode,
		Subprotocols:    acceptSubprotocols,
	})
	if err != nil {
		return
	}
	if deps.V2 {
		outcome := "default"
		if conn.Subprotocol() != "" {
			outcome = "negotiated"
		}
		deps.Metrics.incSubprotocolNegotiation(outcome)
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

	// The read side splits by contract: v1 accepts subscriber-sourced
	// options_update frames; v2 is server-push only — CloseRead keeps
	// control-frame handling (ping/pong, close) alive and closes the
	// connection with StatusPolicyViolation on ANY client data frame,
	// matching the atmos xrpcserver subscription contract.
	loadFilter := func() eventFilter { return filterPtr.Load() }
	if deps.V2 {
		ctx = conn.CloseRead(ctx)
		loadFilter = func() eventFilter { return initialV2Filter }
	} else {
		go runReader(ctx, cancel, conn, deps, &filterPtr, signalHello, logger)
	}

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

	// A clamped v2 timestamp cursor starts at the retention floor, not
	// where the client asked; say so in-band before the first event
	// (subscribeRepos's OutdatedCursor precedent) instead of silently
	// clamping. Seq-mode below-floor was already rejected pre-upgrade,
	// and a future cursor clamping to the live tip is the defined
	// semantics of "start at tip", not a degradation — Mode filters both
	// out here.
	if deps.V2 && cursorPlan.Clamped && cursorPlan.Mode == ModeReplayTimeUS {
		info, ierr := EncodeV2Info("OutdatedCursor",
			fmt.Sprintf("requested timestamp cursor below retention floor; starting at seq %d", startSeq))
		if ierr != nil {
			logger.Error("encode info frame", "err", ierr)
		} else if !writeFrame(ctx, conn, wantZstd, info) {
			return
		}
	}

	runSubscriberLoop(ctx, conn, deps, loadFilter, startSeq, scheme, logger)
}

// writeFrame writes one already-encoded v2 frame, compressing it for
// zstd-negotiated connections (every frame on a zstd conn is one zstd
// frame — message, info, and error frames alike). Returns false when the
// write failed and the connection should unwind.
func writeFrame(ctx context.Context, conn *websocket.Conn, compress bool, frame []byte) bool {
	msgType := websocket.MessageText
	if compress {
		frame = compressFrameV2(frame)
		msgType = websocket.MessageBinary
	}
	writeCtx, wcancel := context.WithTimeout(ctx, frameWriteTimeout)
	defer wcancel()
	return conn.Write(writeCtx, msgType, frame) == nil
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
	loadFilter func() eventFilter,
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

	// sendError emits a terminal xrpc.v1.json error frame on v2
	// connections; the caller returns (closing the connection)
	// immediately after, per the event-stream spec's close-after-error
	// rule. v1 predates error frames — its clients see only the close
	// code — so this is a no-op there. Best-effort: a failed write means
	// the connection is already gone.
	sendError := func(code, msg string) {
		if !deps.V2 {
			return
		}
		writeFrame(ctx, conn, compress, EncodeV2Error(code, msg))
	}

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
				sendError("InternalError", "archive replay unavailable; reconnect")
				return
			}
			logger.Warn("read error", "err", err)
			sendError("InternalError", "stream read failed; reconnect")
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
			sendError("InternalError", "stream read failed; reconnect")
			return
		}

		for _, e := range batch {
			f := loadFilter()
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
			sendError("ConsumerTooSlow",
				fmt.Sprintf("reading below the floor rate %d events behind the tip; reconnect with cursor=%d", lag, cursor))
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

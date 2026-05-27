package subscribe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
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

// NewHandler returns the http.Handler for GET /subscribe. The same
// handler instance is shared across all requests; every connection
// gets its own subscription via b.Subscribe.
func NewHandler(b *Broadcaster, st *store.Store, logger *slog.Logger, m *Metrics) http.Handler {
	logger = logger.With(slog.String("component", "subscribe/handler"))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serve(w, r, b, st, logger, m)
	})
}

func serve(
	w http.ResponseWriter,
	r *http.Request,
	broadcaster *Broadcaster,
	store *store.Store,
	logger *slog.Logger,
	m *Metrics,
) {
	if !lifecycle.IsSteadyState(store) {
		http.Error(w, "service not ready: bootstrap in progress", http.StatusServiceUnavailable)
		return
	}

	// Parse subscriber filter BEFORE upgrading. v1 contract: a bad
	// query yields HTTP 400 with a useful body, not a websocket close.
	//
	// We call url.ParseQuery directly rather than r.URL.Query() because
	// r.URL.Query() silently discards percent-decode errors, returning
	// a partial values map. A client that sends a malformed query
	// (e.g. "wantedCollections=app%XX") deserves an explicit 400, not
	// a connection that silently drops every collection because the
	// filter parsed empty.
	values, qerr := url.ParseQuery(r.URL.RawQuery)
	if qerr != nil {
		http.Error(w, fmt.Sprintf("%s: %s", ErrInvalidOptions.Error(), qerr.Error()), http.StatusBadRequest)
		return
	}

	initialFilter, perr := ParseQuery(values)
	if perr != nil {
		http.Error(w, perr.Error(), http.StatusBadRequest)
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns:  []string{"*"},
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		// Accept already wrote the error response.
		return
	}
	defer func() { _ = conn.CloseNow() }()

	// Raise coder/websocket's default 32 KiB read limit to match the
	// 10MB v1 cap exactly. coder/websocket closes with StatusMessageTooBig
	// when the limit is exceeded, which is the same close code the
	// application path below would have used — so a single source of
	// truth at the websocket layer is fine. We no longer need a
	// redundant len(payload) check in the read loop.
	conn.SetReadLimit(int64(MaxSubscriberMessageBytes))

	// Per-connection filter pointer. Updates from options_update (Task 9)
	// will Store a fresh *Filter; the writer loop Loads on each event.
	// Treated as immutable once published — atomic pointer not RWMutex.
	var filterPtr atomic.Pointer[Filter]
	filterPtr.Store(initialFilter)

	subCh, doneCh, unsubscribe := broadcaster.Subscribe()
	defer unsubscribe()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// Reader goroutine: parses SubscriberSourcedMessage frames. On any
	// validation failure we send a websocket close with the reason and
	// cancel the connection context, which tears down the writer loop.
	go func() {
		defer cancel()
		for {
			msgType, payload, rerr := conn.Read(ctx)
			if rerr != nil {
				// SetReadLimit(MaxSubscriberMessageBytes) above causes
				// coder/websocket to close with StatusMessageTooBig on
				// oversize. Surface that through the existing metric
				// label so operators see the same counter regardless of
				// where the cap is enforced.
				if websocket.CloseStatus(rerr) == websocket.StatusMessageTooBig {
					m.incOptionsUpdateError(optionsUpdateErrorReasonOversize)
				}
				return
			}
			if msgType != websocket.MessageText {
				// V1 ignores binary frames silently; match that.
				continue
			}
			var msg SubscriberSourcedMessage
			if err := json.Unmarshal(payload, &msg); err != nil {
				m.incOptionsUpdateError(optionsUpdateErrorReasonBadEnvelopeJSON)
				_ = conn.Close(websocket.StatusInvalidFramePayloadData,
					"bad SubscriberSourcedMessage envelope")
				return
			}
			switch msg.Type {
			case SubMessageTypeOptionsUpdate:
				var update UpdatePayload
				if err := json.Unmarshal(msg.Payload, &update); err != nil {
					m.incOptionsUpdateError(optionsUpdateErrorReasonBadPayloadJSON)
					_ = conn.Close(websocket.StatusInvalidFramePayloadData,
						"bad options_update payload")
					return
				}
				newFilter, err := ParseUpdatePayload(update)
				if err != nil {
					m.incOptionsUpdateError(optionsUpdateErrorReasonInvalidOptions)
					// Truncate the reason to fit the websocket close-frame
					// 123-byte cap (RFC 6455 §5.5.1).
					reason := truncateCloseReason(err.Error())
					_ = conn.Close(websocket.StatusPolicyViolation, reason)
					return
				}
				filterPtr.Store(newFilter)
				m.incOptionsUpdates()
			default:
				// V1 PARITY: unknown types log a warning and are ignored.
				logger.Warn("unknown subscriber message type", "type", msg.Type)
			}
		}
	}()

	pingTicker := time.NewTicker(pingInterval)
	defer pingTicker.Stop()

	logger.Info("subscriber connected", "remote_addr", r.RemoteAddr)
	defer logger.Info("subscriber disconnected", "remote_addr", r.RemoteAddr)

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
			f := filterPtr.Load()
			if !f.Wants(evt) {
				m.incEventsFiltered()
				continue
			}
			body, eerr := Encode(evt)
			if errors.Is(eerr, errSkipEvent) {
				m.incEventsSkippedSync()
				continue
			}
			if eerr != nil {
				m.incEncodeErrors()
				logger.Warn("encode error",
					"err", eerr,
					"kind", int(evt.Kind),
					"did", evt.DID,
				)
				continue
			}
			if max := f.MaxMessageSizeBytes(); max > 0 && uint32(len(body)) > max {
				m.incEventsOversize()
				continue
			}
			writeCtx, wcancel := context.WithTimeout(ctx, frameWriteTimeout)
			werr := conn.Write(writeCtx, websocket.MessageText, body)
			wcancel()
			if werr != nil {
				return
			}
			m.incEventsSent()
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

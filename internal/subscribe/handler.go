package subscribe

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

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

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns:  []string{"*"},
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		// Accept already wrote the error response.
		return
	}
	defer func() { _ = conn.CloseNow() }()

	subCh, doneCh, unsubscribe := broadcaster.Subscribe()
	defer unsubscribe()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// Reader goroutine: drains client->server frames so the handler
	// notices when the client hangs up. We don't act on any frames
	// (no SubscriberOptionsUpdatePayload in this cut).
	go func() {
		defer cancel()
		for {
			_, _, rerr := conn.Reader(ctx)
			if rerr != nil {
				return
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
			// Broadcaster dropped us (slow consumer or shutdown).
			return
		case <-pingTicker.C:
			pingCtx, pcancel := context.WithTimeout(ctx, frameWriteTimeout)
			perr := conn.Ping(pingCtx)
			pcancel()
			if perr != nil {
				return
			}
		case evt := <-subCh:
			// subCh is never closed by the broadcaster (see spec §6.3).
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

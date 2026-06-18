package http

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/coder/websocket"
)

// subscribeReplayLimit caps how many historical events we replay on
// a fresh subscription. The world's ring buffer caps total retention;
// this is just the per-call ceiling.
const subscribeReplayLimit = 1024

// newRelaySubscribeReposHandler serves
// com.atproto.sync.subscribeRepos. The flow is:
//
//  1. Accept the websocket upgrade.
//  2. Subscribe to the live fanout BEFORE replaying history, so we
//     don't miss frames whose seq lands in the gap between the last
//     replay row and the start of live broadcast.
//  3. Read events with seq > cursor from pebble's ring buffer; if
//     cursor is older than retained history, emit a #info
//     OutdatedCursor frame first.
//  4. Pump live events until the connection closes or the request
//     ctx is cancelled.
func newRelaySubscribeReposHandler(w *world.World, faults *FaultPlan) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		conn, err := websocket.Accept(rw, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true,
		})
		if err != nil {
			return
		}
		defer func() { _ = conn.CloseNow() }()

		framesBeforeClose, faultArmed := faults.onSubscribeConnect()
		writer := subscribeFaultWriter{
			conn:              conn,
			faults:            faults,
			framesBeforeClose: framesBeforeClose,
			faultArmed:        faultArmed,
		}

		var cursor int64
		if c := r.URL.Query().Get("cursor"); c != "" {
			n, err := strconv.ParseInt(c, 10, 64)
			if err != nil {
				_ = conn.Close(websocket.StatusUnsupportedData, "bad cursor")
				return
			}
			cursor = n
		}

		// Reader goroutine: drains client->server frames so the handler
		// notices when the client hangs up (mirrors internal/subscribe).
		// Without this, a client's close frame can't be acknowledged and
		// the close handshake stalls until the library timeout (~5s),
		// keeping the subscriber attached the whole time. We don't act
		// on any client frames; subscribeRepos has no client->server
		// payload in the protocol.
		go func() {
			defer cancel()
			for {
				if _, _, rerr := conn.Reader(ctx); rerr != nil {
					return
				}
			}
		}()

		// Subscribe BEFORE replay — see step 2 in the package comment.
		sub := w.SubscribeFanout()
		defer sub.Close()

		frames, err := w.FirehoseRange(cursor, subscribeReplayLimit)
		if err != nil {
			_ = conn.Close(websocket.StatusInternalError, "history")
			return
		}
		// If the consumer asked for events newer than what's retained,
		// signal the gap with #info OutdatedCursor so they know to
		// expect a discontinuity. Subscribers that reconnect with the
		// fresh seq will then resume normally.
		if cursor > 0 && len(frames) == 0 && w.CurrentSeq() > cursor {
			info := world.EncodeOutdatedCursorInfo()
			if writeErr := writer.Write(ctx, info); writeErr != nil {
				return
			}
		}
		for _, f := range frames {
			if err := writer.Write(ctx, f); err != nil {
				return
			}
		}

		// Live phase.
		for {
			select {
			case <-ctx.Done():
				return
			case f, ok := <-sub.Events():
				if !ok {
					return
				}
				if err := writer.Write(ctx, f); err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}
					_ = conn.Close(websocket.StatusInternalError, "write")
					return
				}
			}
		}
	})
}

type subscribeFaultWriter struct {
	conn              *websocket.Conn
	faults            *FaultPlan
	framesBeforeClose int
	framesWritten     int
	faultArmed        bool
}

func (w *subscribeFaultWriter) Write(ctx context.Context, frame []byte) error {
	if err := w.conn.Write(ctx, websocket.MessageBinary, frame); err != nil {
		return err
	}
	w.framesWritten++
	if !w.faultArmed || w.framesWritten < w.framesBeforeClose {
		return nil
	}
	w.faultArmed = false
	w.faults.noteSubscribeDisconnect()
	return w.conn.Close(websocket.StatusTryAgainLater, "simulated subscribeRepos disconnect")
}

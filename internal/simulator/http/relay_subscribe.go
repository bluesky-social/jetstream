package http

import (
	"context"
	"errors"
	"fmt"
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
		replay, replayArmed := faults.onSubscribeConnectReplay()
		inject, injectArmed := faults.onSubscribeConnectInject()
		writer := subscribeFaultWriter{
			conn:              conn,
			world:             w,
			faults:            faults,
			framesBeforeClose: framesBeforeClose,
			faultArmed:        faultArmed,
			replay:            replay,
			replayArmed:       replayArmed,
			inject:            inject,
			injectArmed:       injectArmed,
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

		// Replay the FULL retained backlog from cursor up to the current tip,
		// looping in subscribeReplayLimit-sized pages. A single capped page
		// would silently skip the middle of a large backlog (the consumer
		// treats the OutdatedCursor #info as a no-op, not a re-subscribe), so
		// any history beyond the first page must be drained here. Live events
		// published during replay are buffered in the fanout (subscribed
		// above) and delivered in the live phase below. replayCursor advances
		// past each page; the tip is sampled once so a fast publisher can't
		// keep this loop running forever (newer events arrive via the fanout).
		replayCursor := cursor
		tip := w.CurrentSeq()
		if cursor > 0 && tip > cursor {
			first, ferr := w.FirehoseRange(cursor, 1)
			if ferr == nil && len(first) == 0 {
				// cursor predates retained history: signal the discontinuity.
				info := world.EncodeOutdatedCursorInfo()
				if writeErr := writer.Write(ctx, info); writeErr != nil {
					return
				}
			}
		}
		for replayCursor < tip {
			frames, err := w.FirehoseRange(replayCursor, subscribeReplayLimit)
			if err != nil {
				_ = conn.Close(websocket.StatusInternalError, "history")
				return
			}
			if len(frames) == 0 {
				break
			}
			for _, f := range frames {
				if err := writer.Write(ctx, f); err != nil {
					return
				}
			}
			replayCursor += int64(len(frames))
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
	world             *world.World
	faults            *FaultPlan
	framesBeforeClose int
	framesWritten     int
	faultArmed        bool

	replay      SubscribeReposReplayFault
	replayArmed bool
	// recent is a ring of the last replay.DuplicateLast frames written,
	// maintained only while a duplicate-mode replay fault is armed.
	recent [][]byte

	inject      SubscribeReposInjectFault
	injectArmed bool
	// swallowNext is set when a fired inject fault's SwallowNext takes
	// effect: the next real frame is suppressed from the wire instead
	// of written.
	swallowNext bool
}

func (w *subscribeFaultWriter) Write(ctx context.Context, frame []byte) error {
	// A fired SwallowNext consumes this frame before it reaches the
	// wire. It does not count toward framesWritten (nothing was
	// written), so disconnect/replay thresholds are unaffected, and it
	// never enters the replay ring.
	if w.swallowNext {
		w.swallowNext = false
		w.faults.noteSubscribeSwallow()
		return nil
	}
	if err := w.conn.Write(ctx, websocket.MessageBinary, frame); err != nil {
		return err
	}
	w.framesWritten++
	if w.replayArmed {
		if w.replay.DuplicateLast > 0 {
			w.recent = append(w.recent, frame)
			if len(w.recent) > w.replay.DuplicateLast {
				w.recent = w.recent[1:]
			}
		}
		if w.framesWritten >= w.replay.AfterFrames {
			if err := w.fireReplay(ctx); err != nil {
				return err
			}
		}
	}
	if w.injectArmed && w.framesWritten >= w.inject.AfterFrames {
		if err := w.fireInject(ctx); err != nil {
			return err
		}
	}
	if !w.faultArmed || w.framesWritten < w.framesBeforeClose {
		return nil
	}
	w.faultArmed = false
	w.faults.noteSubscribeDisconnect()
	return w.conn.Close(websocket.StatusTryAgainLater, "simulated subscribeRepos disconnect")
}

// fireInject writes the fault's frame bytes verbatim onto the wire
// (bypassing Write so they don't advance framesWritten, the disconnect
// threshold, or the replay ring) and arms the SwallowNext suppression.
// Fires immediately after the AfterFrames-th counted frame, so the
// injected bytes land between that frame and the next real one — and
// with SwallowNext set, positionally replace the next real frame.
func (w *subscribeFaultWriter) fireInject(ctx context.Context) error {
	w.injectArmed = false
	if len(w.inject.Frame) > 0 {
		if err := w.conn.Write(ctx, websocket.MessageBinary, w.inject.Frame); err != nil {
			return err
		}
	}
	w.swallowNext = w.inject.SwallowNext
	w.faults.noteSubscribeInject()
	return nil
}

// fireReplay re-sends previously delivered frames verbatim — duplicate
// seqs on the wire, exactly what a relay restored from backup would
// emit. Re-sent frames bypass Write so they don't advance
// framesWritten (the disconnect threshold) or re-trigger this fault.
func (w *subscribeFaultWriter) fireReplay(ctx context.Context) error {
	w.replayArmed = false
	var frames [][]byte
	if w.replay.DuplicateLast > 0 {
		frames = w.recent
		w.recent = nil
	} else {
		// Regress mode: replay every retained frame in (RegressToSeq, tip].
		// The tip is sampled once at fire time; pages are bounded by the
		// same cap the normal replay path uses.
		cursor := w.replay.RegressToSeq
		tip := w.world.CurrentSeq()
		for cursor < tip {
			page, err := w.world.FirehoseRange(cursor, subscribeReplayLimit)
			if err != nil {
				_ = w.conn.Close(websocket.StatusInternalError, "replay fault history")
				return fmt.Errorf("simulator: replay fault firehose range from %d: %w", cursor, err)
			}
			if len(page) == 0 {
				break
			}
			frames = append(frames, page...)
			cursor += int64(len(page))
		}
	}
	for _, f := range frames {
		if err := w.conn.Write(ctx, websocket.MessageBinary, f); err != nil {
			return err
		}
	}
	w.faults.noteSubscribeReplay(len(frames))
	return nil
}

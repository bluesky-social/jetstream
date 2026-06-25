package oracle

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"

	"github.com/bluesky-social/jetstream/internal/simulator/fanout"
	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/streaming"
)

// handlerTransport is an http.RoundTripper that serves requests against an
// in-process http.Handler (the simulator mux) with no socket. Used for the
// runtime's unary HTTP (getRepo/listRepos/PLC) so it stays inside a
// testing/synctest bubble.
type handlerTransport struct {
	handler http.Handler
}

// RoundTrip serves req against the handler via a ResponseRecorder. The
// simulator's unary endpoints return whole bodies (CAR exports, listRepos
// pages), so buffering the response is faithful.
func (t handlerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		defer func() { _ = req.Body.Close() }()
	}
	rec := httptest.NewRecorder()
	t.handler.ServeHTTP(rec, req)
	resp := rec.Result()
	resp.Request = req
	return resp, nil
}

// firehoseConn is an in-memory streaming.Conn that replays the simulator's
// subscribeRepos stream with no socket: it mirrors relay_subscribe.go by
// subscribing to the fanout, replaying history from the requested cursor,
// then forwarding live frames. Injected via jetstreamd LiveDial.
type firehoseConn struct {
	world  *world.World
	cursor int64

	mu       sync.Mutex
	replay   [][]byte // history frames not yet delivered
	replayed bool

	sub    *fanout.Subscriber
	closed chan struct{}
	once   sync.Once
}

const firehoseReplayLimit = 1024

// newFirehoseConn subscribes to the fanout BEFORE reading history, matching
// relay_subscribe.go step 2 so no frame is missed in the gap between the
// last replayed row and the first live broadcast.
func newFirehoseConn(w *world.World, cursor int64) (*firehoseConn, error) {
	sub := w.SubscribeFanout()
	frames, err := w.FirehoseRange(cursor, firehoseReplayLimit)
	if err != nil {
		sub.Close()
		return nil, err
	}
	return &firehoseConn{
		world:  w,
		cursor: cursor,
		replay: frames,
		sub:    sub,
		closed: make(chan struct{}),
	}, nil
}

// Read yields replayed history frames first, then blocks on the live fanout
// channel. The fanout receive is a durably-blocking channel op, so an idle
// firehose lets a synctest bubble settle.
func (c *firehoseConn) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	c.mu.Lock()
	if len(c.replay) > 0 {
		frame := c.replay[0]
		c.replay = c.replay[1:]
		c.mu.Unlock()
		return websocket.MessageBinary, frame, nil
	}
	c.replayed = true
	c.mu.Unlock()

	select {
	case frame, ok := <-c.sub.Events():
		if !ok {
			return 0, nil, websocket.CloseError{Code: websocket.StatusNormalClosure}
		}
		return websocket.MessageBinary, frame, nil
	case <-c.closed:
		return 0, nil, websocket.CloseError{Code: websocket.StatusNormalClosure}
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
}

func (c *firehoseConn) Close(websocket.StatusCode, string) error { c.shutdown(); return nil }
func (c *firehoseConn) CloseNow() error                          { c.shutdown(); return nil }
func (c *firehoseConn) SetReadLimit(int64)                       {}

func (c *firehoseConn) shutdown() {
	c.once.Do(func() {
		close(c.closed)
		c.sub.Close()
	})
}

// inProcessDial returns a jetstreamd LiveDial that hands the runtime a
// firehoseConn reading the given world's fanout. The cursor is parsed from
// the dialed URL so reconnects resume correctly.
func inProcessDial(w *world.World) streaming.DialFunc {
	return func(_ context.Context, rawURL string) (streaming.Conn, *http.Response, error) {
		conn, err := newFirehoseConn(w, cursorFromURL(rawURL))
		if err != nil {
			return nil, nil, err
		}
		return conn, nil, nil
	}
}

// cursorFromURL extracts the ?cursor= query param atmos appends on
// (re)dial; absent or malformed yields 0 (replay from the start of
// retained history).
func cursorFromURL(rawURL string) int64 {
	u, err := url.Parse(rawURL)
	if err != nil {
		return 0
	}
	n, err := strconv.ParseInt(u.Query().Get("cursor"), 10, 64)
	if err != nil {
		return 0
	}
	return n
}

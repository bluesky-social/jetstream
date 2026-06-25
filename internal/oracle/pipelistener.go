package oracle

import (
	"context"
	"net"
	"net/http"
	"sync"
)

// pipeAddr is the synthetic address reported by pipeListener and its conns.
type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }
func (pipeAddr) String() string  { return "pipe" }

// pipeListener is an in-memory net.Listener. Accept blocks on a channel and
// returns net.Pipe-backed conns; net.Pipe is implemented with Go channels, so
// reads/writes on these conns are durably blocking inside a testing/synctest
// bubble (unlike a real socket, which is not). Paired with dialContext it lets
// a real http.Server and http.Client — including a websocket upgrade, which
// needs http.Hijacker — talk in-process with no socket, so the runtime's
// public surface runs inside a bubble.
type pipeListener struct {
	conns  chan net.Conn
	closed chan struct{}
	once   sync.Once
}

func newPipeListener() *pipeListener {
	return &pipeListener{
		conns:  make(chan net.Conn),
		closed: make(chan struct{}),
	}
}

// Accept returns the next dialed conn, or net.ErrClosed once Close is called.
func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *pipeListener) Addr() net.Addr { return pipeAddr{} }

// dialContext hands the listener one end of a fresh net.Pipe and returns the
// other to the caller. Used as http.Transport.DialContext.
func (l *pipeListener) dialContext(ctx context.Context, _, _ string) (net.Conn, error) {
	server, client := net.Pipe()
	select {
	case l.conns <- server:
		return client, nil
	case <-l.closed:
		_ = server.Close()
		_ = client.Close()
		return nil, net.ErrClosed
	case <-ctx.Done():
		_ = server.Close()
		_ = client.Close()
		return nil, ctx.Err()
	}
}

// httpClient returns an *http.Client whose connections all route to this
// listener in-process. HTTP/2 is left disabled so the handshake stays HTTP/1.1
// over the cleartext pipe and websocket upgrades work.
func (l *pipeListener) httpClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: l.dialContext,
		},
	}
}

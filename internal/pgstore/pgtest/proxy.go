package pgtest

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// Proxy is an in-process TCP proxy in front of PostgreSQL that injects
// connection faults (plan S2.2). It parses just enough of the wire protocol
// to find message boundaries. It assumes sslmode=disable, so the first
// client message is the untyped startup packet.
type Proxy struct {
	ln     net.Listener
	target string

	mu    sync.Mutex
	conns map[*proxyConn]struct{}

	dropCommit atomic.Int32 // armed DropCommitResponse count
	wg         sync.WaitGroup
}

type proxyConn struct {
	client, server net.Conn
	once           sync.Once
	// dropping is set once a COMMIT is forwarded under DropCommitResponse:
	// the server's replies are swallowed until ReadyForQuery, then both
	// sides close.
	dropping atomic.Bool
}

func (c *proxyConn) close() {
	c.once.Do(func() {
		_ = c.client.Close()
		_ = c.server.Close()
	})
}

// NewProxy listens on a loopback port and forwards to rawURL's host. It
// returns the proxy and rawURL rewritten to go through it. The proxy
// closes when the test ends.
func NewProxy(t testing.TB, rawURL string) (*Proxy, string) {
	t.Helper()
	u, err := url.Parse(rawURL)
	require.NoError(t, err)
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &Proxy{ln: ln, target: u.Host, conns: map[*proxyConn]struct{}{}}
	p.wg.Add(1)
	go p.accept()
	t.Cleanup(p.Close)
	return p, WithHost(t, rawURL, ln.Addr().String())
}

// Close stops the proxy and kills every connection.
func (p *Proxy) Close() {
	_ = p.ln.Close()
	p.KillAll()
	p.wg.Wait()
}

// KillAll closes every live connection at once, in both directions: a
// transaction in flight on one dies mid-statement or mid-transaction.
func (p *Proxy) KillAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.conns {
		c.close()
	}
}

// DropCommitResponse arms the next COMMIT on any connection: the proxy
// forwards it, lets the server apply it, swallows the reply, and closes the
// connection. The client sees the connection die with the commit applied
// and its result unknown.
func (p *Proxy) DropCommitResponse() { p.dropCommit.Add(1) }

func (p *Proxy) accept() {
	defer p.wg.Done()
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		server, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", p.target)
		if err != nil {
			_ = client.Close()
			continue
		}
		c := &proxyConn{client: client, server: server}
		p.mu.Lock()
		p.conns[c] = struct{}{}
		p.mu.Unlock()
		p.wg.Add(2)
		go func() {
			defer p.wg.Done()
			defer p.forget(c)
			p.clientToServer(c)
		}()
		go func() {
			defer p.wg.Done()
			defer c.close()
			p.serverToClient(c)
		}()
	}
}

func (p *Proxy) forget(c *proxyConn) {
	c.close()
	p.mu.Lock()
	delete(p.conns, c)
	p.mu.Unlock()
}

// readMsg reads one message. typed is false only for the startup packet.
func readMsg(r io.Reader, typed bool) (byte, []byte, error) {
	var hdr [5]byte
	h := hdr[:]
	if !typed {
		h = hdr[1:]
	}
	if _, err := io.ReadFull(r, h); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(hdr[1:])
	if n < 4 || n > 1<<30 {
		return 0, nil, errors.New("pgtest: bad message length")
	}
	msg := make([]byte, len(h)+int(n)-4)
	copy(msg, h)
	if _, err := io.ReadFull(r, msg[len(h):]); err != nil {
		return 0, nil, err
	}
	return hdr[0], msg, nil
}

func (p *Proxy) clientToServer(c *proxyConn) {
	typed := false
	for {
		kind, msg, err := readMsg(c.client, typed)
		if err != nil {
			return
		}
		typed = true
		if kind == 'Q' && isCommit(msg[5:]) && p.takeDrop() {
			c.dropping.Store(true)
		}
		if _, err := c.server.Write(msg); err != nil {
			return
		}
	}
}

func (p *Proxy) takeDrop() bool {
	for {
		n := p.dropCommit.Load()
		if n <= 0 {
			return false
		}
		if p.dropCommit.CompareAndSwap(n, n-1) {
			return true
		}
	}
}

func isCommit(query []byte) bool {
	q := bytes.TrimSpace(bytes.TrimRight(query, "\x00"))
	q = bytes.TrimRight(q, ";")
	return bytes.EqualFold(q, []byte("commit")) || bytes.EqualFold(q, []byte("end"))
}

func (p *Proxy) serverToClient(c *proxyConn) {
	for {
		kind, msg, err := readMsg(c.server, true)
		if err != nil {
			return
		}
		if c.dropping.Load() {
			if kind == 'Z' {
				// The server finished the COMMIT: it applied. Now the
				// client learns nothing but a dead connection.
				return
			}
			continue
		}
		if _, err := c.client.Write(msg); err != nil {
			return
		}
	}
}

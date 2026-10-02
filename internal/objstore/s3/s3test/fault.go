package s3test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
)

// Op names the S3 request a Fault targets. The names match memblob's ops.
type Op string

const (
	OpPut      Op = "put"
	OpGet      Op = "get"
	OpGetRange Op = "get_range"
	OpDelete   Op = "delete"
)

func opOf(req *http.Request) Op {
	switch req.Method {
	case http.MethodPut:
		return OpPut
	case http.MethodDelete:
		return OpDelete
	case http.MethodGet:
		if req.Header.Get("Range") != "" {
			return OpGetRange
		}
		return OpGet
	}
	return Op(strings.ToLower(req.Method))
}

// FaultKind is what a Fault does to one request.
type FaultKind string

const (
	// FaultStatus answers with Status (default 503) and an S3 XML error
	// with Code (default "InternalError"), without reaching the backend.
	FaultStatus FaultKind = "status"
	// FaultTimeout hangs until the request's context ends, like a stalled
	// connection.
	FaultTimeout FaultKind = "timeout"
	// FaultErrorAfter forwards the request and then returns a connection
	// error: the unknown-result case where a PUT landed but its response
	// was lost.
	FaultErrorAfter FaultKind = "error_after"
	// FaultTruncate forwards a GET and cuts the response body in half while
	// keeping the original Content-Length. GET only.
	FaultTruncate FaultKind = "truncate"
	// FaultWrongBytes forwards a GET and flips one bit of the response body,
	// with no error. GET only: a real backend rejects a PUT whose body does
	// not match its signed payload hash.
	FaultWrongBytes FaultKind = "wrong_bytes"
)

// ErrInjected is the connection error FaultErrorAfter returns.
var ErrInjected = errors.New("s3test: injected connection error")

// Fault injects one failure into matching requests. Matching is
// deterministic (operation, key substring, occurrence count), so a failing
// run reproduces exactly.
type Fault struct {
	// Op, when non-empty, restricts the fault to that operation.
	Op Op
	// Key, when non-empty, must be a substring of the request path.
	Key string
	// Ordinal selects the Ordinal-th (1-based) matching request. Zero
	// faults every matching request.
	Ordinal int
	Kind    FaultKind
	// Status and Code configure FaultStatus.
	Status int
	Code   string

	seen  atomic.Int64
	fired atomic.Int64
}

// Fired returns how many requests the fault has hit.
func (f *Fault) Fired() int64 { return f.fired.Load() }

func (f *Fault) match(req *http.Request) bool {
	if f.Op != "" && f.Op != opOf(req) {
		return false
	}
	if f.Key != "" && !strings.Contains(req.URL.Path, f.Key) {
		return false
	}
	n := f.seen.Add(1)
	return f.Ordinal == 0 || n == int64(f.Ordinal)
}

// FaultTransport wraps Base (http.DefaultTransport when nil) and applies the
// first matching Fault to each request. Every fault sees every request, so
// each one's ordinal counts independently. Production code never installs
// one.
type FaultTransport struct {
	Base   http.RoundTripper
	Faults []*Fault
}

var _ http.RoundTripper = (*FaultTransport)(nil)

// Unfired returns the faults that never hit a request, so a test can assert
// its scenario actually happened.
func (t *FaultTransport) Unfired() []*Fault {
	var out []*Fault
	for _, f := range t.Faults {
		if f.Fired() == 0 {
			out = append(out, f)
		}
	}
	return out
}

// RoundTrip implements http.RoundTripper.
func (t *FaultTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}
	var hit *Fault
	for _, f := range t.Faults {
		if f.match(req) && hit == nil {
			hit = f
		}
	}
	if hit == nil {
		return base.RoundTrip(req)
	}
	hit.fired.Add(1)
	switch hit.Kind {
	case FaultStatus:
		status, code := hit.Status, hit.Code
		if status == 0 {
			status = http.StatusServiceUnavailable
		}
		if code == "" {
			code = "InternalError"
		}
		drain(req.Body)
		return xmlError(req, status, code, "injected by s3test"), nil
	case FaultTimeout:
		drain(req.Body)
		<-req.Context().Done()
		return nil, req.Context().Err()
	case FaultErrorAfter:
		resp, err := base.RoundTrip(req)
		if err == nil {
			drain(resp.Body)
		}
		return nil, ErrInjected
	case FaultTruncate, FaultWrongBytes:
		if req.Method != http.MethodGet {
			return nil, fmt.Errorf("s3test: fault %q does not apply to %s", hit.Kind, req.Method)
		}
		resp, err := base.RoundTrip(req)
		if err != nil || resp.StatusCode/100 != 2 {
			return resp, err
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return nil, err
		}
		if hit.Kind == FaultTruncate {
			// Content-Length still claims the full body.
			resp.Body = io.NopCloser(bytes.NewReader(body[:len(body)/2]))
			return resp, nil
		}
		if len(body) > 0 {
			body[len(body)/2] ^= 0x01
		}
		resp.Body = io.NopCloser(bytes.NewReader(body))
		resp.ContentLength = int64(len(body))
		resp.Header.Set("Content-Length", strconv.Itoa(len(body)))
		return resp, nil
	}
	return nil, fmt.Errorf("s3test: unknown fault kind %q", hit.Kind)
}

func drain(body io.ReadCloser) {
	if body != nil {
		_, _ = io.Copy(io.Discard, body)
		_ = body.Close()
	}
}

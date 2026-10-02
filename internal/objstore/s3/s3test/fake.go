package s3test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

// FakeEndpoint is the endpoint to configure a Blob with when its Transport
// is a Fake. Nothing listens on it.
const FakeEndpoint = "http://s3.fake.invalid"

// Fake is an in-memory S3 behind an http.RoundTripper. It implements just
// the path-style PutObject, GetObject (with a single Range), and
// DeleteObject the Blob sends, with S3's status codes and XML error bodies.
// It does not check signatures.
type Fake struct {
	// MissingAs403 answers a missing key with 403 AccessDenied, as real AWS
	// S3 does when the credentials lack ListBucket.
	MissingAs403 bool

	mu      sync.Mutex
	objects map[string][]byte // "bucket/key"
}

var _ http.RoundTripper = (*Fake)(nil)

// NewFake returns an empty Fake.
func NewFake() *Fake {
	return &Fake{objects: make(map[string][]byte)}
}

// Len returns the number of stored objects.
func (f *Fake) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.objects)
}

// RoundTrip implements http.RoundTripper.
func (f *Fake) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := req.Context().Err(); err != nil {
		return nil, err
	}
	path := strings.TrimPrefix(req.URL.Path, "/")
	bucket, key, ok := strings.Cut(path, "/")
	if !ok || bucket == "" || key == "" {
		return xmlError(req, http.StatusBadRequest, "InvalidRequest", "want a path-style /bucket/key URL"), nil
	}
	name := bucket + "/" + key
	switch req.Method {
	case http.MethodPut:
		var data []byte
		if req.Body != nil {
			var err error
			if data, err = io.ReadAll(req.Body); err != nil {
				return nil, err
			}
		}
		if data == nil {
			data = []byte{}
		}
		f.mu.Lock()
		f.objects[name] = data
		f.mu.Unlock()
		return respond(req, http.StatusOK, nil, nil), nil
	case http.MethodGet:
		f.mu.Lock()
		data, ok := f.objects[name]
		f.mu.Unlock()
		if !ok {
			if f.MissingAs403 {
				return xmlError(req, http.StatusForbidden, "AccessDenied", "Access Denied"), nil
			}
			return xmlError(req, http.StatusNotFound, "NoSuchKey", "The specified key does not exist."), nil
		}
		rng := req.Header.Get("Range")
		if rng == "" {
			return respond(req, http.StatusOK, bytes.Clone(data), nil), nil
		}
		first, last, err := parseRange(rng)
		if err != nil {
			return xmlError(req, http.StatusBadRequest, "InvalidArgument", err.Error()), nil
		}
		size := int64(len(data))
		if first >= size {
			return xmlError(req, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "The requested range is not satisfiable"), nil
		}
		last = min(last, size-1)
		h := http.Header{"Content-Range": {fmt.Sprintf("bytes %d-%d/%d", first, last, size)}}
		return respond(req, http.StatusPartialContent, bytes.Clone(data[first:last+1]), h), nil
	case http.MethodDelete:
		f.mu.Lock()
		delete(f.objects, name)
		f.mu.Unlock()
		return respond(req, http.StatusNoContent, nil, nil), nil
	}
	return xmlError(req, http.StatusMethodNotAllowed, "MethodNotAllowed", req.Method), nil
}

func parseRange(h string) (first, last int64, err error) {
	spec, ok := strings.CutPrefix(h, "bytes=")
	a, b, ok2 := strings.Cut(spec, "-")
	if !ok || !ok2 {
		return 0, 0, fmt.Errorf("bad range %q", h)
	}
	if first, err = strconv.ParseInt(a, 10, 64); err != nil {
		return 0, 0, err
	}
	if last, err = strconv.ParseInt(b, 10, 64); err != nil {
		return 0, 0, err
	}
	if last < first {
		return 0, 0, fmt.Errorf("bad range %q", h)
	}
	return first, last, nil
}

func respond(req *http.Request, status int, body []byte, h http.Header) *http.Response {
	if h == nil {
		h = http.Header{}
	}
	h.Set("Content-Length", strconv.Itoa(len(body)))
	return &http.Response{
		Status:        fmt.Sprintf("%d %s", status, http.StatusText(status)),
		StatusCode:    status,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        h,
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       req,
	}
}

func xmlError(req *http.Request, status int, code, msg string) *http.Response {
	body := fmt.Appendf(nil, `<?xml version="1.0" encoding="UTF-8"?>`+"\n"+
		`<Error><Code>%s</Code><Message>%s</Message><RequestId>fake</RequestId></Error>`, code, msg)
	if req.Method == http.MethodHead {
		body = nil
	}
	return respond(req, status, body, http.Header{"Content-Type": {"application/xml"}})
}

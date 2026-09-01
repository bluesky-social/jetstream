package xrpcapi

import (
	"context"
	"net/http"
)

const (
	getSegmentNSID = "network.bsky.jetstream.getSegment"
	getBlockNSID   = "network.bsky.jetstream.getBlock"
)

type headRequestKey struct{}

// HeadHandler returns the exact-method adapter for one of the two archive
// query endpoints that support HEAD. Atmos v0.3.7 deliberately routes query
// handlers as GET only, so the adapter lets Atmos keep parsing parameters and
// writing XRPC errors while the shared archive handler sees HEAD semantics.
// Callers should mount this only as an exact HEAD route on the public mux.
func (s *Server) HeadHandler(nsid string) http.Handler {
	switch nsid {
	case getSegmentNSID, getBlockNSID:
	default:
		panic("xrpcapi: HEAD is only supported for archive query endpoints")
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), headRequestKey{}, true)
		getReq := r.Clone(ctx)
		getReq.Method = http.MethodGet
		// ServeHTTP is intentionally used rather than calling an individual
		// handler: it preserves Atmos's query parameter decoding and XRPC error
		// envelopes for malformed and unavailable requests.
		s.xrpc.ServeHTTP(w, getReq)
	})
}

func isHeadRequest(r *http.Request) bool {
	if r.Method == http.MethodHead {
		return true
	}
	_, ok := r.Context().Value(headRequestKey{}).(bool)
	return ok
}

func contentRequest(r *http.Request) *http.Request {
	if !isHeadRequest(r) {
		return r
	}
	if r.Method == http.MethodHead {
		return r
	}
	getReq := r.Clone(r.Context())
	getReq.Method = http.MethodHead
	return getReq
}

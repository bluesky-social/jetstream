package xrpcapi

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/bluesky-social/jetstream/api/jetstream"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/atmos/xrpcserver"
)

// DictionaryConfig wires the /subscribe-v2 zstd dictionary into the
// download endpoint. The server currently publishes exactly one dictionary
// (the compiled-in current one); retired IDs 404 and the client re-fetches
// without an id parameter to discover the current dictionary.
type DictionaryConfig struct {
	// ID is the zstd dictionary ID embedded in Bytes' header
	// (subscribe.DictionaryV2ID).
	ID uint32
	// Bytes is the raw structured dictionary (subscribe.DictionaryV2()).
	// Treated as read-only.
	Bytes []byte
}

// getZstdDictionaryHandler serves the /subscribe-v2 compression dictionary.
// The artifact is a small in-memory blob, immutable for a given ID, so the
// handler is a straight conditional-GET-capable byte serve: ETag on the ID,
// long-lived public caching (the ID names the content, so a stale cache
// can never serve wrong bytes for an ID).
type getZstdDictionaryHandler struct {
	dict    DictionaryConfig
	modTime time.Time
}

func newGetZstdDictionaryHandler(dict DictionaryConfig) *getZstdDictionaryHandler {
	// http.ServeContent needs a modtime for If-Modified-Since; process
	// start is correct enough (ETag is the authoritative validator).
	return &getZstdDictionaryHandler{dict: dict, modTime: time.Now().UTC()}
}

func (h *getZstdDictionaryHandler) ServeXRPC(_ context.Context, w http.ResponseWriter, r *xrpcserver.Request) error {
	id, err := r.Params.Int64("id")
	switch {
	case err != nil && r.Params.Has("id"):
		return xrpcserver.InvalidRequest("id must be an integer zstd dictionary ID")
	case r.Params.Has("id") && (id <= 0 || uint64(id) != uint64(h.dict.ID)):
		return &xrpc.Error{
			StatusCode: http.StatusNotFound,
			Name:       jetstream.ErrJetstreamGetZstdDictionary_DictionaryNotFound,
			Message:    fmt.Sprintf("no dictionary with id %d; current dictionary id is %d", id, h.dict.ID),
		}
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", fmt.Sprintf("%q", fmt.Sprintf("zstd-dict-%d", h.dict.ID)))
	// Immutable for a given ID; a year of public caching lets a CDN absorb
	// the fetch storm when a large client fleet restarts.
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	w.Header().Set("X-Zstd-Dictionary-Id", fmt.Sprintf("%d", h.dict.ID))
	http.ServeContent(w, r.HTTPReq, "zstd_dictionary", h.modTime, bytes.NewReader(h.dict.Bytes))
	return nil
}

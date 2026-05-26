// Package http hosts the simulator's HTTP surface: PLC, PDS, and
// relay endpoints under a single mux at production paths.
package http

import (
	"net/http"
	"strings"

	"github.com/bluesky-social/jetstream-v2/internal/simulator/world"
)

// NewHandler builds the simulator's HTTP handler. publicURL is the
// externally-reachable base URL of the simulator (without trailing
// slash); it's published in DID documents as the PDS endpoint so
// atmos's verifier rounds back to us for getRepo.
func NewHandler(w *world.World, publicURL string) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /xrpc/com.atproto.sync.getRepo", newPDSGetRepoHandler(w))
	mux.Handle("GET /xrpc/com.atproto.sync.listRepos", newRelayListReposHandler(w))
	mux.Handle("GET /xrpc/com.atproto.sync.subscribeRepos", newRelaySubscribeReposHandler(w))

	// PLC's `/<did>` doesn't fit Go ServeMux's path syntax cleanly
	// because `did:` contains a colon. Pre-route any request whose
	// first path segment starts with `did:` through the PLC handler.
	plc := newPLCHandler(w, strings.TrimRight(publicURL, "/"))
	root := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/did:") {
			plc.ServeHTTP(rw, r)
			return
		}
		mux.ServeHTTP(rw, r)
	})
	return root
}

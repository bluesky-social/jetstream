// Package http hosts the simulator's HTTP surface: PLC, PDS, and
// relay endpoints under a single mux at production paths.
package http

import (
	"net/http"
	"strings"

	"github.com/bluesky-social/jetstream/internal/simulator/world"
)

// NewHandler builds the simulator's HTTP handler. publicURL is the
// externally-reachable base URL of the simulator (without trailing
// slash); it's published in DID documents as the PDS endpoint so
// atmos's verifier rounds back to us for getRepo.
func NewHandler(w *world.World, publicURL string) http.Handler {
	return NewHandlerWithOptions(w, publicURL, HandlerOptions{})
}

// HandlerOptions carries optional simulator test hooks.
type HandlerOptions struct {
	// Faults, when non-nil, is a deterministic fault schedule the
	// getRepo handler consults before serving each CAR. nil (the
	// zero value) means no fault injection; the oracle fault-injection
	// harness is the primary caller that sets it.
	Faults *FaultPlan

	// OnGetRepoServed, when non-nil, fires after getRepo has fully
	// served a DID's current-head CAR. It is a TIMING SIGNAL ("the
	// snapshot for this DID is taken; its head rev is pinned"), never a
	// data channel — getRepo carries creates only (current head), so a
	// caller that wants to land a durable update/delete uses this hook
	// to learn when it is safe to generate that mutation on the live
	// firehose at a rev above the now-pinned backfill head. The restart
	// oracle tier is the primary caller; nil means no-op. It does NOT
	// fire on the fault/truncation paths, which do not serve a clean
	// snapshot.
	OnGetRepoServed func(did string)

	// EnableFirehoseTip mounts the read-only GET /_oracle/firehose-tip
	// endpoint, which reports the world's current firehose tip seq. It is
	// off by default (production never sets it) and exists for the restart
	// oracle's cutover gate: a child queries the tip so it can hold the
	// pre-cutover barrier until its bootstrap-live consumer has durably
	// archived every frame up to it. The path is namespaced off the
	// atproto xrpc surface so it cannot collide with a real lexicon method.
	EnableFirehoseTip bool
}

// NewHandlerWithOptions builds the simulator's HTTP handler, optionally
// wiring deterministic fault injection via opts.Faults. It exists for
// the oracle fault-injection harness and similar tests that need to
// drive failure modes; production and tests that don't inject faults
// should call NewHandler, which passes an empty HandlerOptions.
func NewHandlerWithOptions(w *world.World, publicURL string, opts HandlerOptions) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /xrpc/com.atproto.sync.getRepo", newPDSGetRepoHandler(w, opts.Faults, opts.OnGetRepoServed))
	mux.Handle("GET /xrpc/com.atproto.sync.listRepos", newRelayListReposHandler(w, opts.Faults))
	mux.Handle("GET /xrpc/com.atproto.sync.subscribeRepos", newRelaySubscribeReposHandler(w, opts.Faults))
	if opts.EnableFirehoseTip {
		mux.Handle("GET "+oracleFirehoseTipURLPath, newFirehoseTipHandler(w))
	}

	// PLC's `/<did>` doesn't fit Go ServeMux's path syntax cleanly
	// because `did:` contains a colon. Pre-route any request whose
	// first path segment starts with `did:` through the PLC handler.
	plc := newPLCHandler(w, strings.TrimRight(publicURL, "/"), opts.Faults)
	root := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/did:") {
			plc.ServeHTTP(rw, r)
			return
		}
		mux.ServeHTTP(rw, r)
	})
	return root
}

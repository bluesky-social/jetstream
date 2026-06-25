package http

import (
	"bytes"
	"net/http"

	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/jcalabro/atmos"
)

// newPDSGetRepoHandler serves com.atproto.sync.getRepo. Streams CAR
// bytes straight to the response. Ignores `since` in v1 — always
// returns the full repo (which is valid behavior; consumers can
// request diffs but aren't required to).
func newPDSGetRepoHandler(w *world.World, faults *FaultPlan, onServed func(did string)) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		didStr := r.URL.Query().Get("did")
		did, err := atmos.ParseDID(didStr)
		if err != nil {
			http.Error(rw, "bad did", http.StatusBadRequest)
			return
		}
		// Inject a scheduled fault before touching the real repo. This is
		// a clean early exit: nothing has been written to rw yet, so
		// http.Error sets a proper status code and body — unlike the
		// mid-stream CAR truncation below, which can only happen after
		// headers are committed. Each call consumes one unit of this
		// DID's fault budget; once exhausted, getRepo serves normally.
		if status, ok := faults.maybeGetRepoHTTPFault(string(did)); ok {
			http.Error(rw, "simulated getRepo fault", status)
			return
		}
		acct, ok, err := w.FindAccountByDID(did)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		if !ok {
			http.NotFound(rw, r)
			return
		}
		rw.Header().Set("Content-Type", "application/vnd.ipld.car")
		if faults.maybeGetRepoCARTruncation(string(did)) {
			var buf bytes.Buffer
			if err := w.ExportRepoCAR(acct.Index, &buf); err != nil {
				return
			}
			body := buf.Bytes()
			if len(body) > 0 {
				_, _ = rw.Write(body[:max(1, len(body)/2)])
			}
			return
		}
		if err := w.ExportRepoCAR(acct.Index, rw); err != nil {
			// Headers may already be flushed; the response body is
			// committed at this point. Nothing useful we can do
			// except let the client see a truncated CAR. A future
			// metric would surface the rate of these.
			return
		}
		// The snapshot for this DID is now fully served; its head rev is
		// pinned. Fire the timing signal AFTER the body is written so a
		// caller can safely generate a post-backfill mutation (see
		// HandlerOptions.OnGetRepoServed).
		//
		// LOAD-BEARING ORDERING (restart oracle): onServed runs before this
		// handler returns, and ExportRepoCAR above neither sets Content-Length
		// nor flushes, so the response stays chunked — the client's CAR reader
		// sees io.EOF only when the terminating chunk is emitted as the handler
		// returns, i.e. strictly AFTER onServed. The restart-tier chain
		// coordinator relies on exactly this: it generates the live op for a DID
		// inside onServed, and that op must be committed to the world (ground
		// truth) before the child observes getRepo EOF -> marks backfill
		// complete -> cuts over. Adding a Content-Length header or an explicit
		// Flush before onServed would let the child see EOF first, dropping the
		// live op from the child's pre-cutover window while it stays in ground
		// truth -> a "compare oracle model" divergence. Keep the body unflushed
		// and onServed last.
		if onServed != nil {
			onServed(string(did))
		}
	})
}

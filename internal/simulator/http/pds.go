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
func newPDSGetRepoHandler(w *world.World, faults *FaultPlan) http.Handler {
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
	})
}

package http

import (
	"net/http"

	"github.com/bluesky-social/jetstream-v2/internal/simulator/world"
	"github.com/jcalabro/atmos"
)

// newPDSGetRepoHandler serves com.atproto.sync.getRepo. Streams CAR
// bytes straight to the response. Ignores `since` in v1 — always
// returns the full repo (which is valid behavior; consumers can
// request diffs but aren't required to).
func newPDSGetRepoHandler(w *world.World) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		didStr := r.URL.Query().Get("did")
		did, err := atmos.ParseDID(didStr)
		if err != nil {
			http.Error(rw, "bad did", http.StatusBadRequest)
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
		if err := w.ExportRepoCAR(acct.Index, rw); err != nil {
			// Headers may already be flushed; the response body is
			// committed at this point. Nothing useful we can do
			// except let the client see a truncated CAR. A future
			// metric would surface the rate of these.
			return
		}
	})
}

package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/jcalabro/atmos"
)

// newPDSGetRepoHandler serves com.atproto.sync.getRepo. Streams CAR
// bytes straight to the response. Ignores `since` in v1 — always
// returns the full repo (which is valid behavior; consumers can
// request diffs but aren't required to).
func newPDSGetRepoHandler(w *world.World, topology pdsTopology, faults *FaultPlan, onServed func(did string)) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		didStr := r.URL.Query().Get("did")
		did, err := atmos.ParseDID(didStr)
		if err != nil {
			http.Error(rw, "bad did", http.StatusBadRequest)
			return
		}
		// Reject unrecognized authorities before anything else — including
		// fault injection. A misrouted client must see the topology's 404,
		// not consume another DID's fault budget or receive a simulated
		// response that masks the routing bug.
		if topology.virtual {
			if _, direct := topology.pdsIndex(r.Host); !direct && !strings.EqualFold(r.Host, topology.relayAuthority) {
				http.NotFound(rw, r)
				return
			}
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
		if fault, ok := faults.maybeGetRepoResponseFault(string(did)); ok {
			for k, v := range fault.Headers {
				rw.Header().Set(k, v)
			}
			if fault.RedirectLocation != "" {
				http.Redirect(rw, r, fault.RedirectLocation, fault.Status)
				return
			}
			if fault.Error != "" || fault.Message != "" {
				rw.Header().Set("Content-Type", "application/json")
				rw.WriteHeader(fault.Status)
				_ = json.NewEncoder(rw).Encode(map[string]string{
					"error":   fault.Error,
					"message": fault.Message,
				})
				return
			}
			http.Error(rw, "simulated getRepo fault", fault.Status)
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
		// Unknown authorities were rejected before fault injection above;
		// here a recognized virtual PDS serves only its own accounts (the
		// relay authority keeps legacy getRepo behavior — the retry path's
		// relay fallback depends on it).
		if pdsIndex, direct := topology.pdsIndex(r.Host); topology.virtual && direct && w.PDSIndexForAccount(acct.Index) != pdsIndex {
			http.NotFound(rw, r)
			return
		}
		if status, unavailable, err := w.RepoUnavailableStatus(acct.Index); err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		} else if unavailable {
			writeXRPCError(rw, http.StatusBadRequest, unavailableGetRepoErrorName(status),
				fmt.Sprintf("repo %s is %s", did, status))
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
		// Signal after writing the snapshot but before returning.
		// With no Content-Length or Flush, the chunked response
		// reaches EOF only after onServed. The restart oracle relies
		// on its live mutation being committed before the child sees
		// EOF and can finish backfill. Earlier flushing or a
		// Content-Length could let cutover omit that mutation.
		if onServed != nil {
			onServed(string(did))
		}
	})
}

func unavailableGetRepoErrorName(status string) string {
	switch status {
	case "takendown":
		return "RepoTakendown"
	case "suspended":
		return "RepoSuspended"
	case "deactivated":
		return "RepoDeactivated"
	default:
		return "InvalidRequest"
	}
}

func writeXRPCError(rw http.ResponseWriter, status int, name, message string) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(status)
	_ = json.NewEncoder(rw).Encode(struct {
		Error   string `json:"error"`
		Message string `json:"message,omitempty"`
	}{
		Error:   name,
		Message: message,
	})
}

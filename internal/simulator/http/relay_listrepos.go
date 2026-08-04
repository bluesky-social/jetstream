package http

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/bluesky-social/jetstream/internal/simulator/world"
)

type listReposOutput struct {
	Cursor string                 `json:"cursor,omitempty"`
	Repos  []listReposOutputEntry `json:"repos"`
}

type listReposOutputEntry struct {
	DID    string `json:"did"`
	Head   string `json:"head"`
	Rev    string `json:"rev"`
	Active bool   `json:"active"`
}

func newRelayListHostsHandler(w *world.World, topology pdsTopology, faults *FaultPlan) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if topology.virtual && !strings.EqualFold(r.Host, topology.relayAuthority) {
			http.NotFound(rw, r)
			return
		}
		if status, ok := faults.maybeListHostsHTTPFault(r.URL.Query().Get("cursor")); ok {
			writeFleetHTTPFault(rw, status, "simulated listHosts fault")
			return
		}
		hosts := make([]map[string]any, 0, len(topology.hosts))
		for i, hostname := range topology.hosts {
			accounts := w.AccountCount()
			if topology.virtual {
				accounts = w.RelayAccountFloor(i)
			}
			hosts = append(hosts, map[string]any{
				"hostname": hostname, "status": faults.listHostsStatus(hostname, "active"), "accountCount": accounts, "seq": w.CurrentSeq(),
			})
		}
		rw.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(rw).Encode(map[string]any{"hosts": hosts})
	})
}

// newRelayListReposHandler serves com.atproto.sync.listRepos. Cursor
// is the stringified next-start index; "" means start at 0. Limit is
// capped at 1000 (the protocol max).
func newRelayListReposHandler(w *world.World, topology pdsTopology, faults *FaultPlan, pageLimitCap int) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		cursor := q.Get("cursor")
		if _, direct := topology.pdsIndex(r.Host); direct {
			if status, ok := faults.maybePDSListReposHTTPFault(r.Host, cursor); ok {
				writeFleetHTTPFault(rw, status, "simulated PDS listRepos fault")
				return
			}
		}
		start := 0
		if cursor != "" {
			n, err := strconv.Atoi(cursor)
			if err != nil || n < 0 {
				http.Error(rw, "bad cursor", http.StatusBadRequest)
				return
			}
			start = n
		}
		limit := 50
		if l := q.Get("limit"); l != "" {
			n, err := strconv.Atoi(l)
			if err != nil || n <= 0 {
				http.Error(rw, "bad limit", http.StatusBadRequest)
				return
			}
			limit = n
		}
		if pageLimitCap > 0 && limit > pageLimitCap {
			limit = pageLimitCap
		}
		mode, faulted := faults.maybeListReposFault(cursor)
		pageStart := start
		pageLimit := limit
		if faulted && mode == ListReposFaultDuplicatePreviousPage && start > 0 {
			pageStart = max(0, start-limit)
		}
		if faulted && mode == ListReposFaultShrinkPage {
			pageLimit = max(1, limit/2)
		}
		var entries []world.ListReposEntry
		var next int
		var err error
		total := w.AccountCount()
		if pdsIndex, direct := topology.pdsIndex(r.Host); direct {
			if topology.virtual {
				entries, next, err = w.ListReposPageForPDS(pdsIndex, pageStart, pageLimit)
				total = w.PDSAccountCount(pdsIndex)
			} else {
				entries, next, err = w.ListReposPage(pageStart, pageLimit)
			}
		} else if topology.virtual && strings.EqualFold(r.Host, topology.relayAuthority) {
			entries, next, err = w.RelayListReposPage(pageStart, pageLimit)
			total = 0
			for i := range w.AccountCount() {
				if w.RelayKnowsAccount(i) {
					total++
				}
			}
		} else {
			http.NotFound(rw, r)
			return
		}
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		if faulted && mode == ListReposFaultDuplicatePreviousPage {
			next = start
		}
		out := listReposOutput{
			Repos: make([]listReposOutputEntry, len(entries)),
		}
		for i, e := range entries {
			out.Repos[i] = listReposOutputEntry{
				DID:    string(e.DID),
				Head:   e.Head,
				Rev:    e.Rev,
				Active: e.Active,
			}
		}
		// Cursor is omitted on the last page.
		if faulted && mode == ListReposFaultCursorLoop {
			if cursor != "" {
				out.Cursor = cursor
			} else {
				out.Cursor = strconv.Itoa(start)
			}
		} else if next < total {
			out.Cursor = strconv.Itoa(next)
		}
		rw.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(rw).Encode(out)
	})
}

func writeFleetHTTPFault(rw http.ResponseWriter, status int, message string) {
	if status == http.StatusTooManyRequests {
		rw.Header().Set("RateLimit-Remaining", "0")
	}
	http.Error(rw, message, status)
}

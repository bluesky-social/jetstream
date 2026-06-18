package http

import (
	"encoding/json"
	"net/http"
	"strconv"

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

// newRelayListReposHandler serves com.atproto.sync.listRepos. Cursor
// is the stringified next-start index; "" means start at 0. Limit is
// capped at 1000 (the protocol max).
func newRelayListReposHandler(w *world.World) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		start := 0
		if c := q.Get("cursor"); c != "" {
			n, err := strconv.Atoi(c)
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
		entries, next, err := w.ListReposPage(start, limit)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
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
		if next < w.AccountCount() {
			out.Cursor = strconv.Itoa(next)
		}
		rw.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(rw).Encode(out)
	})
}

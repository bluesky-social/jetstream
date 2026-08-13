package http_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	simhttp "github.com/bluesky-social/jetstream/internal/simulator/http"
	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/stretchr/testify/require"
)

type topologyListHostsOutput struct {
	Hosts []struct {
		Hostname     string `json:"hostname"`
		AccountCount int    `json:"accountCount"`
	} `json:"hosts"`
}

type topologyListReposOutput struct {
	Repos []struct {
		DID string `json:"did"`
	} `json:"repos"`
}

func topologyRequest(t *testing.T, handler http.Handler, authority, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+authority+path, nil)
	req.Host = authority
	rw := httptest.NewRecorder()
	handler.ServeHTTP(rw, req)
	return rw
}

func TestVirtualPDSTopology_ListHostsPartitionAndRelayGap(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 40, 1)
	handler := simhttp.NewHandler(w, "http://sim.invalid")

	rw := topologyRequest(t, handler, "sim.invalid", "/xrpc/com.atproto.sync.listHosts?limit=1000")
	require.Equal(t, http.StatusOK, rw.Code)
	var roster topologyListHostsOutput
	require.NoError(t, json.Unmarshal(rw.Body.Bytes(), &roster))
	require.Len(t, roster.Hosts, w.PDSHostCount())
	relayFloor := 0
	for i, host := range roster.Hosts {
		require.Equal(t, world.VirtualPDSHostname(i), host.Hostname)
		require.Equal(t, w.RelayAccountFloor(i), host.AccountCount)
		relayFloor += host.AccountCount
	}
	require.Less(t, relayFloor, w.AccountCount(), "listHosts accountCount must remain an incomplete relay floor")

	directDIDs := make(map[string]string, w.AccountCount())
	for i := range w.PDSHostCount() {
		hostname := world.VirtualPDSHostname(i)
		rw = topologyRequest(t, handler, hostname, "/xrpc/com.atproto.sync.listRepos?limit=1000")
		require.Equal(t, http.StatusOK, rw.Code)
		var page topologyListReposOutput
		require.NoError(t, json.Unmarshal(rw.Body.Bytes(), &page))
		require.Len(t, page.Repos, w.PDSAccountCount(i))
		for _, entry := range page.Repos {
			_, duplicate := directDIDs[entry.DID]
			require.False(t, duplicate, "DID %s appeared on multiple authoritative PDSes", entry.DID)
			directDIDs[entry.DID] = hostname
		}
	}
	require.Len(t, directDIDs, w.AccountCount())

	rw = topologyRequest(t, handler, "sim.invalid", "/xrpc/com.atproto.sync.listRepos?limit=1000")
	require.Equal(t, http.StatusOK, rw.Code)
	var relayPage topologyListReposOutput
	require.NoError(t, json.Unmarshal(rw.Body.Bytes(), &relayPage))
	require.Less(t, len(relayPage.Repos), len(directDIDs))
	relayDIDs := make(map[string]struct{}, len(relayPage.Repos))
	for _, entry := range relayPage.Repos {
		relayDIDs[entry.DID] = struct{}{}
	}
	missing := 0
	for did := range directDIDs {
		if _, ok := relayDIDs[did]; !ok {
			missing++
		}
	}
	require.Greater(t, missing, 0, "direct topology must expose at least one relay-gap DID")
}

func TestVirtualPDSTopology_WrongPDSRejectsGetRepo(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 8, 1)
	handler := simhttp.NewHandler(w, "http://sim.invalid")
	acct, err := w.LoadAccount(0)
	require.NoError(t, err)
	owner := w.PDSIndexForAccount(acct.Index)
	wrong := (owner + 1) % w.PDSHostCount()
	path := "/xrpc/com.atproto.sync.getRepo?did=" + url.QueryEscape(string(acct.DID))

	rw := topologyRequest(t, handler, world.VirtualPDSHostname(wrong), path)
	require.Equal(t, http.StatusNotFound, rw.Code)
	rw = topologyRequest(t, handler, world.VirtualPDSHostname(owner), path)
	require.Equal(t, http.StatusOK, rw.Code, fmt.Sprintf("owner PDS %d must serve its repo", owner))
}

func TestVirtualPDSTopology_PerHostFaultsAndStatusLies(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 20, 1)
	faults := simhttp.NewFaultPlan()
	hostname := world.VirtualPDSHostname(0)
	faults.AddListHostsHTTPFailures("", http.StatusServiceUnavailable, 1)
	faults.SetListHostsStatus(hostname, "offline")
	faults.AddPDSListReposHTTPFailures(hostname, "1", http.StatusTooManyRequests, 1)
	handler := simhttp.NewHandlerWithOptions(w, "http://sim.invalid", simhttp.HandlerOptions{Faults: faults})

	rw := topologyRequest(t, handler, "sim.invalid", "/xrpc/com.atproto.sync.listHosts?limit=1000")
	require.Equal(t, http.StatusServiceUnavailable, rw.Code)
	rw = topologyRequest(t, handler, "sim.invalid", "/xrpc/com.atproto.sync.listHosts?limit=1000")
	require.Equal(t, http.StatusOK, rw.Code)
	var roster struct {
		Hosts []struct {
			Hostname string `json:"hostname"`
			Status   string `json:"status"`
		} `json:"hosts"`
	}
	require.NoError(t, json.Unmarshal(rw.Body.Bytes(), &roster))
	require.Equal(t, "offline", roster.Hosts[0].Status)
	require.Equal(t, 1, faults.ListHostsHTTPFailuresFired(""))

	rw = topologyRequest(t, handler, hostname, "/xrpc/com.atproto.sync.listRepos?limit=1")
	require.Equal(t, http.StatusOK, rw.Code)
	var first topologyListReposOutput
	require.NoError(t, json.Unmarshal(rw.Body.Bytes(), &first))
	require.Len(t, first.Repos, 1)
	rw = topologyRequest(t, handler, hostname, "/xrpc/com.atproto.sync.listRepos?limit=1&cursor=1")
	require.Equal(t, http.StatusTooManyRequests, rw.Code)
	require.Equal(t, "0", rw.Header().Get("RateLimit-Remaining"))
	rw = topologyRequest(t, handler, hostname, "/xrpc/com.atproto.sync.listRepos?limit=1&cursor=1")
	require.Equal(t, http.StatusOK, rw.Code, "host must recover after its scheduled mid-enumeration death")
	require.Equal(t, 1, faults.PDSListReposHTTPFailuresFired(hostname, "1"))
}

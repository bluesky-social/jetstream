package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	stdsync "sync"
	"sync/atomic"
	"testing"
	"time"

	simhttp "github.com/bluesky-social/jetstream/internal/simulator/http"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/jcalabro/jttp"
	"github.com/stretchr/testify/require"
)

func TestPDS_GetRepoRoundTrips(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 2)
	srv := httptest.NewServer(simhttp.NewHandler(w, "http://example.test"))
	defer srv.Close()

	a, err := w.LoadAccount(0)
	require.NoError(t, err)

	xc := &xrpc.Client{
		Host:       srv.URL,
		HTTPClient: gt.Some(jttp.New()),
	}
	sc := sync.NewClient(sync.Options{Client: xc})

	body, err := sc.GetRepoStream(context.Background(), a.DID, "")
	require.NoError(t, err)
	defer func() { _ = body.Close() }()

	// LoadFromCAR validates the CAR's structural integrity and the
	// commit decodes correctly. Signature validation against the
	// PLC-published key is exercised in Task 14's listRepos test.
	rp, commit, err := loadFromCAR(body)
	require.NoError(t, err)
	require.Equal(t, a.DID, rp.DID)
	require.NotEmpty(t, commit.Sig)
}

// TestPDS_GetRepoServedHookFiresOncePerServe pins the OnGetRepoServed
// timing signal: it fires exactly once per successful getRepo, carrying
// the served DID, AFTER the CAR body is written. It must NOT fire on the
// not-found path (no snapshot served).
func TestPDS_GetRepoServedHookFiresOncePerServe(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 2)
	a0, err := w.LoadAccount(0)
	require.NoError(t, err)
	a1, err := w.LoadAccount(1)
	require.NoError(t, err)

	var served []string
	var mu stdsync.Mutex
	srv := httptest.NewServer(simhttp.NewHandlerWithOptions(w, "http://example.test", simhttp.HandlerOptions{
		OnGetRepoServed: func(did string) {
			mu.Lock()
			served = append(served, did)
			mu.Unlock()
		},
	}))
	defer srv.Close()

	xc := &xrpc.Client{Host: srv.URL, HTTPClient: gt.Some(jttp.New())}
	sc := sync.NewClient(sync.Options{Client: xc})

	for _, did := range []atmos.DID{a0.DID, a1.DID, a0.DID} {
		body, err := sc.GetRepoStream(context.Background(), did, "")
		require.NoError(t, err)
		_, err = io.Copy(io.Discard, body)
		require.NoError(t, err)
		require.NoError(t, body.Close())
	}

	mu.Lock()
	got := append([]string(nil), served...)
	mu.Unlock()
	require.Equal(t, []string{string(a0.DID), string(a1.DID), string(a0.DID)}, got,
		"hook fires once per successful getRepo, in order, carrying the served DID")
}

// TestPDS_GetRepoServedHookDoesNotFireWhenNotFound pins that the timing
// signal is tied to a real snapshot: an unknown DID 404s and the hook
// must stay silent.
func TestPDS_GetRepoServedHookDoesNotFireWhenNotFound(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 2, 1)

	var fired atomic.Int64
	srv := httptest.NewServer(simhttp.NewHandlerWithOptions(w, "http://example.test", simhttp.HandlerOptions{
		OnGetRepoServed: func(string) { fired.Add(1) },
	}))
	defer srv.Close()

	url := srv.URL + "/xrpc/com.atproto.sync.getRepo?did=" + "did:plc:doesnotexist0000000000000"
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.Equal(t, int64(0), fired.Load(), "hook must not fire when no snapshot is served")
}

// TestPDS_GetRepoFaultHandlerServesTransient503ThenCAR pins the
// SIMULATOR HANDLER's fault-injection mechanic: a scheduled getRepo fault
// returns the configured 503 (and increments the fired counter) on the
// first request, then serves the real CAR once the budget is exhausted.
// It deliberately disables client retries (MaxAttempts=1) and drives
// GetRepoStream by hand so it isolates the handler contract — it does NOT
// exercise jetstream's backfill retry loop. That end-to-end recovery is
// covered by backfill.TestRun_TransientGetRepoFailureThenRecovers and by
// the swarm-mode oracle lifecycle test.
func TestPDS_GetRepoFaultHandlerServesTransient503ThenCAR(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 2)
	a, err := w.LoadAccount(0)
	require.NoError(t, err)

	faults := simhttp.NewFaultPlan()
	faults.AddGetRepoHTTPFailures(string(a.DID), http.StatusServiceUnavailable, 1)

	srv := httptest.NewServer(simhttp.NewHandlerWithOptions(w, "http://example.test", simhttp.HandlerOptions{
		Faults: faults,
	}))
	defer srv.Close()

	xc := &xrpc.Client{
		Host:       srv.URL,
		HTTPClient: gt.Some(http.DefaultClient),
		Retry:      gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
	}
	sc := sync.NewClient(sync.Options{Client: xc})

	body, err := sc.GetRepoStream(context.Background(), a.DID, "")
	require.Error(t, err)
	require.Nil(t, body)
	require.Equal(t, 1, faults.GetRepoHTTPFailuresFired(string(a.DID)))

	body, err = sc.GetRepoStream(context.Background(), a.DID, "")
	require.NoError(t, err)
	defer func() { _ = body.Close() }()

	rp, commit, err := loadFromCAR(body)
	require.NoError(t, err)
	require.Equal(t, a.DID, rp.DID)
	require.NotEmpty(t, commit.Sig)
	require.Equal(t, 1, faults.GetRepoHTTPFailuresFired(string(a.DID)))
}

func TestPDS_GetRepoResponseFaultServesXRPCBodyAndRateLimitHeaders(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 2)
	a, err := w.LoadAccount(0)
	require.NoError(t, err)

	reset := time.Now().UTC().Add(10 * time.Minute).Unix()
	faults := simhttp.NewFaultPlan()
	faults.AddGetRepoResponseFault(string(a.DID), simhttp.GetRepoResponseFault{
		Status:  http.StatusTooManyRequests,
		Error:   "RateLimitExceeded",
		Message: "slow down",
		Headers: map[string]string{
			"RateLimit-Limit":     "100",
			"RateLimit-Remaining": "0",
			"RateLimit-Reset":     fmt.Sprintf("%d", reset),
		},
	}, 1)

	srv := httptest.NewServer(simhttp.NewHandlerWithOptions(w, "http://example.test", simhttp.HandlerOptions{
		Faults: faults,
	}))
	defer srv.Close()

	xc := &xrpc.Client{
		Host:       srv.URL,
		HTTPClient: gt.Some(http.DefaultClient),
		Retry:      gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
	}
	sc := sync.NewClient(sync.Options{Client: xc})

	body, err := sc.GetRepoStream(context.Background(), a.DID, "")
	require.Error(t, err)
	require.Nil(t, body)
	var xerr *xrpc.Error
	require.ErrorAs(t, err, &xerr)
	require.Equal(t, http.StatusTooManyRequests, xerr.StatusCode)
	require.Equal(t, "RateLimitExceeded", xerr.Name)
	require.Equal(t, "slow down", xerr.Message)
	require.NotNil(t, xerr.RateLimit)
	require.Equal(t, time.Unix(reset, 0), xerr.RateLimit.Reset)
	require.Equal(t, 1, faults.GetRepoResponseFaultsFired(string(a.DID)))
}

func TestPDS_GetRepoResponseFaultCanRedirect(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 2)
	a, err := w.LoadAccount(0)
	require.NoError(t, err)

	faults := simhttp.NewFaultPlan()
	faults.AddGetRepoResponseFault(string(a.DID), simhttp.GetRepoResponseFault{
		Status:           http.StatusFound,
		RedirectLocation: "https://pds.example.test/xrpc/com.atproto.sync.getRepo?did=" + string(a.DID),
	}, 1)

	srv := httptest.NewServer(simhttp.NewHandlerWithOptions(w, "http://example.test", simhttp.HandlerOptions{
		Faults: faults,
	}))
	defer srv.Close()

	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		srv.URL+"/xrpc/com.atproto.sync.getRepo?did="+string(a.DID), nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusFound, resp.StatusCode)
	require.Equal(t, "https://pds.example.test/xrpc/com.atproto.sync.getRepo?did="+string(a.DID), resp.Header.Get("Location"))
	require.Equal(t, 1, faults.GetRepoResponseFaultsFired(string(a.DID)))
}

// TestPDS_GetRepoFaultHandlerServesTruncatedCARThenCAR pins the simulator
// handler's mid-body fault injection: the first matching request returns
// a 200 response whose CAR body is incomplete, and the next request serves
// the real CAR. The first request deliberately goes through raw HTTP instead
// of sync.GetRepoStream so the assertion is at the response-body boundary.
func TestPDS_GetRepoFaultHandlerServesTruncatedCARThenCAR(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 2)
	a, err := w.LoadAccount(0)
	require.NoError(t, err)

	faults := simhttp.NewFaultPlan()
	faults.AddGetRepoCARTruncations(string(a.DID), 1)

	srv := httptest.NewServer(simhttp.NewHandlerWithOptions(w, "http://example.test", simhttp.HandlerOptions{
		Faults: faults,
	}))
	defer srv.Close()

	url := srv.URL + "/xrpc/com.atproto.sync.getRepo?did=" + string(a.DID)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/vnd.ipld.car", resp.Header.Get("Content-Type"))
	_, _, err = loadFromCAR(resp.Body)
	require.Error(t, err, "first response must be an incomplete CAR")
	require.Equal(t, 1, faults.GetRepoCARTruncationsFired(string(a.DID)))

	req, err = http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	rp, commit, err := loadFromCAR(bytes.NewReader(body))
	require.NoError(t, err)
	require.Equal(t, a.DID, rp.DID)
	require.NotEmpty(t, commit.Sig)
	require.Equal(t, 1, faults.GetRepoCARTruncationsFired(string(a.DID)))
}

func TestPDS_GetRepoUnavailableReturnsXRPCError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		status string
		name   string
	}{
		{status: "takendown", name: "RepoTakendown"},
		{status: "suspended", name: "RepoSuspended"},
		{status: "deactivated", name: "RepoDeactivated"},
	} {
		t.Run(tc.status, func(t *testing.T) {
			t.Parallel()
			w := newTestWorld(t, 5, 2)
			a, err := w.LoadAccount(0)
			require.NoError(t, err)
			require.NoError(t, w.SetRepoUnavailableForTest(0, tc.status))

			srv := httptest.NewServer(simhttp.NewHandler(w, "http://example.test"))
			defer srv.Close()

			xc := &xrpc.Client{
				Host:       srv.URL,
				HTTPClient: gt.Some(http.DefaultClient),
				Retry:      gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
			}
			sc := sync.NewClient(sync.Options{Client: xc})

			body, err := sc.GetRepoStream(context.Background(), a.DID, "")
			require.Nil(t, body)
			var xerr *xrpc.Error
			require.True(t, errors.As(err, &xerr), "getRepo unavailable must parse as *xrpc.Error")
			require.Equal(t, http.StatusBadRequest, xerr.StatusCode)
			require.Equal(t, tc.name, xerr.Name)

			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
				srv.URL+"/xrpc/com.atproto.sync.getRepo?did="+string(a.DID), nil)
			require.NoError(t, err)
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			require.Equal(t, "application/json", resp.Header.Get("Content-Type"))
			var envelope struct {
				Error   string `json:"error"`
				Message string `json:"message"`
			}
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&envelope))
			require.Equal(t, tc.name, envelope.Error)
			require.Contains(t, envelope.Message, tc.status)
		})
	}
}

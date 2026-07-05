package http_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	simhttp "github.com/bluesky-social/jetstream/internal/simulator/http"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/jcalabro/jttp"
	"github.com/stretchr/testify/require"
)

func TestListRepos_PagesAcrossAllAccounts(t *testing.T) {
	t.Parallel()
	const total = 25
	w := newTestWorld(t, total, 1)
	srv := httptest.NewServer(simhttp.NewHandler(w, "")) // pds endpoint not needed here
	defer srv.Close()

	xc := &xrpc.Client{Host: srv.URL, HTTPClient: gt.Some(jttp.New())}
	sc := sync.NewClient(sync.Options{Client: xc})

	seen := make(map[atmos.DID]bool)
	for page, err := range sc.ListRepos(context.Background(), 10, "") {
		require.NoError(t, err)
		for _, e := range page.Entries {
			require.False(t, seen[e.DID], "duplicate DID %s", e.DID)
			seen[e.DID] = true
		}
	}
	require.Equal(t, total, len(seen))
}

func TestListReposFaults_ModelDuplicateLoopAndShrinkPages(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 25, 1)
	faults := simhttp.NewFaultPlan()
	faults.AddListReposFault("10", simhttp.ListReposFaultDuplicatePreviousPage, 1)
	faults.AddListReposFault("20", simhttp.ListReposFaultCursorLoop, 1)
	faults.AddListReposFault("0", simhttp.ListReposFaultShrinkPage, 1)

	srv := httptest.NewServer(simhttp.NewHandlerWithOptions(w, "", simhttp.HandlerOptions{
		Faults: faults,
	}))
	defer srv.Close()

	first := getListReposRaw(t, srv.URL, "0", 10)
	require.Len(t, first.Repos, 5, "shrink-page fault halves the requested page")
	require.Equal(t, "5", first.Cursor)
	require.Equal(t, 1, faults.ListReposFaultsFired("0"))

	page10 := getListReposRaw(t, srv.URL, "10", 10)
	require.Len(t, page10.Repos, 10)
	require.Equal(t, "20", page10.Cursor, "duplicate-previous-page still advances the requested cursor")
	require.Equal(t, 1, faults.ListReposFaultsFired("10"))

	page20 := getListReposRaw(t, srv.URL, "20", 10)
	require.Len(t, page20.Repos, 5)
	require.Equal(t, "20", page20.Cursor, "cursor-loop fault repeats the cursor after serving the page")
	require.Equal(t, 1, faults.ListReposFaultsFired("20"))
}

func TestListRepos_GetRepoVerifiesCommitSignature(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 1)

	// Two-step server setup so we can pass srv.URL into the handler
	// (the handler advertises srv.URL as the PDS endpoint in DID
	// documents).
	srv := httptest.NewServer(nil)
	srv.Config.Handler = simhttp.NewHandler(w, srv.URL)
	defer srv.Close()

	// Both clients here use http.DefaultClient because:
	//   - identity.DefaultResolver's default httpClient enables
	//     WithStrictSSRFProtection() which blocks loopback even on
	//     the initial request.
	//   - We're talking to httptest.Server on 127.0.0.1.
	xc := &xrpc.Client{Host: srv.URL, HTTPClient: gt.Some(http.DefaultClient)}
	directory := &identity.Directory{
		Resolver: &identity.DefaultResolver{
			HTTPClient: gt.Some(http.DefaultClient),
			PLCURL:     gt.Some(srv.URL),
		},
		SkipHandleVerification: true,
	}
	sc := sync.NewClient(sync.Options{
		Client:    xc,
		Directory: gt.Some(directory),
	})

	a, _ := w.LoadAccount(2)
	body, err := sc.GetRepoStream(context.Background(), a.DID, "")
	require.NoError(t, err)
	defer func() { _ = body.Close() }()

	rp, commit, err := loadFromCAR(body)
	require.NoError(t, err)

	// Verify the commit signature against the pubkey we publish via
	// PLC. This is the strongest signal that the simulator's
	// keys+DID+CAR pipeline is internally consistent.
	id, err := directory.LookupDID(context.Background(), rp.DID)
	require.NoError(t, err)
	pub, err := id.PublicKey()
	require.NoError(t, err)
	require.NoError(t, commit.VerifySignature(pub))
}

type rawListReposPage struct {
	Cursor string `json:"cursor,omitempty"`
	Repos  []struct {
		DID    string `json:"did"`
		Head   string `json:"head"`
		Rev    string `json:"rev"`
		Active bool   `json:"active"`
	} `json:"repos"`
}

func getListReposRaw(t *testing.T, baseURL, cursor string, limit int) rawListReposPage {
	t.Helper()
	u, err := url.Parse(baseURL + "/xrpc/com.atproto.sync.listRepos")
	require.NoError(t, err)
	q := u.Query()
	q.Set("limit", fmt.Sprintf("%d", limit))
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, u.String(), nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out rawListReposPage
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out
}

package http_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	simhttp "github.com/bluesky-social/jetstream/internal/simulator/http"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// TestAdversarialOpCommit_PassesRealVerifier is the load-bearing pin
// for the #204 oracle tier: atmos's REAL Sync-1.1 verifier must accept
// a commit carrying a spec-invalid op path (the verifier checks MST
// consistency, not spec validity) and yield the lie to the consumer,
// where jetstream's ingest gate is the first spec check. If an atmos
// upgrade starts validating op paths in the verifier, this test fails
// before the oracle lifecycle does, pointing straight at the drift.
func TestAdversarialOpCommit_PassesRealVerifier(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 1)

	srv := httptest.NewServer(nil)
	srv.Config.Handler = simhttp.NewHandler(w, srv.URL)
	defer srv.Close()

	directory := &identity.Directory{
		Resolver: &identity.DefaultResolver{
			HTTPClient: gt.Some(http.DefaultClient),
			PLCURL:     gt.Some(srv.URL),
		},
		SkipHandleVerification: true,
	}
	verifier, err := sync.NewVerifier(sync.VerifierOptions{
		Directory:  directory,
		StateStore: sync.NewMemStateStore(),
		// PolicyError so a structural rejection surfaces as an error
		// instead of a silent resync enqueue — the assertion below
		// needs rejection vs acceptance to be unambiguous.
		Policy: gt.Some(sync.PolicyError),
		SyncClient: gt.Some(sync.NewClient(sync.Options{
			Client:    &xrpc.Client{Host: srv.URL, HTTPClient: gt.Some(http.DefaultClient)},
			Directory: gt.Some(directory),
		})),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = verifier.Close() })

	const badKey = "app.bsky.feed.post/.."
	sibling, err := w.GenerateAdversarialOpForTest(context.Background(), 2, badKey, "invalid_rkey")
	require.NoError(t, err)

	frames, err := w.FirehoseRange(0, 1)
	require.NoError(t, err)
	require.Len(t, frames, 1)
	body, ok := bytes.CutPrefix(frames[0], frameHeaderCommitBytes)
	require.True(t, ok, "expected #commit header")
	var cm comatproto.SyncSubscribeRepos_Commit
	require.NoError(t, cm.UnmarshalCBOR(body))

	ops, err := verifier.VerifyCommit(context.Background(), &cm)
	require.NoError(t, err, "the verifier must accept the adversarial commit (MST-consistent lie)")
	require.Len(t, ops, 2, "both the sibling and the lie must be yielded to the consumer")

	var sawSibling, sawLie bool
	for _, op := range ops {
		path := string(op.Collection) + "/" + string(op.RKey)
		switch path {
		case sibling.Collection + "/" + sibling.Rkey:
			sawSibling = true
		case badKey:
			sawLie = true
			require.Error(t, op.RKey.Validate(),
				"the lie must still be spec-invalid after the verifier — jetstream's gate is the first spec check")
		}
	}
	require.True(t, sawSibling, "benign sibling op must be yielded")
	require.True(t, sawLie, "adversarial op must be yielded, not filtered, by the verifier")
}

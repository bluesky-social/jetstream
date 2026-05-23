package syncstate

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/jcalabro/atmos"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/require"
)

// TestStateStore_Swarm pins observational equivalence to an in-memory
// reference implementation across a randomized op stream. The reference
// is sync.MemStateStore — the in-memory map atmos itself uses for tests.
// If our pebble shape diverges (e.g. drops a Save under load, or fails
// to round-trip a particular field combination), the test will catch it.
func TestStateStore_Swarm(t *testing.T) {
	t.Parallel()
	iters := 500
	if testing.Short() {
		iters = 100
	}

	pebbleStore := New(newTestStore(t))
	refStore := atmossync.NewMemStateStore()

	r := rand.New(rand.NewPCG(0xCAFE, 0xBEEF))

	dids := make([]atmos.DID, 8)
	for i := range dids {
		body := fmt.Sprintf("aaaaaaaaaaaaaaaaaaaaaaa%d", i)
		body = body[:24]
		dids[i] = parseDID(t, "did:plc:"+body)
	}
	cid := fixedCID(t)

	for range iters {
		d := dids[r.IntN(len(dids))]
		switch r.IntN(5) {
		case 0:
			cs := atmossync.ChainState{Rev: fmt.Sprintf("rev-%d", r.IntN(1000)), Data: cid}
			require.NoError(t, pebbleStore.SaveChain(t.Context(), d, cs))
			require.NoError(t, refStore.SaveChain(t.Context(), d, cs))
		case 1:
			hs := atmossync.HostingState{
				Active: r.IntN(2) == 0,
				Status: []string{"", "takendown", "suspended", "deactivated"}[r.IntN(4)],
				Seq:    int64(r.IntN(1_000_000)),
				Time:   "2026-05-21T00:00:00Z",
			}
			require.NoError(t, pebbleStore.SaveHosting(t.Context(), d, hs))
			require.NoError(t, refStore.SaveHosting(t.Context(), d, hs))
		case 2:
			require.NoError(t, pebbleStore.Delete(t.Context(), d))
			require.NoError(t, refStore.Delete(t.Context(), d))
		case 3:
			got, err := pebbleStore.LoadChain(t.Context(), d)
			require.NoError(t, err)
			want, err := refStore.LoadChain(t.Context(), d)
			require.NoError(t, err)
			require.Equal(t, want == nil, got == nil, "did=%s chain presence mismatch", d)
			if want != nil {
				require.Equal(t, want.Rev, got.Rev, "did=%s rev mismatch", d)
				require.True(t, want.Data.Equal(got.Data), "did=%s data CID mismatch", d)
			}
		case 4:
			got, err := pebbleStore.LoadHosting(t.Context(), d)
			require.NoError(t, err)
			want, err := refStore.LoadHosting(t.Context(), d)
			require.NoError(t, err)
			require.Equal(t, want == nil, got == nil, "did=%s hosting presence mismatch", d)
			if want != nil {
				require.Equal(t, *want, *got, "did=%s hosting mismatch", d)
			}
		}
	}
}

package live

import (
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/ingest/syncstate"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/segment"
	atmoscbor "github.com/jcalabro/atmos/cbor"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/require"
)

func fixedCIDForPromote(t *testing.T) atmoscbor.CID {
	t.Helper()
	cid, err := atmoscbor.ParseCIDString("bafyreigwexhqswvbgxqe5w7tnbcc7g5oh54oas5jewopl5jpcsjp3lk7vy")
	require.NoError(t, err)
	return cid
}

// TestPromoteSyncState pins the §2.2 two-phase contract at the
// consumer boundary: chain state staged by the verifier becomes
// flushable only once the full row group of the event that produced
// it has been appended, gated by rev so a later pipelined event's
// state stays pending.
func TestPromoteSyncState(t *testing.T) {
	t.Parallel()
	raw, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	ss := syncstate.New(raw)

	did := "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	atmosDID := atmosDIDFromString(t, did)
	require.NoError(t, ss.SaveChain(t.Context(), atmosDID, atmossync.ChainState{Rev: "3lrev2", Data: fixedCIDForPromote(t)}))

	c := &Consumer{cfg: Config{SyncStateStore: ss}}

	// A group for an EARLIER rev must not promote the newer pending state.
	c.promoteSyncState([]segment.Event{
		{Kind: segment.KindCreate, DID: did, Rev: "3lrev1"},
	})
	require.NoError(t, ss.Flush())
	fresh := syncstate.New(raw)
	got, err := fresh.LoadChain(t.Context(), atmosDID)
	require.NoError(t, err)
	require.Nil(t, got, "newer-rev pending chain state must not flush on an older group")

	// The producing event's group (sync row + replacement creates,
	// all rev 3lrev2) promotes it.
	c.promoteSyncState([]segment.Event{
		{Kind: segment.KindSync, DID: did, Rev: "3lrev2"},
		{Kind: segment.KindCreate, DID: did, Rev: "3lrev2"},
	})
	require.NoError(t, ss.Flush())
	got, err = fresh.LoadChain(t.Context(), atmosDID)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, "3lrev2", got.Rev)
}

func TestPromoteSyncStateHostingOnAccountRow(t *testing.T) {
	t.Parallel()
	raw, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	ss := syncstate.New(raw)

	did := "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"
	atmosDID := atmosDIDFromString(t, did)
	require.NoError(t, ss.SaveHosting(t.Context(), atmosDID, atmossync.HostingState{Active: false, Status: "takendown", Seq: 9}))

	c := &Consumer{cfg: Config{SyncStateStore: ss}}

	// A commit group does not promote hosting state, and neither does
	// a REDELIVERED account row with an older upstream seq.
	c.promoteSyncState([]segment.Event{{Kind: segment.KindCreate, DID: did, Rev: "3lrev1"}})
	c.promoteSyncState([]segment.Event{{Kind: segment.KindAccount, DID: did, UpstreamRelayCursor: 8}})
	require.NoError(t, ss.Flush())
	fresh := syncstate.New(raw)
	got, err := fresh.LoadHosting(t.Context(), atmosDID)
	require.NoError(t, err)
	require.Nil(t, got)

	// The account event's own row group does.
	c.promoteSyncState([]segment.Event{{Kind: segment.KindAccount, DID: did, UpstreamRelayCursor: 9}})
	require.NoError(t, ss.Flush())
	got, err = fresh.LoadHosting(t.Context(), atmosDID)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, "takendown", got.Status)
}

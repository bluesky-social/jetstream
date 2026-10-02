package live

import (
	"testing"

	"github.com/bluesky-social/jetstream/internal/ingest/syncstate"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/metastore/memstore"
	"github.com/bluesky-social/jetstream/segment"
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

// flushSyncState commits promoted state on its own: the durable cursor batch
// minus the cursor.
func flushSyncState(t *testing.T, s metastore.Store, ss *syncstate.StateStore) {
	t.Helper()
	b := s.NewBatch()
	ss.StageFlush(b)
	require.NoError(t, b.Commit(t.Context()))
	ss.CommitStaged()
}

// TestPromoteSyncState pins the §2.2 two-phase contract at the
// consumer boundary: chain state staged by the verifier becomes
// flushable only once the full row group of the event that produced
// it has been appended, gated by rev so a later pipelined event's
// state stays pending.
func TestPromoteSyncState(t *testing.T) {
	t.Parallel()
	raw := memstore.New()
	ss := syncstate.New(raw)

	did := "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	atmosDID := atmosDIDFromString(t, did)
	require.NoError(t, ss.SaveChain(t.Context(), atmosDID, atmossync.ChainState{Rev: "3lrev2", Data: fixedCIDForPromote(t)}))

	c := &Consumer{cfg: Config{SyncStateStore: ss}}

	// A group for an EARLIER rev must not promote the newer pending state.
	c.promoteSyncState([]segment.Event{
		{Kind: segment.KindCreate, DID: did, Rev: "3lrev1"},
	})
	flushSyncState(t, raw, ss)
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
	flushSyncState(t, raw, ss)
	got, err = fresh.LoadChain(t.Context(), atmosDID)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, "3lrev2", got.Rev)
}

func TestPromoteSyncStateHostingOnAccountRow(t *testing.T) {
	t.Parallel()
	raw := memstore.New()
	ss := syncstate.New(raw)

	did := "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"
	atmosDID := atmosDIDFromString(t, did)
	require.NoError(t, ss.SaveHosting(t.Context(), atmosDID, atmossync.HostingState{Active: false, Status: "takendown", Seq: 9}))

	c := &Consumer{cfg: Config{SyncStateStore: ss}}

	// A commit group does not promote hosting state, and neither does
	// a REDELIVERED account row with an older upstream seq.
	c.promoteSyncState([]segment.Event{{Kind: segment.KindCreate, DID: did, Rev: "3lrev1"}})
	c.promoteSyncState([]segment.Event{{Kind: segment.KindAccount, DID: did, UpstreamRelayCursor: 8}})
	flushSyncState(t, raw, ss)
	fresh := syncstate.New(raw)
	got, err := fresh.LoadHosting(t.Context(), atmosDID)
	require.NoError(t, err)
	require.Nil(t, got)

	// The account event's own row group does.
	c.promoteSyncState([]segment.Event{{Kind: segment.KindAccount, DID: did, UpstreamRelayCursor: 9}})
	flushSyncState(t, raw, ss)
	got, err = fresh.LoadHosting(t.Context(), atmosDID)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, "takendown", got.Status)
}

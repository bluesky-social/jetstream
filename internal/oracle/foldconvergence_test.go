package oracle

import (
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// TestFoldConvergence_DeletedRecordConverges: a create then a delete in the
// emitted stream folds out — the folding consumer converges to "deleted", which
// matches ground truth (also folded from the same complete stream). The
// at-least-once create row is expected and not an error.
func TestFoldConvergence_DeletedRecordConverges(t *testing.T) {
	t.Parallel()
	full := []ObservedEvent{
		{Seq: 50, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r"},
		{Seq: 120, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r"},
	}
	// Unfiltered: the client emits the full stream; both sides fold to empty.
	require.NoError(t, CheckFoldConvergence(full, full, nil))
}

// TestFoldConvergence_ReactivationKeepsNewerRecord: an account deleted at seq
// 100 then re-creates a record at seq 200. The DID-level tombstone is a
// half-open seq window, not a permanent mask: the pre-deletion record folds out
// (kill 100 >= create 60) while the post-deletion record survives (create
// 200 > kill 100). Both sides see the full stream, so they converge.
func TestFoldConvergence_ReactivationKeepsNewerRecord(t *testing.T) {
	t.Parallel()
	full := []ObservedEvent{
		{Seq: 60, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "old"},
		{Seq: 100, Kind: segment.KindAccount, DID: "did:plc:a", Payload: oracleAccountPayload(t, false, "deleted")},
		{Seq: 200, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "new"},
	}
	require.NoError(t, CheckFoldConvergence(full, full, nil),
		"reactivated account's newer record must survive the fold")
}

// TestFoldConvergence_CollectionRestrictionAfterFold proves the OUTPUT
// restriction is applied AFTER the fold, so a DID-level killer (empty
// collection) still purges a collection-filtered record. The client emitted only
// the in-scope collection plus the (empty-collection) account-delete that
// bypasses the collection filter (§R3). The check restricts output to
// collection "c" and both sides agree the record is dead.
func TestFoldConvergence_CollectionRestrictionAfterFold(t *testing.T) {
	t.Parallel()
	full := []ObservedEvent{
		{Seq: 10, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r"},
		{Seq: 20, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "other", Rkey: "x"},
		{Seq: 30, Kind: segment.KindAccount, DID: "did:plc:a", Payload: oracleAccountPayload(t, false, "deleted")},
	}
	// Client filtered to collection "c": it delivers the "c" create and the
	// account-delete (DID-level events bypass the collection filter), but not
	// the "other" create.
	emitted := []ObservedEvent{
		{Seq: 10, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r"},
		{Seq: 30, Kind: segment.KindAccount, DID: "did:plc:a", Payload: oracleAccountPayload(t, false, "deleted")},
	}
	require.NoError(t, CheckFoldConvergence(emitted, full, []string{"c"}),
		"DID-level killer must purge a collection-filtered record after the fold")
}

// TestFoldConvergence_MissingDIDKillerDiverges is the checker's OWN gate: it
// proves CheckFoldConvergence detects the §R3 DID-tombstone gap. A
// collection-filtered client downloads the in-scope create but NEVER receives
// the (empty-collection) account-delete that kills it — exactly the gap the
// step-3 start-snapshot closes. The emitted stream folds to "record present"
// while ground truth (which sees the killer) folds to "deleted", so the check
// must report divergence. This is the synthetic stand-in for the end-to-end
// gate test; it passes NOW because it asserts the checker reports the bug.
func TestFoldConvergence_MissingDIDKillerDiverges(t *testing.T) {
	t.Parallel()
	full := []ObservedEvent{
		{Seq: 10, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r"},
		{Seq: 30, Kind: segment.KindAccount, DID: "did:plc:a", Payload: oracleAccountPayload(t, false, "deleted")},
	}
	// The filtered client downloaded the create but the empty-collection
	// account-delete rode in no collection block and sat below the tip, so it
	// was never delivered — the gap.
	emitted := []ObservedEvent{
		{Seq: 10, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r"},
	}
	err := CheckFoldConvergence(emitted, full, []string{"c"})
	require.Error(t, err, "a filtered stream missing the DID-level killer must diverge")
	require.Contains(t, err.Error(), "ground truth DELETED")
}

// TestFoldConvergence_StaleVersionDiverges: the client folds to an OLDER
// version than ground truth (it missed the update). A folding consumer cannot
// converge, so the check must report a stale version.
func TestFoldConvergence_StaleVersionDiverges(t *testing.T) {
	t.Parallel()
	full := []ObservedEvent{
		{Seq: 10, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r"},
		{Seq: 20, Kind: segment.KindUpdate, DID: "did:plc:a", Collection: "c", Rkey: "r"},
	}
	emitted := []ObservedEvent{
		{Seq: 10, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r"},
	}
	err := CheckFoldConvergence(emitted, full, []string{"c"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "STALE")
}

// TestFoldConvergence_SeamBoundaries exercises two DIDs whose deletes straddle a
// would-be overlay seam (one at 150, one at 151). Under the relaxed contract the
// seam is irrelevant: both deletes are in the emitted stream, so both records
// fold out and converge.
func TestFoldConvergence_SeamBoundaries(t *testing.T) {
	t.Parallel()
	full := []ObservedEvent{
		{Seq: 10, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r1"},
		{Seq: 150, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r1"},
		{Seq: 80, Kind: segment.KindCreate, DID: "did:plc:b", Collection: "c", Rkey: "r2"},
		{Seq: 151, Kind: segment.KindDelete, DID: "did:plc:b", Collection: "c", Rkey: "r2"},
	}
	require.NoError(t, CheckFoldConvergence(full, full, nil))
}

// TestFoldConvergence_WildcardCollectionRestriction guards that the output
// restriction honors the same ".*" namespace-wildcard semantics the client
// Matcher uses, so the oracle and the client agree on which records are in
// scope.
func TestFoldConvergence_WildcardCollectionRestriction(t *testing.T) {
	t.Parallel()
	full := []ObservedEvent{
		{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "app.bsky.graph.follow", Rkey: "r1"},
		{Seq: 2, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r2"},
	}
	// Client filtered to app.bsky.graph.* delivers only the follow.
	emitted := []ObservedEvent{
		{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "app.bsky.graph.follow", Rkey: "r1"},
	}
	require.NoError(t, CheckFoldConvergence(emitted, full, []string{"app.bsky.graph.*"}),
		"wildcard restriction must select the in-namespace record and exclude the sibling namespace")
}

// TestFoldConvergence_MalformedAccountPayloadFailsLoud guards the oracle's
// fidelity: a corrupt account-delete payload must surface a hard error, not
// fold as deleted=false. Silently dropping a malformed DID tombstone could turn
// a real purge into a no-op and report a false-green convergence — exactly the
// failure the sibling oracle paths (CheckCompacted, Reconstruct) refuse to make.
func TestFoldConvergence_MalformedAccountPayloadFailsLoud(t *testing.T) {
	t.Parallel()
	full := []ObservedEvent{
		{Seq: 10, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r"},
		// Not valid CBOR for SyncSubscribeRepos_Account.
		{Seq: 20, Kind: segment.KindAccount, DID: "did:plc:a", Payload: []byte{0xff, 0x00, 0x13, 0x37}},
	}
	err := CheckFoldConvergence(full, full, nil)
	require.Error(t, err, "a malformed account payload must fail the convergence check, not be silently ignored")
	require.Contains(t, err.Error(), "fold failed")
}

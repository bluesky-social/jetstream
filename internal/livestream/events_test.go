package livestream

import (
	"bytes"
	"testing"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/api/lextypes"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/mst"
	atmosrepo "github.com/jcalabro/atmos/repo"
	"github.com/jcalabro/atmos/streaming"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

const testIndexedAt int64 = 1_700_000_000_000_000

// buildCommit constructs a real #commit event with the given
// (collection, rkey) record creates. Each create writes a tiny CBOR
// map {"v": i}. The returned event is shaped exactly like one
// atmos's streaming decoder would emit.
func buildCommit(t *testing.T, did, rev string, recs ...struct{ Coll, Rkey string }) (streaming.Event, [][]byte) {
	t.Helper()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	mstore := mst.NewMemBlockStore()
	r := &atmosrepo.Repo{
		DID:   atmosDIDFromString(t, did),
		Clock: atmos.NewTIDClock(0),
		Store: mstore,
		Tree:  mst.NewTree(mstore),
	}

	payloads := make([][]byte, 0, len(recs))
	ops := make([]comatproto.SyncSubscribeRepos_RepoOp, 0, len(recs))
	for i, rc := range recs {
		val := map[string]any{"v": i}
		require.NoError(t, r.Create(rc.Coll, rc.Rkey, val))
		// Capture the encoded record bytes from the repo. atmos's
		// Repo.Get returns the CID and the raw block bytes that
		// will land in the CAR — exactly what atmos's streaming
		// decoder will see on the other side.
		cid, blk, err := r.Get(rc.Coll, rc.Rkey)
		require.NoError(t, err)
		payloads = append(payloads, append([]byte(nil), blk...))

		ops = append(ops, comatproto.SyncSubscribeRepos_RepoOp{
			Action: "create",
			Path:   rc.Coll + "/" + rc.Rkey,
			CID:    gt.Some(lextypes.LexCIDLink{Link: cid.String()}),
		})
	}

	var carBuf bytes.Buffer
	require.NoError(t, r.ExportCAR(&carBuf, key))

	return streaming.Event{
		Seq: 42,
		Commit: &comatproto.SyncSubscribeRepos_Commit{
			Repo:   did,
			Rev:    rev,
			Ops:    ops,
			Blocks: carBuf.Bytes(),
		},
	}, payloads
}

// atmosDIDFromString is a tiny helper because atmos.DID is a string
// alias but its constructor enforces some validation.
func atmosDIDFromString(t *testing.T, s string) atmos.DID {
	t.Helper()
	d, err := atmos.ParseDID(s)
	require.NoError(t, err)
	return d
}

func TestConvertEvent_CommitCreate(t *testing.T) {
	t.Parallel()

	did := "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	evt, payloads := buildCommit(t, did, "3l3qo2vutsw2b",
		struct{ Coll, Rkey string }{"app.bsky.feed.post", "rec0"},
		struct{ Coll, Rkey string }{"app.bsky.feed.like", "rec1"},
	)

	got, err := ConvertEvent(evt, testIndexedAt)
	require.NoError(t, err)
	require.Len(t, got, 2)

	for i, want := range []struct {
		coll, rkey string
		payload    []byte
	}{
		{"app.bsky.feed.post", "rec0", payloads[0]},
		{"app.bsky.feed.like", "rec1", payloads[1]},
	} {
		ev := got[i]
		require.Equal(t, segment.KindCreate, ev.Kind)
		require.Equal(t, did, ev.DID)
		require.Equal(t, want.coll, ev.Collection)
		require.Equal(t, want.rkey, ev.Rkey)
		require.Equal(t, "3l3qo2vutsw2b", ev.Rev)
		require.Equal(t, testIndexedAt, ev.IndexedAt)
		require.Equal(t, uint64(0), ev.Seq, "Seq is allocated downstream by ingest.Writer")
		require.Equal(t, want.payload, ev.Payload)
	}
}

func TestConvertEvent_Identity(t *testing.T) {
	t.Parallel()

	id := &comatproto.SyncSubscribeRepos_Identity{
		DID:    "did:plc:bbb",
		Handle: gt.Some("bob.test"),
		Seq:    99,
		Time:   "2026-05-21T00:00:00Z",
	}
	evt := streaming.Event{Seq: 99, Identity: id}

	got, err := ConvertEvent(evt, testIndexedAt)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, segment.KindIdentity, got[0].Kind)
	require.Equal(t, "did:plc:bbb", got[0].DID)
	require.Equal(t, testIndexedAt, got[0].IndexedAt)

	// Round-trip the payload to confirm faithful serialization.
	var roundTrip comatproto.SyncSubscribeRepos_Identity
	require.NoError(t, roundTrip.UnmarshalCBOR(got[0].Payload))
	require.Equal(t, id.DID, roundTrip.DID)
	require.Equal(t, id.Seq, roundTrip.Seq)
	require.Equal(t, id.Time, roundTrip.Time)
	require.True(t, roundTrip.Handle.HasVal())
	require.Equal(t, "bob.test", roundTrip.Handle.Val())
}

func TestConvertEvent_Account(t *testing.T) {
	t.Parallel()

	acc := &comatproto.SyncSubscribeRepos_Account{
		DID:    "did:plc:ccc",
		Active: false,
		Status: gt.Some("takendown"),
		Seq:    100,
		Time:   "2026-05-21T00:00:00Z",
	}
	evt := streaming.Event{Seq: 100, Account: acc}

	got, err := ConvertEvent(evt, testIndexedAt)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, segment.KindAccount, got[0].Kind)
	require.Equal(t, "did:plc:ccc", got[0].DID)

	var roundTrip comatproto.SyncSubscribeRepos_Account
	require.NoError(t, roundTrip.UnmarshalCBOR(got[0].Payload))
	require.Equal(t, acc.DID, roundTrip.DID)
	require.Equal(t, acc.Active, roundTrip.Active)
	require.True(t, roundTrip.Status.HasVal())
	require.Equal(t, "takendown", roundTrip.Status.Val())
}

func TestConvertEvent_Sync(t *testing.T) {
	t.Parallel()

	sync := &comatproto.SyncSubscribeRepos_Sync{
		DID:    "did:plc:ddd",
		Rev:    "rev-xyz",
		Blocks: []byte{0x01, 0x02, 0x03},
		Seq:    101,
		Time:   "2026-05-21T00:00:00Z",
	}
	evt := streaming.Event{Seq: 101, Sync: sync}

	got, err := ConvertEvent(evt, testIndexedAt)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, segment.KindSync, got[0].Kind)
	require.Equal(t, "did:plc:ddd", got[0].DID)
	require.Equal(t, "rev-xyz", got[0].Rev)

	var roundTrip comatproto.SyncSubscribeRepos_Sync
	require.NoError(t, roundTrip.UnmarshalCBOR(got[0].Payload))
	require.Equal(t, sync.DID, roundTrip.DID)
	require.Equal(t, sync.Rev, roundTrip.Rev)
	require.Equal(t, sync.Blocks, roundTrip.Blocks)
}

func TestConvertEvent_InfoEmits_Nothing(t *testing.T) {
	t.Parallel()
	evt := streaming.Event{Info: &comatproto.SyncSubscribeRepos_Info{}}
	got, err := ConvertEvent(evt, testIndexedAt)
	require.NoError(t, err)
	require.Empty(t, got)
}

// TestConvertEvent_EmptyEvent_UnknownKind: an event with no
// recognized field set is reported as ErrUnknownEventKind so the Run
// loop can refuse to advance the upstream cursor past frames a future
// jetstream build might learn to decode. See the cursor-skip branch
// in consumer.processBatch.
func TestConvertEvent_EmptyEvent_UnknownKind(t *testing.T) {
	t.Parallel()
	got, err := ConvertEvent(streaming.Event{Seq: 7}, testIndexedAt)
	require.ErrorIs(t, err, ErrUnknownEventKind)
	require.Nil(t, got)
}

func TestConvertEvent_CommitDelete_PayloadNil(t *testing.T) {
	t.Parallel()

	did := "did:plc:eee"

	// Build a delete commit. We create a record first, then delete it, so
	// the CAR has valid structure but the delete op has no CID.
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	mstore := mst.NewMemBlockStore()
	r := &atmosrepo.Repo{
		DID:   atmosDIDFromString(t, did),
		Clock: atmos.NewTIDClock(0),
		Store: mstore,
		Tree:  mst.NewTree(mstore),
	}

	// Create then delete to generate a valid CAR.
	require.NoError(t, r.Create("app.bsky.feed.post", "rec0", map[string]any{"v": 0}))

	var carBuf bytes.Buffer
	require.NoError(t, r.ExportCAR(&carBuf, key))

	commit := &comatproto.SyncSubscribeRepos_Commit{
		Repo: did,
		Rev:  "rev-del",
		Ops: []comatproto.SyncSubscribeRepos_RepoOp{
			{Action: "delete", Path: "app.bsky.feed.post/rec0", CID: gt.None[lextypes.LexCIDLink]()},
		},
		Blocks: carBuf.Bytes(),
	}

	got, err := ConvertEvent(streaming.Event{Seq: 5, Commit: commit}, testIndexedAt)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, segment.KindDelete, got[0].Kind)
	require.Equal(t, "app.bsky.feed.post", got[0].Collection)
	require.Equal(t, "rec0", got[0].Rkey)
	require.Nil(t, got[0].Payload)
}

// TestConvertEvent_CommitMissingCAR_Errors pins data-integrity
// behavior for the case where a Create/Update op references a CID
// that isn't in the commit's CAR block index. atmos surfaces this
// silently as Operation.BlockData()==nil; convertCommit must refuse
// rather than write a Create with nil payload (PRACTICES.md:
// crashing > silent corruption).
func TestConvertEvent_CommitMissingCAR_Errors(t *testing.T) {
	t.Parallel()

	did := "did:plc:missingcar"

	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	mstore := mst.NewMemBlockStore()
	r := &atmosrepo.Repo{
		DID:   atmosDIDFromString(t, did),
		Clock: atmos.NewTIDClock(0),
		Store: mstore,
		Tree:  mst.NewTree(mstore),
	}
	// Empty repo CAR — no record blocks at all.
	var carBuf bytes.Buffer
	require.NoError(t, r.ExportCAR(&carBuf, key))

	// Op references a CID that is NOT in the CAR.
	commit := &comatproto.SyncSubscribeRepos_Commit{
		Repo: did,
		Rev:  "rev-missing",
		Ops: []comatproto.SyncSubscribeRepos_RepoOp{{
			Action: "create",
			Path:   "app.bsky.feed.post/rec0",
			CID: gt.Some(lextypes.LexCIDLink{
				Link: "bafyreiabsentcidthatisnotinthecarfile00000000000000000",
			}),
		}},
		Blocks: carBuf.Bytes(),
	}

	_, err = ConvertEvent(streaming.Event{Seq: 1, Commit: commit}, testIndexedAt)
	require.Error(t, err, "create op with missing CAR block must surface as an error")
	require.Contains(t, err.Error(), "did:plc:missingcar")
}

// TestConvertEvent_CommitResync pins the post-Sync-1.1 mapping:
// atmos's verifier triggers an async resync after a chain break,
// and the resulting ops arrive with Action=ActionResync. They
// carry the live record bytes; we map them to KindCreate so the
// archive records the post-resync state.
func TestConvertEvent_CommitResync(t *testing.T) {
	t.Parallel()

	did := "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	evt, payloads := buildCommit(t, did, "3l3qo2vutsw2c",
		struct{ Coll, Rkey string }{"app.bsky.feed.post", "rec0"},
	)
	// Mutate the op action from "create" to "resync". This is exactly
	// what atmos's resync worker pool produces after a chain-break
	// resolution.
	evt.Commit.Ops[0].Action = "resync"

	got, err := ConvertEvent(evt, testIndexedAt)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, segment.KindCreate, got[0].Kind, "ActionResync must map to KindCreate")
	require.Equal(t, did, got[0].DID)
	require.Equal(t, "app.bsky.feed.post", got[0].Collection)
	require.Equal(t, "rec0", got[0].Rkey)
	require.Equal(t, "3l3qo2vutsw2c", got[0].Rev)
	require.Equal(t, payloads[0], got[0].Payload, "resync ops carry the live record bytes")
}

func TestConvertEvent_CommitUnknownAction_Errors(t *testing.T) {
	t.Parallel()

	did := "did:plc:fff"

	// Build a minimal valid CAR for the unknown action test.
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	mstore := mst.NewMemBlockStore()
	r := &atmosrepo.Repo{
		DID:   atmosDIDFromString(t, did),
		Clock: atmos.NewTIDClock(0),
		Store: mstore,
		Tree:  mst.NewTree(mstore),
	}

	var carBuf bytes.Buffer
	require.NoError(t, r.ExportCAR(&carBuf, key))

	commit := &comatproto.SyncSubscribeRepos_Commit{
		Repo: did,
		Rev:  "rev-bad",
		Ops: []comatproto.SyncSubscribeRepos_RepoOp{
			{Action: "lol-no", Path: "x.y/r"},
		},
		Blocks: carBuf.Bytes(),
	}

	_, err = ConvertEvent(streaming.Event{Commit: commit}, testIndexedAt)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown commit action")
}

package live

import (
	"bytes"
	"errors"
	"testing"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/api/lextypes"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/mst"
	atmosrepo "github.com/jcalabro/atmos/repo"
	"github.com/jcalabro/atmos/streaming"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

const testWitnessedAt int64 = 1_700_000_000_000_000

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

	got, err := ConvertEvent(evt, testWitnessedAt)
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
		require.Equal(t, testWitnessedAt, ev.WitnessedAt)
		require.Equal(t, uint64(0), ev.Seq, "Seq is allocated downstream by ingest.Writer")
		require.Equal(t, int64(42), ev.UpstreamRelayCursor)
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

	got, err := ConvertEvent(evt, testWitnessedAt)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, segment.KindIdentity, got[0].Kind)
	require.Equal(t, "did:plc:bbb", got[0].DID)
	require.Equal(t, testWitnessedAt, got[0].WitnessedAt)
	require.Equal(t, int64(99), got[0].UpstreamRelayCursor)

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

	got, err := ConvertEvent(evt, testWitnessedAt)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, segment.KindAccount, got[0].Kind)
	require.Equal(t, "did:plc:ccc", got[0].DID)
	require.Equal(t, int64(100), got[0].UpstreamRelayCursor)

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
		Rev:    "3l3qo2vutsw2b",
		Blocks: []byte{0x01, 0x02, 0x03},
		Seq:    101,
		Time:   "2026-05-21T00:00:00Z",
	}
	evt := streaming.Event{Seq: 101, Sync: sync}

	got, err := ConvertEvent(evt, testWitnessedAt)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, segment.KindSync, got[0].Kind)
	require.Equal(t, "did:plc:ddd", got[0].DID)
	require.Equal(t, "3l3qo2vutsw2b", got[0].Rev)
	require.Equal(t, int64(101), got[0].UpstreamRelayCursor)

	var roundTrip comatproto.SyncSubscribeRepos_Sync
	require.NoError(t, roundTrip.UnmarshalCBOR(got[0].Payload))
	require.Equal(t, sync.DID, roundTrip.DID)
	require.Equal(t, sync.Rev, roundTrip.Rev)
	require.Equal(t, sync.Blocks, roundTrip.Blocks)
}

func TestConvertEvent_AsyncResyncEmptyRepoEmitsSyncTombstone(t *testing.T) {
	t.Parallel()

	sync := &comatproto.SyncSubscribeRepos_Sync{
		DID: "did:plc:emptyasyncresync",
		Rev: "3l3qo2vutsw2c",
	}
	evt := streaming.Event{
		Sync:   sync,
		Resync: streaming.ResyncAsync,
	}

	got, err := ConvertEvent(evt, testWitnessedAt)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, segment.KindSync, got[0].Kind)
	require.Equal(t, "did:plc:emptyasyncresync", got[0].DID)
	require.Equal(t, "3l3qo2vutsw2c", got[0].Rev)
	require.Equal(t, int64(0), got[0].UpstreamRelayCursor, "async resync events are synthetic and have no relay seq")

	var roundTrip comatproto.SyncSubscribeRepos_Sync
	require.NoError(t, roundTrip.UnmarshalCBOR(got[0].Payload))
	require.Equal(t, sync.DID, roundTrip.DID)
	require.Equal(t, sync.Rev, roundTrip.Rev)
}

func TestConvertEvent_ResyncWithoutSyncEnvelopeErrors(t *testing.T) {
	t.Parallel()

	got, err := ConvertEvent(streaming.Event{Resync: streaming.ResyncAsync}, testWitnessedAt)
	require.ErrorIs(t, err, ErrUnknownEventKind)
	require.Contains(t, err.Error(), "resync event missing sync envelope")
	require.Nil(t, got)
}

func TestConvertEvent_InfoEmits_Nothing(t *testing.T) {
	t.Parallel()
	evt := streaming.Event{Info: &comatproto.SyncSubscribeRepos_Info{}}
	got, err := ConvertEvent(evt, testWitnessedAt)
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
	got, err := ConvertEvent(streaming.Event{Seq: 7}, testWitnessedAt)
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
		Rev:  "3l3qo2vutsw2d",
		Ops: []comatproto.SyncSubscribeRepos_RepoOp{
			{Action: "delete", Path: "app.bsky.feed.post/rec0", CID: gt.None[lextypes.LexCIDLink]()},
		},
		Blocks: carBuf.Bytes(),
	}

	got, err := ConvertEvent(streaming.Event{Seq: 5, Commit: commit}, testWitnessedAt)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, segment.KindDelete, got[0].Kind)
	require.Equal(t, "app.bsky.feed.post", got[0].Collection)
	require.Equal(t, "rec0", got[0].Rkey)
	require.Equal(t, int64(5), got[0].UpstreamRelayCursor)
	require.Nil(t, got[0].Payload)
}

// TestConvertEvent_CommitMissingCAR_DropsBadOpKeepsRest pins the
// data-tolerance behavior for partial CARs from non-canonical PDSes:
// a create/update op referencing a CID that isn't in the commit's
// CAR block index gets dropped (we will not archive a Create with
// nil payload), but other ops in the same commit are still emitted.
// The drop is surfaced via *DroppedOpsError (carrying
// per-op detail) alongside the surviving events, so the consumer
// can bump a metric, log, and continue. Pre-fix: the whole commit
// errored, the consumer's processBatch returned the error, the
// orchestrator's errgroup tore the process down — a single
// misbehaving PDS DoSed the entire backfill.
func TestConvertEvent_CommitMissingCAR_DropsBadOpKeepsRest(t *testing.T) {
	t.Parallel()

	did := "did:plc:missingcar"

	// Build a commit whose first and third ops have valid CIDs (their
	// blocks are in the CAR), but the middle op references a CID that
	// is NOT in the CAR. Mirrors a partial CAR from a misbehaving PDS.
	evt, payloads := buildCommit(t, did, "3l3qo2vutsw2e",
		struct{ Coll, Rkey string }{"app.bsky.feed.post", "good0"},
		struct{ Coll, Rkey string }{"app.bsky.feed.like", "good1"},
	)
	// Splice an orphan op in between the two valid creates. The CID
	// is computed over data the CAR does not carry, so it parses
	// cleanly (no syntax error) but no block will be found at lookup
	// time — the exact shape of the partial-CAR PDS bug we hit in
	// production with did:web:atpub.social.clipsymphony.com.
	orphanCID := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("not-in-the-car"))
	orphanOp := comatproto.SyncSubscribeRepos_RepoOp{
		Action: "create",
		Path:   "app.bsky.feed.post/orphan",
		CID:    gt.Some(lextypes.LexCIDLink{Link: orphanCID.String()}),
	}
	evt.Commit.Ops = []comatproto.SyncSubscribeRepos_RepoOp{
		evt.Commit.Ops[0], orphanOp, evt.Commit.Ops[1],
	}

	got, err := ConvertEvent(evt, testWitnessedAt)

	// The typed error must be reachable via errors.AsType, carrying
	// per-op detail.
	dme, ok := errors.AsType[*DroppedOpsError](err)
	require.True(t, ok,
		"partial CAR must surface *DroppedOpsError, not abort the commit; got=%v", err)
	require.Len(t, dme.Dropped, 1, "exactly one op should be reported as dropped")
	require.Equal(t, did, dme.Dropped[0].DID)
	require.Equal(t, "app.bsky.feed.post", dme.Dropped[0].Collection)
	require.Equal(t, "orphan", dme.Dropped[0].RKey)
	require.Equal(t, "create", dme.Dropped[0].Action)
	require.Equal(t, orphanCID.String(), dme.Dropped[0].CID)

	// Critically, the surviving ops MUST still be returned alongside
	// the error — the call site decides to fall through and archive
	// them. This is the property that distinguishes the new design
	// from "any non-nil error means drop the result."
	require.Len(t, got, 2, "the two well-formed ops must still be emitted")
	require.Equal(t, "good0", got[0].Rkey)
	require.Equal(t, payloads[0], got[0].Payload)
	require.Equal(t, "good1", got[1].Rkey)
	require.Equal(t, payloads[1], got[1].Payload)
}

// TestConvertEvent_CommitResync pins the post-Sync-1.1 mapping:
// atmos's verifier triggers an async resync after a chain break,
// and the resulting ops arrive with Action=ActionResync. They
// carry the live record bytes; we map them to KindCreateResync so
// /subscribe can hide the replacement row while /subscribe-v2 and
// archive readers can still observe the post-resync state.
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

	got, err := ConvertEvent(evt, testWitnessedAt)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, segment.KindCreateResync, got[0].Kind, "ActionResync must map to KindCreateResync")
	require.Equal(t, did, got[0].DID)
	require.Equal(t, "app.bsky.feed.post", got[0].Collection)
	require.Equal(t, "rec0", got[0].Rkey)
	require.Equal(t, "3l3qo2vutsw2c", got[0].Rev)
	require.Equal(t, payloads[0], got[0].Payload, "resync ops carry the live record bytes")
}

// TestConvertEvent_CommitInvalidRev_DropsWholeEvent pins the #197
// validation gate: a #commit whose rev is not a spec-valid TID is
// dropped in its entirety (every op shares the commit rev, and rev
// ordering drives merge-filter and compaction decisions — archiving
// rows under a garbage rev would silently corrupt those decisions).
// The drop is surfaced as a typed *InvalidEventError carrying the
// reason so the consumer can count it on the shared drop metric.
func TestConvertEvent_CommitInvalidRev_DropsWholeEvent(t *testing.T) {
	t.Parallel()

	for _, rev := range []string{"", "not-a-tid", "3l3qo2vutsw2", "3l3qo2vutsw2bb", "Al3qo2vutsw2b", "3l3qo2vutsw1b"} {
		evt, _ := buildCommit(t, "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", rev,
			struct{ Coll, Rkey string }{"app.bsky.feed.post", "rec0"},
		)
		got, err := ConvertEvent(evt, testWitnessedAt)
		ive, ok := errors.AsType[*InvalidEventError](err)
		require.True(t, ok, "rev=%q must surface *InvalidEventError, got err=%v", rev, err)
		require.Equal(t, ingest.DropReasonInvalidRev, ive.Reason)
		require.Nil(t, got, "rev=%q: no events may survive an invalid commit rev", rev)
	}
}

// TestConvertEvent_SyncInvalidRev_DropsWholeEvent: a #sync's rev seeds
// the DID tombstone that compaction folds by rev comparison, so an
// invalid rev must drop the event (tombstone plus any replacement
// rows) rather than archive a tombstone with garbage ordering.
func TestConvertEvent_SyncInvalidRev_DropsWholeEvent(t *testing.T) {
	t.Parallel()

	sync := &comatproto.SyncSubscribeRepos_Sync{
		DID: "did:plc:ddd",
		Rev: "not-a-tid-rev",
	}
	got, err := ConvertEvent(streaming.Event{Seq: 101, Sync: sync}, testWitnessedAt)
	ive, ok := errors.AsType[*InvalidEventError](err)
	require.True(t, ok, "invalid sync rev must surface *InvalidEventError, got %v", err)
	require.Equal(t, ingest.DropReasonInvalidRev, ive.Reason)
	require.Nil(t, got)
}

// TestConvertEvent_CommitInvalidCollection_DropsOpKeepsSiblings pins
// per-op path validation: an op whose collection is not a spec-valid
// NSID — including a `$`-prefixed name that could shadow the
// $account/$identity/$sync sentinel index — is dropped while
// well-formed siblings in the same commit survive. Wire op paths are
// attacker-controlled independently of the CAR contents, so the op
// can reference a perfectly valid block.
func TestConvertEvent_CommitInvalidCollection_DropsOpKeepsSiblings(t *testing.T) {
	t.Parallel()

	for _, coll := range []string{"$account", "nodots", "two.segments", ".leading.dot.x", "app.bsky.feed.9name"} {
		evt, payloads := buildCommit(t, "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", "3l3qo2vutsw2b",
			struct{ Coll, Rkey string }{"app.bsky.feed.post", "good0"},
		)
		// Splice a hostile op that reuses the valid record's CID so the
		// block lookup succeeds and only the collection check can drop it.
		evil := evt.Commit.Ops[0]
		evil.Path = coll + "/evilrkey"
		evt.Commit.Ops = append(evt.Commit.Ops, evil)

		got, err := ConvertEvent(evt, testWitnessedAt)
		doe, ok := errors.AsType[*DroppedOpsError](err)
		require.True(t, ok, "coll=%q must surface *DroppedOpsError, got %v", coll, err)
		require.Len(t, doe.Dropped, 1)
		require.Equal(t, ingest.DropReasonInvalidCollection, doe.Dropped[0].Reason)
		require.Equal(t, coll, doe.Dropped[0].Collection)

		require.Len(t, got, 1, "coll=%q: the well-formed sibling must survive", coll)
		require.Equal(t, "good0", got[0].Rkey)
		require.Equal(t, payloads[0], got[0].Payload)
	}
}

// TestConvertEvent_CommitInvalidRkey_DropsOpKeepsSiblings: same gate,
// record-key side. Also covers delete ops (a delete with a
// spec-invalid rkey can never match an archived record; archiving it
// would put unmatchable garbage in the tombstone path).
func TestConvertEvent_CommitInvalidRkey_DropsOpKeepsSiblings(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		rkey   string
		action string
	}{
		{".", "create"},
		{"..", "create"},
		{"has space", "create"},
		{"emoji\xf0\x9f\x98\x80", "create"},
		{"..", "delete"},
	} {
		evt, payloads := buildCommit(t, "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", "3l3qo2vutsw2b",
			struct{ Coll, Rkey string }{"app.bsky.feed.post", "good0"},
		)
		evil := evt.Commit.Ops[0]
		evil.Action = tc.action
		evil.Path = "app.bsky.feed.post/" + tc.rkey
		if tc.action == "delete" {
			evil.CID = gt.None[lextypes.LexCIDLink]()
		}
		evt.Commit.Ops = append(evt.Commit.Ops, evil)

		got, err := ConvertEvent(evt, testWitnessedAt)
		doe, ok := errors.AsType[*DroppedOpsError](err)
		require.True(t, ok, "rkey=%q must surface *DroppedOpsError, got %v", tc.rkey, err)
		require.Len(t, doe.Dropped, 1)
		require.Equal(t, ingest.DropReasonInvalidRkey, doe.Dropped[0].Reason)
		require.Equal(t, tc.rkey, doe.Dropped[0].RKey)

		require.Len(t, got, 1, "rkey=%q: the well-formed sibling must survive", tc.rkey)
		require.Equal(t, payloads[0], got[0].Payload)
	}
}

// TestConvertEvent_MissingBlockDrop_CarriesReason pins that the
// pre-existing missing-block drop now reports itself through the same
// generalized DroppedOpsError with its own reason, so the consumer
// can fold all per-op drops into one labeled counter family.
func TestConvertEvent_MissingBlockDrop_CarriesReason(t *testing.T) {
	t.Parallel()

	evt, _ := buildCommit(t, "did:plc:missingcar", "3l3qo2vutsw2b",
		struct{ Coll, Rkey string }{"app.bsky.feed.post", "good0"},
	)
	orphanCID := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("not-in-the-car"))
	evt.Commit.Ops = append(evt.Commit.Ops, comatproto.SyncSubscribeRepos_RepoOp{
		Action: "create",
		Path:   "app.bsky.feed.post/orphan",
		CID:    gt.Some(lextypes.LexCIDLink{Link: orphanCID.String()}),
	})

	got, err := ConvertEvent(evt, testWitnessedAt)
	doe, ok := errors.AsType[*DroppedOpsError](err)
	require.True(t, ok)
	require.Len(t, doe.Dropped, 1)
	require.Equal(t, ingest.DropReasonMissingBlock, doe.Dropped[0].Reason)
	require.Len(t, got, 1)
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
		Rev:  "3l3qo2vutsw2f",
		Ops: []comatproto.SyncSubscribeRepos_RepoOp{
			{Action: "lol-no", Path: "x.y/r"},
		},
		Blocks: carBuf.Bytes(),
	}

	_, err = ConvertEvent(streaming.Event{Commit: commit}, testWitnessedAt)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown commit action")
}

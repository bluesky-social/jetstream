package livestream

import (
	"bytes"
	"errors"
	"math"
	"math/rand/v2"
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

// swarmIterations is the number of swarm events to generate per test.
// Short mode is the everyday fast loop; the long run gets a much
// larger count to surface invariant violations.
func swarmIterations(t *testing.T) int {
	t.Helper()
	if testing.Short() {
		return 200
	}
	return 2000
}

// swarmCase is one synthesized input plus the expected outcome.
//
// Exactly one of expectErr / expectAnyErr / expectN must be the
// "operative" expectation — the test asserts on whichever is set.
type swarmCase struct {
	desc         string // why this case exists, useful in failure messages
	evt          streaming.Event
	expectErr    error          // non-nil → errors.Is(got, expectErr) must hold
	expectAnyErr bool           // if true, any non-nil error is acceptable
	expectN      int            // success path: exact number of segment.Events
	expectK      []segment.Kind // success path: kinds in order
}

// TestConvertEvent_Swarm exercises the actual semantic contract of
// ConvertEvent: malformed inputs surface as errors, well-formed
// inputs round-trip with the right Kind/DID/payload, column-width
// limits hold, and no input panics. The assertions are structured
// per-case (expected error or expected outputs) so a regression that
// drops events on the floor cannot pass — it would either yield the
// wrong count, the wrong Kind, or fail to error on an adversarial
// input.
//
// Critically, this test asserts a per-Kind minimum success count at
// the end so a regression that errors on every input (e.g. a
// mistakenly-introduced top-level guard) is caught — the original
// swarm would have silently `continue`d every iteration and reported
// green.
func TestConvertEvent_Swarm(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewPCG(0xfeed, 0xface))

	successByKind := map[segment.Kind]int{}
	expectedErrSeen := 0
	unknownErrSeen := 0

	const indexedAt int64 = 1_700_000_000_000_000

	for i := range swarmIterations(t) {
		c := nextSwarmCase(t, r)

		// Recover from panic so the test can localize the offender,
		// then re-fail. The original swarm only caught panics
		// implicitly via the test runner crash.
		var got []segment.Event
		var gotErr error
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					t.Fatalf("iter %d (%s): ConvertEvent panicked: %v", i, c.desc, rec)
				}
			}()
			got, gotErr = ConvertEvent(c.evt, indexedAt)
		}()

		switch {
		case c.expectErr != nil:
			require.ErrorIs(t, gotErr, c.expectErr,
				"iter %d (%s): expected sentinel error", i, c.desc)
			require.Nil(t, got, "iter %d (%s): error path must not yield events", i, c.desc)
			expectedErrSeen++
			if errors.Is(gotErr, ErrUnknownEventKind) {
				unknownErrSeen++
			}

		case c.expectAnyErr:
			require.Error(t, gotErr,
				"iter %d (%s): expected any non-nil error", i, c.desc)
			require.Nil(t, got, "iter %d (%s): error path must not yield events", i, c.desc)
			expectedErrSeen++

		default:
			require.NoError(t, gotErr, "iter %d (%s): expected success", i, c.desc)
			require.Len(t, got, c.expectN,
				"iter %d (%s): wrong event count", i, c.desc)
			for j, want := range c.expectK {
				require.Equal(t, want, got[j].Kind,
					"iter %d (%s): kind[%d]", i, c.desc, j)
			}
			for j, ev := range got {
				assertSegmentInvariants(t, i, c.desc, j, ev, indexedAt)
				successByKind[ev.Kind]++
			}
		}
	}

	// Coverage floor: every kind must succeed at least N times. A
	// regression that errors on every input would show 0s here.
	for _, k := range []segment.Kind{
		segment.KindCreate, segment.KindDelete,
		segment.KindIdentity, segment.KindAccount, segment.KindSync,
	} {
		require.Greater(t, successByKind[k], 0,
			"swarm produced zero successful conversions of Kind=%d", k)
	}
	require.Greater(t, expectedErrSeen, 0,
		"swarm never exercised an error path — adversarial cases are not landing")
	require.Greater(t, unknownErrSeen, 0,
		"swarm never exercised the ErrUnknownEventKind path")
}

// assertSegmentInvariants checks the column-width and field
// requirements that segment encoding will impose downstream. Anything
// ConvertEvent emits MUST satisfy these or the segment writer will
// reject it at runtime.
func assertSegmentInvariants(t *testing.T, iter int, desc string, j int, ev segment.Event, indexedAt int64) {
	t.Helper()
	require.Equal(t, indexedAt, ev.IndexedAt,
		"iter %d (%s) ev[%d]: IndexedAt must be propagated", iter, desc, j)
	require.Equal(t, uint64(0), ev.Seq,
		"iter %d (%s) ev[%d]: Seq is allocated downstream", iter, desc, j)
	require.NotEmpty(t, ev.DID,
		"iter %d (%s) ev[%d]: DID must not be empty", iter, desc, j)
	require.LessOrEqual(t, len(ev.DID), math.MaxUint16,
		"iter %d (%s) ev[%d]: DID exceeds uint16 column", iter, desc, j)
	require.LessOrEqual(t, len(ev.Collection), math.MaxUint8,
		"iter %d (%s) ev[%d]: Collection exceeds uint8 column", iter, desc, j)
	require.LessOrEqual(t, len(ev.Rkey), math.MaxUint8,
		"iter %d (%s) ev[%d]: Rkey exceeds uint8 column", iter, desc, j)
	require.LessOrEqual(t, len(ev.Rev), math.MaxUint8,
		"iter %d (%s) ev[%d]: Rev exceeds uint8 column", iter, desc, j)
	switch ev.Kind {
	case segment.KindCreate, segment.KindUpdate:
		require.NotNil(t, ev.Payload,
			"iter %d (%s) ev[%d]: Create/Update must carry payload bytes", iter, desc, j)
	case segment.KindDelete:
		require.Nil(t, ev.Payload,
			"iter %d (%s) ev[%d]: Delete must not carry payload bytes", iter, desc, j)
	case segment.KindIdentity, segment.KindAccount, segment.KindSync:
		require.NotEmpty(t, ev.Payload,
			"iter %d (%s) ev[%d]: non-commit kinds carry CBOR payload", iter, desc, j)
	}
}

// nextSwarmCase chooses a case with a weighted distribution so each
// branch of ConvertEvent gets meaningful coverage and adversarial
// cases land regularly.
func nextSwarmCase(t *testing.T, r *rand.Rand) swarmCase {
	t.Helper()
	switch r.IntN(12) {
	case 0, 1:
		return wellFormedCommit(t, r)
	case 2:
		return wellFormedDelete(t, r)
	case 3:
		return wellFormedIdentity(r)
	case 4:
		return wellFormedAccount(r)
	case 5:
		return wellFormedSync(r)
	case 6:
		return commitWithUnknownAction(t, r)
	case 7:
		return commitWithMissingCAR(t, r)
	case 8:
		return wellFormedInfo()
	case 9:
		return wellFormedResync(t, r)
	default:
		return adversarialUnknownEvent(r)
	}
}

// wellFormedDelete builds a #commit whose op is a delete. Deletes
// have no CID and no payload, so this exercises the "skip BlockData"
// branch of convertCommit and the segment.KindDelete case of the
// invariant assertions.
func wellFormedDelete(t *testing.T, r *rand.Rand) swarmCase {
	t.Helper()
	did := randomDID(r)

	// Build a valid CAR (atmos requires one even for delete-only ops).
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	mstore := mst.NewMemBlockStore()
	repo := &atmosrepo.Repo{
		DID:   atmosDIDFromString(t, did),
		Clock: atmos.NewTIDClock(0),
		Store: mstore,
		Tree:  mst.NewTree(mstore),
	}
	require.NoError(t, repo.Create(randomCollection(r), randomRkey(r), map[string]any{"v": 0}))
	var carBuf bytes.Buffer
	require.NoError(t, repo.ExportCAR(&carBuf, key))

	return swarmCase{
		desc: "well-formed delete",
		evt: streaming.Event{
			Seq: int64(r.Uint32()),
			Commit: &comatproto.SyncSubscribeRepos_Commit{
				Repo: did,
				Rev:  "rev-del",
				Ops: []comatproto.SyncSubscribeRepos_RepoOp{{
					Action: "delete",
					Path:   randomCollection(r) + "/" + randomRkey(r),
					CID:    gt.None[lextypes.LexCIDLink](),
				}},
				Blocks: carBuf.Bytes(),
			},
		},
		expectN: 1,
		expectK: []segment.Kind{segment.KindDelete},
	}
}

func wellFormedCommit(t *testing.T, r *rand.Rand) swarmCase {
	t.Helper()
	did := randomDID(r)
	rev := "3l3qo2vutsw2b"

	// Mix of creates and one occasional delete. Deletes have nil
	// payload; creates carry the CAR's CBOR block.
	nOps := 1 + r.IntN(3)
	specs := make([]struct{ Coll, Rkey string }, nOps)
	for i := range specs {
		specs[i] = struct{ Coll, Rkey string }{
			Coll: randomCollection(r),
			Rkey: randomRkey(r),
		}
	}
	evt, _ := buildCommit(t, did, rev, specs...)

	kinds := make([]segment.Kind, nOps)
	for i := range kinds {
		kinds[i] = segment.KindCreate
	}
	return swarmCase{
		desc:    "well-formed commit",
		evt:     evt,
		expectN: nOps,
		expectK: kinds,
	}
}

// wellFormedResync builds a #commit whose ops are all ActionResync
// (the wire form of streaming.ActionResync). Atmos's verifier resync
// worker emits these after a chain break, with the live record bytes
// in the CAR. ConvertEvent maps ActionResync → KindCreate so the
// archive records the post-resync state (a duplicate Create is
// acceptable; the segment is an event log, not a state table).
func wellFormedResync(t *testing.T, r *rand.Rand) swarmCase {
	t.Helper()
	did := randomDID(r)
	rev := "3l3qo2vutsw2c"

	nOps := 1 + r.IntN(3)
	specs := make([]struct{ Coll, Rkey string }, nOps)
	for i := range specs {
		specs[i] = struct{ Coll, Rkey string }{
			Coll: randomCollection(r),
			Rkey: randomRkey(r),
		}
	}
	evt, _ := buildCommit(t, did, rev, specs...)
	// Mutate every op's Action from "create" to "resync". The
	// payload bytes in the CAR remain valid and addressable; only
	// the action label changes.
	for i := range evt.Commit.Ops {
		evt.Commit.Ops[i].Action = string(streaming.ActionResync)
	}

	kinds := make([]segment.Kind, nOps)
	for i := range kinds {
		kinds[i] = segment.KindCreate
	}
	return swarmCase{
		desc:    "well-formed resync",
		evt:     evt,
		expectN: nOps,
		expectK: kinds,
	}
}

func wellFormedIdentity(r *rand.Rand) swarmCase {
	return swarmCase{
		desc: "well-formed identity",
		evt: streaming.Event{
			Seq: int64(r.Uint32()),
			Identity: &comatproto.SyncSubscribeRepos_Identity{
				DID:    randomDID(r),
				Handle: gt.Some("h.test"),
				Time:   "2026-05-21T00:00:00Z",
			},
		},
		expectN: 1,
		expectK: []segment.Kind{segment.KindIdentity},
	}
}

func wellFormedAccount(r *rand.Rand) swarmCase {
	return swarmCase{
		desc: "well-formed account",
		evt: streaming.Event{
			Seq: int64(r.Uint32()),
			Account: &comatproto.SyncSubscribeRepos_Account{
				DID:    randomDID(r),
				Active: r.IntN(2) == 0,
				Time:   "2026-05-21T00:00:00Z",
			},
		},
		expectN: 1,
		expectK: []segment.Kind{segment.KindAccount},
	}
}

func wellFormedSync(r *rand.Rand) swarmCase {
	return swarmCase{
		desc: "well-formed sync",
		evt: streaming.Event{
			Seq: int64(r.Uint32()),
			Sync: &comatproto.SyncSubscribeRepos_Sync{
				DID:    randomDID(r),
				Rev:    "rev",
				Blocks: []byte{0x01, 0x02},
				Time:   "2026-05-21T00:00:00Z",
			},
		},
		expectN: 1,
		expectK: []segment.Kind{segment.KindSync},
	}
}

func wellFormedInfo() swarmCase {
	// #info events are archival no-ops: zero events, no error. The
	// caller IS allowed to advance the cursor for them.
	return swarmCase{
		desc:    "info (no-op)",
		evt:     streaming.Event{Info: &comatproto.SyncSubscribeRepos_Info{}},
		expectN: 0,
	}
}

func commitWithUnknownAction(t *testing.T, r *rand.Rand) swarmCase {
	t.Helper()
	did := randomDID(r)

	// Need a valid CAR for the iterator's first decode pass even
	// though the action will fail before we use the block.
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	mstore := mst.NewMemBlockStore()
	repo := &atmosrepo.Repo{
		DID:   atmosDIDFromString(t, did),
		Clock: atmos.NewTIDClock(0),
		Store: mstore,
		Tree:  mst.NewTree(mstore),
	}
	var carBuf bytes.Buffer
	require.NoError(t, repo.ExportCAR(&carBuf, key))

	bogusActions := []string{"explode", "rotate", "garbage", ""}
	return swarmCase{
		desc: "commit with unknown action",
		evt: streaming.Event{
			Seq: int64(r.Uint32()),
			Commit: &comatproto.SyncSubscribeRepos_Commit{
				Repo: did,
				Rev:  "rev-bad",
				Ops: []comatproto.SyncSubscribeRepos_RepoOp{{
					Action: bogusActions[r.IntN(len(bogusActions))],
					Path:   "x.y/r",
				}},
				Blocks: carBuf.Bytes(),
			},
		},
		// actionKind returns a plain fmt.Errorf, not a sentinel —
		// ErrUnknownEventKind is reserved for top-level kind misses.
		// Just require that some error comes back.
		expectAnyErr: true,
	}
}

func commitWithMissingCAR(t *testing.T, r *rand.Rand) swarmCase {
	t.Helper()
	did := randomDID(r)

	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	mstore := mst.NewMemBlockStore()
	repo := &atmosrepo.Repo{
		DID:   atmosDIDFromString(t, did),
		Clock: atmos.NewTIDClock(0),
		Store: mstore,
		Tree:  mst.NewTree(mstore),
	}
	var carBuf bytes.Buffer
	require.NoError(t, repo.ExportCAR(&carBuf, key))

	return swarmCase{
		desc: "commit referencing CID missing from CAR",
		evt: streaming.Event{
			Seq: int64(r.Uint32()),
			Commit: &comatproto.SyncSubscribeRepos_Commit{
				Repo: did,
				Rev:  "rev-orphan",
				Ops: []comatproto.SyncSubscribeRepos_RepoOp{{
					Action: "create",
					Path:   "app.bsky.feed.post/orphan",
					CID: gt.Some(lextypes.LexCIDLink{
						Link: "bafyreiabsentcidthatisnotinthecarfile00000000000000000",
					}),
				}},
				Blocks: carBuf.Bytes(),
			},
		},
		expectAnyErr: true,
	}
}

func adversarialUnknownEvent(r *rand.Rand) swarmCase {
	// Empty event — ConvertEvent must surface ErrUnknownEventKind so
	// the consumer's processBatch refuses to advance the cursor.
	return swarmCase{
		desc:      "empty / unrecognized event",
		evt:       streaming.Event{Seq: int64(r.Uint32())},
		expectErr: ErrUnknownEventKind,
	}
}

// randomDID produces a syntactically-plausible DID with a length
// drawn from a distribution that includes near-boundary values, so
// the uint16-column-width invariant on segment.Event.DID can be
// exercised — the original generator was fixed at 24 bytes and never
// tripped it. Capped at atmos's ParseDID 2048-char ceiling.
func randomDID(r *rand.Rand) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	const atmosMaxDIDLen = 2048
	// 1-in-50 chance of a long DID near atmos's parse ceiling.
	var n int
	switch r.IntN(50) {
	case 0:
		n = atmosMaxDIDLen - len("did:plc:")
	case 1, 2, 3:
		n = 1024
	default:
		n = 8 + r.IntN(64)
	}
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = alphabet[r.IntN(len(alphabet))]
	}
	return "did:plc:" + string(buf)
}

// randomCollection returns a typical NSID. Empty / near-boundary
// values are NOT generated here because they would fail atmos's repo
// validation (the test fixture builder uses atmos.Repo.Create which
// rejects empty NSIDs); the column-width invariants for those edge
// cases are exercised by the unit tests in events_test.go and by the
// segment package's own swarm.
func randomCollection(_ *rand.Rand) string {
	return "app.bsky.feed.post"
}

// randomRkey returns a TID-shaped rkey. Same caveat as
// randomCollection re: atmos validation.
func randomRkey(r *rand.Rand) string {
	const tid = "abcdefghijklmnopqrstuvwxyz234567"
	buf := make([]byte, 13)
	for i := range buf {
		buf[i] = tid[r.IntN(len(tid))]
	}
	return string(buf)
}

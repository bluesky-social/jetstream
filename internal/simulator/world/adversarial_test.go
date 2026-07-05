package world

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/mst"
	"github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/require"
)

// adversarialOpCases are the lie classes GenerateAdversarialOpForTest
// must carry through the honest pipeline. Each must survive raw MST
// insert, CAR packaging, wire CBOR, and commit inversion (the
// structural checks atmos's verifier applies), spike-verified
// 2026-07-04 and pinned here against atmos drift.
var adversarialOpCases = []struct {
	name   string
	badKey string
	reason string
}{
	{"null_byte_rkey", "app.bsky.feed.post/bad\x00key", "invalid_rkey"},
	{"emoji_rkey", "app.bsky.feed.post/bad\U0001F600key", "invalid_rkey"},
	{"dot_rkey", "app.bsky.feed.post/.", "invalid_rkey"},
	{"dotdot_rkey", "app.bsky.feed.post/..", "invalid_rkey"},
	{"rkey_over_512", "app.bsky.feed.post/" + strings.Repeat("x", 600), "invalid_rkey"},
	{"rkey_unrepresentable_300", "app.bsky.feed.post/" + strings.Repeat("x", 300), "field_too_long"},
	{"dollar_collection", "$account/3lzzzzzzzzz2a", "invalid_collection"},
	{"empty_collection", "/3lzzzzzzzzz2a", "invalid_collection"},
	{"no_slash", "nosslashatall", "invalid_collection"},
	{"unicode_collection", "app.bskÿ.feed.post/3lzzzzzzzzz2a", "invalid_collection"},
	{"two_segment_nsid", "bsky.post/3lzzzzzzzzz2a", "invalid_collection"},
}

func TestGenerateAdversarialOpForTest_VerifierConsistentLies(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t)

	for i, tc := range adversarialOpCases {
		t.Run(tc.name, func(t *testing.T) {
			sibling, err := w.GenerateAdversarialOpForTest(context.Background(), i, tc.badKey, tc.reason)
			require.NoError(t, err)
			require.NotEmpty(t, sibling.Rev)

			frames, err := w.FirehoseRange(int64(i), 1)
			require.NoError(t, err)
			require.Len(t, frames, 1)
			body, ok := bytes.CutPrefix(frames[0], frameHeaderCommit)
			require.True(t, ok, "expected #commit header")

			// Wire round-trip: the lie survives byte-exactly.
			var cm comatproto.SyncSubscribeRepos_Commit
			require.NoError(t, cm.UnmarshalCBOR(body))
			require.Len(t, cm.Ops, 2)
			require.Equal(t, tc.badKey, cm.Ops[1].Path, "adversarial path must survive the wire byte-exactly")
			require.Equal(t, sibling.Collection+"/"+sibling.Rkey, cm.Ops[0].Path)

			// Envelope/inner-commit agreement + signature: what the
			// verifier's checkCommitFields + sig pass require.
			rp, commit, err := repo.LoadFromCAR(bytes.NewReader(cm.Blocks))
			require.NoError(t, err)
			require.Equal(t, cm.Rev, commit.Rev)
			acct := accountForRepo(t, w, cm.Repo)
			require.NoError(t, commit.VerifySignature(acct.priv.PublicKey()))

			// MST consistency: the verifier's checkOpCIDs does
			// tree.Get(op.Path) on the post-state MST from the CAR.
			tree := mst.LoadTree(rp.Store, commit.Data)
			got, err := tree.Get(tc.badKey)
			require.NoError(t, err)
			require.NotNil(t, got, "adversarial key must be present in the signed MST")

			// Inversion: the verifier inverts ops against the CAR.
			_, err = atmossync.InvertCommit(&cm)
			require.NoError(t, err, "adversarial commit must invert cleanly")
		})
	}

	// Ledger: one entry per case, in order, with the expected shape.
	entries := w.AdversarialLedger().Entries()
	require.Len(t, entries, len(adversarialOpCases))
	for i, tc := range adversarialOpCases {
		e := entries[i]
		require.Equal(t, AdversarialSourceLive, e.Source)
		require.Equal(t, AdversarialLayerGate, e.Layer)
		require.Equal(t, tc.reason, e.Reason)
		require.Equal(t, int64(i+1), e.Seq)
		require.NotEmpty(t, e.DID)
		require.False(t, e.WholeEvent)
		coll, rkey := repo.SplitMSTKey(tc.badKey)
		require.Equal(t, coll, e.Collection)
		require.Equal(t, rkey, e.Rkey)
	}
}

func TestGenerateAdversarialOpForTest_SiblingArchivable(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t)

	sibling, err := w.GenerateAdversarialOpForTest(context.Background(), 0, "app.bsky.feed.post/.", "invalid_rkey")
	require.NoError(t, err)

	// The sibling rides the same frame with a CID resolvable from the
	// CAR — the property the ingest gate needs to archive it while
	// dropping the lie.
	frames, err := w.FirehoseRange(0, 1)
	require.NoError(t, err)
	body, _ := bytes.CutPrefix(frames[0], frameHeaderCommit)
	var cm comatproto.SyncSubscribeRepos_Commit
	require.NoError(t, cm.UnmarshalCBOR(body))
	rp, _, err := repo.LoadFromCAR(bytes.NewReader(cm.Blocks))
	require.NoError(t, err)
	_, blockData, err := rp.Get(sibling.Collection, sibling.Rkey)
	require.NoError(t, err)
	require.Equal(t, sibling.Payload, blockData)
}

func TestInjectAdversarialRecordForBackfill_SilentAndPersisted(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t)

	// Invalid UTF-8 is the class that can ONLY go this route.
	badKey := "app.bsky.feed.post/bad\xff\xfekey"
	require.NoError(t, w.InjectAdversarialRecordForBackfill(context.Background(), 0, badKey, "invalid_rkey"))

	// Silent: no firehose frame.
	frames, err := w.FirehoseRange(0, 10)
	require.NoError(t, err)
	require.Empty(t, frames)

	// Persisted: the getRepo CAR export carries the lie.
	var carBuf bytes.Buffer
	require.NoError(t, w.ExportRepoCAR(0, &carBuf))
	rp, commit, err := repo.LoadFromCAR(bytes.NewReader(carBuf.Bytes()))
	require.NoError(t, err)
	tree := mst.LoadTree(rp.Store, commit.Data)
	got, err := tree.Get(badKey)
	require.NoError(t, err)
	require.NotNil(t, got, "adversarial key must be served by getRepo")

	entries := w.AdversarialLedger().Entries()
	require.Len(t, entries, 1)
	require.Equal(t, AdversarialSourceBackfill, entries[0].Source)
	require.Equal(t, "invalid_rkey", entries[0].Reason)
	require.Zero(t, entries[0].Seq)
}

func TestGenerateAdversarialSyncForTest_GarbageEnvelopeRev(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t)

	for _, badRev := range []string{"", "not-a-tid", "zzzzzzzzzzzzz"} {
		frame, err := w.GenerateAdversarialSyncForTest(context.Background(), 0, badRev)
		require.NoError(t, err)

		body, ok := bytes.CutPrefix(frame, frameHeaderSync)
		require.True(t, ok, "expected #sync header")
		var syncEvt comatproto.SyncSubscribeRepos_Sync
		require.NoError(t, syncEvt.UnmarshalCBOR(body))
		require.Equal(t, badRev, syncEvt.Rev, "lying rev must survive the wire")
		require.NotEmpty(t, syncEvt.Time)

		// The CAR body stays honest (real signed head).
		_, commit, err := repo.LoadFromCAR(bytes.NewReader(syncEvt.Blocks))
		require.NoError(t, err)
		acct := accountForRepo(t, w, syncEvt.DID)
		require.NoError(t, commit.VerifySignature(acct.priv.PublicKey()))
	}

	// Rejects an honest rev — the generator exists only to lie.
	_, err := w.GenerateAdversarialSyncForTest(context.Background(), 0, "3lzzzzzzzzz2a")
	require.Error(t, err)

	entries := w.AdversarialLedger().Entries()
	require.Len(t, entries, 3)
	for _, e := range entries {
		require.Equal(t, AdversarialSourceLive, e.Source)
		require.Equal(t, AdversarialLayerGate, e.Layer)
		require.Equal(t, "invalid_rev", e.Reason)
		require.True(t, e.WholeEvent)
		require.Positive(t, e.Seq)
	}
}

func TestGenerateVerifierRejectedCommitForTest_SignedInLie(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t)

	frame, err := w.GenerateVerifierRejectedCommitForTest(context.Background(), 0, "not-a-tid", "non_tid_rev")
	require.NoError(t, err)

	body, ok := bytes.CutPrefix(frame, frameHeaderCommit)
	require.True(t, ok)
	var cm comatproto.SyncSubscribeRepos_Commit
	require.NoError(t, cm.UnmarshalCBOR(body))
	require.Equal(t, "not-a-tid", cm.Rev)

	// repo.LoadFromCAR (the getRepo/backfill load path) must REJECT
	// this head — pins the plan-note claim that a lying non-empty head
	// rev fails at atmos's loader, never reaching jetstream's backfill
	// handler.
	_, _, err = repo.LoadFromCAR(bytes.NewReader(cm.Blocks))
	require.ErrorContains(t, err, "invalid rev")

	// The lie is SIGNED IN: envelope rev == inner commit rev, valid
	// signature — precisely what makes the verifier reject it as
	// InvalidRevError rather than FieldMismatchError. Decode the commit
	// block directly since the loader above refuses.
	_, blocks, err := car.ReadAll(bytes.NewReader(cm.Blocks))
	require.NoError(t, err)
	rootCID, err := cbor.ParseCIDString(cm.Commit.Link)
	require.NoError(t, err)
	var commitData []byte
	for _, b := range blocks {
		if b.CID == rootCID {
			commitData = b.Data
			break
		}
	}
	require.NotNil(t, commitData, "commit block must be present in the CAR")
	commit, err := repo.DecodeCommitCBOR(commitData)
	require.NoError(t, err)
	require.Equal(t, "not-a-tid", commit.Rev)
	acct := accountForRepo(t, w, cm.Repo)
	require.NoError(t, commit.VerifySignature(acct.priv.PublicKey()))

	entries := w.AdversarialLedger().Entries()
	require.Len(t, entries, 1)
	require.Equal(t, AdversarialLayerVerifier, entries[0].Layer)
	require.Equal(t, "non_tid_rev", entries[0].Reason)
	require.True(t, entries[0].WholeEvent)
}

func TestGenerateVerifierRejectedCommitForTest_HonestFollowUpRestoresChain(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t)

	_, err := w.GenerateVerifierRejectedCommitForTest(context.Background(), 0, "not-a-tid", "non_tid_rev")
	require.NoError(t, err)

	// The documented self-heal contract: the next honest commit on the
	// account must succeed and carry a valid, parseable rev again.
	frame, op, err := w.GenerateRecordOpForTest(context.Background(), 0, "create", collPost, "3lzzzzzzzzz2b")
	require.NoError(t, err)
	body, _ := bytes.CutPrefix(frame, frameHeaderCommit)
	var cm comatproto.SyncSubscribeRepos_Commit
	require.NoError(t, cm.UnmarshalCBOR(body))
	require.Equal(t, op.Rev, cm.Rev)
	require.NotEqual(t, "not-a-tid", cm.Rev)

	// Its Since points at the lie's rev — the chain-break shape that
	// forces the verifier's resync repair.
	require.True(t, cm.Since.HasVal())
	require.Equal(t, "not-a-tid", cm.Since.Val())
}

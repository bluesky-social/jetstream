package world

import (
	"bytes"
	"context"
	"testing"

	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/require"
)

// TestGenerateMultiOpCommitForTest_StripsOnlyChosenLeafBlocks pins the
// partial-CAR wire shape: the stripped op's CID is on the wire ops list
// but its record block is absent from the CAR, while sibling record
// blocks, the commit block, and the MST nodes all remain. The world's
// own persisted repo still holds every record — only the frame lies.
func TestGenerateMultiOpCommitForTest_StripsOnlyChosenLeafBlocks(t *testing.T) {
	t.Parallel()
	w := newRuntimeWorld(t, 4, 0)

	const idx = 0
	frame, ops, err := w.GenerateMultiOpCommitForTest(context.Background(), idx, []TargetedOpSpec{
		{Action: "create", Collection: collPost, Rkey: "survivor1"},
		{Action: "create", Collection: collPost, Rkey: "stripped1", StripBlock: true},
		{Action: "create", Collection: collPost, Rkey: "survivor2"},
	})
	require.NoError(t, err)
	require.Len(t, ops, 3)
	for _, op := range ops {
		require.NotEmpty(t, op.Payload, "descriptor still carries the true record block")
		require.Equal(t, ops[0].Rev, op.Rev, "all ops share the commit rev")
	}

	body, ok := bytes.CutPrefix(frame, frameHeaderCommit)
	require.True(t, ok, "expected #commit header")
	var cm comatproto.SyncSubscribeRepos_Commit
	require.NoError(t, cm.UnmarshalCBOR(body))
	require.Len(t, cm.Ops, 3)

	_, blocks, err := car.ReadAll(bytes.NewReader(cm.Blocks))
	require.NoError(t, err)
	carIndex := make(map[cbor.CID]struct{}, len(blocks))
	for _, b := range blocks {
		carIndex[b.CID] = struct{}{}
	}

	commitCID, err := cbor.ParseCIDString(cm.Commit.Link)
	require.NoError(t, err)
	require.Contains(t, carIndex, commitCID, "commit block must stay in the CAR")

	for _, wireOp := range cm.Ops {
		require.True(t, wireOp.CID.HasVal(), "create ops carry a CID on the wire")
		opCID, err := cbor.ParseCIDString(wireOp.CID.Val().Link)
		require.NoError(t, err)
		if wireOp.Path == collPost+"/stripped1" {
			require.NotContains(t, carIndex, opCID,
				"the stripped op's record block must be absent from the CAR")
		} else {
			require.Contains(t, carIndex, opCID,
				"sibling record blocks must survive in the CAR: %s", wireOp.Path)
		}
	}

	// The world's persisted repo is unaffected by the wire omission.
	rp, _, err := w.LoadRepo(idx)
	require.NoError(t, err)
	for _, rkey := range []string{"survivor1", "stripped1", "survivor2"} {
		_, _, err := rp.Get(collPost, rkey)
		require.NoErrorf(t, err, "record %s must be present in the world's repo head", rkey)
	}
}

// TestGenerateMultiOpCommitForTest_RejectsBadSpecs pins the loud-failure
// contract: duplicate paths, stripping a delete, and an empty spec list
// all error rather than emitting a frame the caller didn't ask for.
func TestGenerateMultiOpCommitForTest_RejectsBadSpecs(t *testing.T) {
	t.Parallel()
	w := newRuntimeWorld(t, 4, 0)

	_, _, err := w.GenerateMultiOpCommitForTest(context.Background(), 0, nil)
	require.Error(t, err, "empty spec list must fail")

	_, _, err = w.GenerateMultiOpCommitForTest(context.Background(), 0, []TargetedOpSpec{
		{Action: "create", Collection: collPost, Rkey: "dup1"},
		{Action: "create", Collection: collPost, Rkey: "dup1"},
	})
	require.Error(t, err, "duplicate paths in one commit must fail")

	_, _, err = w.GenerateMultiOpCommitForTest(context.Background(), 0, []TargetedOpSpec{
		{Action: "create", Collection: collPost, Rkey: "victim1"},
	})
	require.NoError(t, err)
	_, _, err = w.GenerateMultiOpCommitForTest(context.Background(), 0, []TargetedOpSpec{
		{Action: "delete", Collection: collPost, Rkey: "victim1", StripBlock: true},
	})
	require.Error(t, err, "stripping a delete op must fail")
}

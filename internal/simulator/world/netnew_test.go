package world

import (
	"bytes"
	"context"
	"testing"

	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/repo"
	"github.com/stretchr/testify/require"
)

func TestAddHiddenAccountForTest_OmittedFromListReposButServableAndLive(t *testing.T) {
	t.Parallel()
	w := newRuntimeWorld(t, 4, 1)

	idx, acct, err := w.AddHiddenAccountForTest(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, 4, idx)
	require.Equal(t, 4, w.AccountCount(), "hidden account must not expand listRepos roster")
	indices, err := w.AccountIndicesForTest()
	require.NoError(t, err)
	require.Contains(t, indices, idx)

	page, _, err := w.ListReposPage(0, 100)
	require.NoError(t, err)
	for _, entry := range page {
		require.NotEqual(t, acct.DID, entry.DID, "hidden DID must be omitted from listRepos")
	}

	found, ok, err := w.FindAccountByDID(acct.DID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, idx, found.Index)

	rp, _, err := w.LoadRepo(idx)
	require.NoError(t, err)
	require.Equal(t, acct.DID, rp.DID)

	frame, op, err := w.GenerateRecordOpForTest(context.Background(), idx, "create", collPost, "net-new-proof")
	require.NoError(t, err)
	require.Equal(t, "net-new-proof", op.Rkey)

	body, ok := bytes.CutPrefix(frame, frameHeaderCommit)
	require.True(t, ok)
	var cm comatproto.SyncSubscribeRepos_Commit
	require.NoError(t, cm.UnmarshalCBOR(body))
	require.Equal(t, string(acct.DID), cm.Repo)
	require.Len(t, cm.Ops, 1)

	liveRepo, _, err := repo.LoadFromCAR(bytes.NewReader(cm.Blocks))
	require.NoError(t, err)
	require.Equal(t, acct.DID, liveRepo.DID)
}

// Hidden accounts live at indices >= cfg.Accounts; the multi-op targeted
// generator must accept them just like GenerateRecordOpForTest does, or
// net-new accounts can never emit multi-op / partial-CAR fault traffic.
func TestGenerateMultiOpCommitForTest_AcceptsHiddenAccount(t *testing.T) {
	t.Parallel()
	w := newRuntimeWorld(t, 4, 1)

	idx, acct, err := w.AddHiddenAccountForTest(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, 4, idx)

	frame, ops, err := w.GenerateMultiOpCommitForTest(context.Background(), idx, []TargetedOpSpec{
		{Action: "create", Collection: collPost, Rkey: "net-new-multi-a"},
		{Action: "create", Collection: collPost, Rkey: "net-new-multi-b"},
	})
	require.NoError(t, err)
	require.Len(t, ops, 2)

	body, ok := bytes.CutPrefix(frame, frameHeaderCommit)
	require.True(t, ok)
	var cm comatproto.SyncSubscribeRepos_Commit
	require.NoError(t, cm.UnmarshalCBOR(body))
	require.Equal(t, string(acct.DID), cm.Repo)
	require.Len(t, cm.Ops, 2)
}

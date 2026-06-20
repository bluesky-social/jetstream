package world

import (
	"bytes"
	"context"
	"testing"

	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/repo"
	"github.com/stretchr/testify/require"
)

// TestGenerateRecordOpForTest_DrivesExactChain pins the targeted
// generator: a create→update→delete→recreate chain on a SINGLE caller-
// chosen rkey produces four valid #commit frames with the exact op
// actions, strictly increasing revs, and a CAR that round-trips. The
// recreate (same rkey, above the delete) proves a key can be reused —
// the no-permanent-tombstone fixture random traffic can't produce.
func TestGenerateRecordOpForTest_DrivesExactChain(t *testing.T) {
	t.Parallel()
	w := newRuntimeWorld(t, 4, 0)

	const (
		idx  = 0
		coll = collPost
		rkey = "3kchainkey00000"
	)

	steps := []struct {
		action       string
		wantPayload  bool
		wantInRepoAt bool // record present in repo head after this op
	}{
		{action: "create", wantPayload: true, wantInRepoAt: true},
		{action: "update", wantPayload: true, wantInRepoAt: true},
		{action: "delete", wantPayload: false, wantInRepoAt: false},
		{action: "create", wantPayload: true, wantInRepoAt: true},
	}

	var prevRev string
	for i, step := range steps {
		frame, op, err := w.GenerateRecordOpForTest(context.Background(), idx, step.action, coll, rkey)
		require.NoErrorf(t, err, "step %d (%s)", i, step.action)

		require.Equal(t, step.action, op.Action)
		require.Equal(t, coll, op.Collection)
		require.Equal(t, rkey, op.Rkey)
		require.NotEmpty(t, op.Rev)
		require.Greaterf(t, op.Rev, prevRev, "step %d rev must strictly increase", i)
		prevRev = op.Rev
		if step.wantPayload {
			require.NotEmpty(t, op.Payload, "step %d should carry a record block", i)
		} else {
			require.Nil(t, op.Payload, "delete carries no payload")
		}

		// Frame decodes as a #commit carrying exactly our one op.
		body, ok := bytes.CutPrefix(frame, frameHeaderCommit)
		require.True(t, ok, "step %d expected #commit header", i)
		var cm comatproto.SyncSubscribeRepos_Commit
		require.NoError(t, cm.UnmarshalCBOR(body))
		require.Len(t, cm.Ops, 1)
		require.Equal(t, step.action, cm.Ops[0].Action)
		require.Equal(t, coll+"/"+rkey, cm.Ops[0].Path)
		require.Equal(t, op.Rev, cm.Rev)

		// CAR round-trips; repo head reflects presence/absence of the key.
		rp, _, err := repo.LoadFromCAR(bytes.NewReader(cm.Blocks))
		require.NoError(t, err)
		require.Equal(t, cm.Repo, string(rp.DID))
	}

	// After the recreate, the live repo state shows the record present
	// again (no permanent tombstone in the world's own model).
	rp, _, err := w.LoadRepo(idx)
	require.NoError(t, err)
	_, _, err = rp.Get(coll, rkey)
	require.NoError(t, err, "recreated record must be present at head")
}

// TestGenerateRecordOpForTest_RejectsImpossibleOps pins the loud-failure
// contract: update/delete on a missing key and create on an existing key
// error rather than silently mutating a different path.
func TestGenerateRecordOpForTest_RejectsImpossibleOps(t *testing.T) {
	t.Parallel()
	w := newRuntimeWorld(t, 4, 0)

	const (
		idx  = 0
		coll = collPost
		rkey = "3krejectkey0000"
	)

	_, _, err := w.GenerateRecordOpForTest(context.Background(), idx, "update", coll, rkey)
	require.Error(t, err, "update on missing key must fail")

	_, _, err = w.GenerateRecordOpForTest(context.Background(), idx, "delete", coll, rkey)
	require.Error(t, err, "delete on missing key must fail")

	_, _, err = w.GenerateRecordOpForTest(context.Background(), idx, "create", coll, rkey)
	require.NoError(t, err)

	_, _, err = w.GenerateRecordOpForTest(context.Background(), idx, "create", coll, rkey)
	require.Error(t, err, "create on existing key must fail")
}

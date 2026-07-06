package oracle

import (
	"context"
	"testing"

	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/stretchr/testify/require"
)

func TestExpectedEventLogFromFirehoseExpandsCommitOps(t *testing.T) {
	t.Parallel()

	w := newExpectedEventLogWorld(t)
	_, err := w.GenerateOneForTest(context.Background())
	require.NoError(t, err)

	rows, err := ExpectedEventLogFromFirehose(w, 0, 10)
	require.NoError(t, err)
	require.NotEmpty(t, rows)

	for _, row := range rows {
		require.Equal(t, uint64(1), row.Seq)
		require.Contains(t, []string{"create", "update", "delete"}, row.Kind)
		require.NotEmpty(t, row.DID)
		require.NotEmpty(t, row.Collection)
		require.NotEmpty(t, row.Rkey)
		require.NotEmpty(t, row.Rev)
		if row.Kind == "create" || row.Kind == "update" {
			require.NotZero(t, row.PayloadLen)
			require.NotEmpty(t, row.PayloadSHA256_64)
		}
	}
}

func TestExpectedEventLogFromFirehoseNormalizesAccountAndSyncFrames(t *testing.T) {
	t.Parallel()

	w := newExpectedEventLogWorld(t)
	_, err := w.GenerateAccountDeleteForTest(context.Background(), 0)
	require.NoError(t, err)
	_, err = w.GenerateSyncForTest(context.Background(), 1)
	require.NoError(t, err)

	rows, err := ExpectedEventLogFromFirehose(w, 0, 10)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(rows), 3)

	require.Equal(t, uint64(1), rows[0].Seq)
	require.Equal(t, "account", rows[0].Kind)
	require.NotEmpty(t, rows[0].DID)
	require.NotZero(t, rows[0].PayloadLen)
	require.NotEmpty(t, rows[0].PayloadSHA256_64)
	require.True(t, rows[0].AccountDeleted)

	require.Equal(t, uint64(2), rows[1].Seq)
	require.Equal(t, "sync", rows[1].Kind)
	require.NotEmpty(t, rows[1].DID)
	require.NotEmpty(t, rows[1].Rev)
	require.NotZero(t, rows[1].PayloadLen)
	require.NotEmpty(t, rows[1].PayloadSHA256_64)
	require.Equal(t, uint64(2), rows[2].Seq)
	require.Equal(t, "create_resync", rows[2].Kind)
	require.Equal(t, rows[1].DID, rows[2].DID)
	require.NotEmpty(t, rows[2].Collection)
	require.NotEmpty(t, rows[2].Rkey)
	require.Equal(t, rows[1].Rev, rows[2].Rev)
	require.NotZero(t, rows[2].PayloadLen)
}

func TestExpectedEventLogFromFirehoseHonorsCursorAndLimit(t *testing.T) {
	t.Parallel()

	w := newExpectedEventLogWorld(t)
	_, err := w.GenerateOneForTest(context.Background())
	require.NoError(t, err)
	_, err = w.GenerateSyncForTest(context.Background(), 0)
	require.NoError(t, err)

	rows, err := ExpectedEventLogFromFirehose(w, 1, 1)
	require.NoError(t, err)
	require.NotEmpty(t, rows)
	for _, row := range rows {
		require.Equal(t, uint64(2), row.Seq)
	}
	require.Equal(t, "sync", rows[0].Kind)
}

// TestExpectedEventLogFromFirehoseDropsMissingBlockOps pins the model's
// mirror of the consumer's partial-CAR contract: an op whose record
// block is stripped from the CAR diff produces NO expected row, while
// sibling ops in the same commit expand normally.
func TestExpectedEventLogFromFirehoseDropsMissingBlockOps(t *testing.T) {
	t.Parallel()

	w := newExpectedEventLogWorld(t)
	_, ops, err := w.GenerateMultiOpCommitForTest(context.Background(), 0, []world.TargetedOpSpec{
		{Action: "create", Collection: "app.bsky.feed.post", Rkey: "survivor1"},
		{Action: "create", Collection: "app.bsky.feed.post", Rkey: "stripped1", StripBlock: true},
		{Action: "create", Collection: "app.bsky.feed.post", Rkey: "survivor2"},
	})
	require.NoError(t, err)
	require.Len(t, ops, 3)

	rows, err := ExpectedEventLogFromFirehose(w, 0, 10)
	require.NoError(t, err)
	require.Len(t, rows, 2, "the stripped op must not produce an expected row")
	for _, row := range rows {
		require.Equal(t, "create", row.Kind)
		require.Contains(t, []string{"survivor1", "survivor2"}, row.Rkey)
		require.NotZero(t, row.PayloadLen)
	}
}

func newExpectedEventLogWorld(t *testing.T) *world.World {
	t.Helper()

	cfg := Config{
		Mode:                "expected-event-log",
		Seed:                123,
		Accounts:            3,
		MinInitialRecords:   2,
		MaxInitialRecords:   4,
		LiveEventsBootstrap: 4,
		LiveEventsSteady:    4,
	}
	w := newRestartWorld(t, cfg)
	t.Cleanup(func() { require.NoError(t, w.Close()) })
	return w
}

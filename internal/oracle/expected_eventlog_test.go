package oracle

import (
	"context"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/simulator/world"
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
	require.Len(t, rows, 2)

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
	require.Len(t, rows, 1)
	require.Equal(t, uint64(2), rows[0].Seq)
	require.Equal(t, "sync", rows[0].Kind)
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

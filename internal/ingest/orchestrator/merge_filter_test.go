package orchestrator

import (
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)

func TestShouldKeep(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ev   segment.Event
		st   *backfill.RepoStatus
		want bool
	}{
		{
			name: "nil RepoStatus → keep create",
			ev:   segment.Event{Kind: segment.KindCreate, Rev: "3l4"},
			st:   nil,
			want: true,
		},
		{
			name: "nil RepoStatus → keep identity",
			ev:   segment.Event{Kind: segment.KindIdentity},
			st:   nil,
			want: true,
		},
		{
			name: "StatusNotStarted → keep create",
			ev:   segment.Event{Kind: segment.KindCreate, Rev: "3l4"},
			st:   &backfill.RepoStatus{Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusNotStarted, Rev: "ignored"}},
			want: true,
		},
		{
			name: "StatusFailed → keep create",
			ev:   segment.Event{Kind: segment.KindCreate, Rev: "3l4"},
			st:   &backfill.RepoStatus{Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusFailed, Rev: "ignored"}},
			want: true,
		},
		{
			name: "StatusComplete + empty BackfillRev → keep create (defensive)",
			ev:   segment.Event{Kind: segment.KindCreate, Rev: "3l4"},
			st:   &backfill.RepoStatus{Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: ""}},
			want: true,
		},
		{
			name: "StatusComplete + ev.Rev empty → keep (defensive)",
			ev:   segment.Event{Kind: segment.KindCreate, Rev: ""},
			st:   &backfill.RepoStatus{Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: "3l5"}},
			want: true,
		},
		{
			name: "create with ev.Rev < BackfillRev → drop",
			ev:   segment.Event{Kind: segment.KindCreate, Rev: "3l4"},
			st:   &backfill.RepoStatus{Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: "3l5"}},
			want: false,
		},
		{
			name: "create with ev.Rev == BackfillRev → drop",
			ev:   segment.Event{Kind: segment.KindCreate, Rev: "3l5"},
			st:   &backfill.RepoStatus{Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: "3l5"}},
			want: false,
		},
		{
			name: "create with ev.Rev > BackfillRev → keep",
			ev:   segment.Event{Kind: segment.KindCreate, Rev: "3l6"},
			st:   &backfill.RepoStatus{Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: "3l5"}},
			want: true,
		},
		{
			name: "update with ev.Rev <= BackfillRev → drop",
			ev:   segment.Event{Kind: segment.KindUpdate, Rev: "3l5"},
			st:   &backfill.RepoStatus{Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: "3l5"}},
			want: false,
		},
		{
			name: "delete with ev.Rev <= BackfillRev → drop",
			ev:   segment.Event{Kind: segment.KindDelete, Rev: "3l4"},
			st:   &backfill.RepoStatus{Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: "3l5"}},
			want: false,
		},
		{
			name: "identity with ev.Rev <= BackfillRev → keep (non-commit)",
			ev:   segment.Event{Kind: segment.KindIdentity},
			st:   &backfill.RepoStatus{Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: "3l5"}},
			want: true,
		},
		{
			name: "account → keep regardless",
			ev:   segment.Event{Kind: segment.KindAccount},
			st:   &backfill.RepoStatus{Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: "3l5"}},
			want: true,
		},
		{
			name: "sync → keep regardless",
			ev:   segment.Event{Kind: segment.KindSync},
			st:   &backfill.RepoStatus{Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: "3l5"}},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, shouldKeep(&tt.ev, tt.st))
		})
	}
}

func TestRepoStatusLookup_CachesAndCountsFirstReads(t *testing.T) {
	t.Parallel()
	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	// Seed one row; leave the other DID absent so we cover both paths.
	rs := &backfill.RepoStatus{
		Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: "rev-a"},
		Rev:      "rev-a",
	}
	enc, err := backfill.EncodeRepoStatus(rs)
	require.NoError(t, err)
	require.NoError(t, st.Set(backfill.RepoKey("did:plc:a"), enc, store.SyncWrites))

	var lookups int
	cache := newRepoStatusLookup(st, func() { lookups++ })

	got, err := cache.get("did:plc:a")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, "rev-a", got.Backfill.Rev)
	require.Equal(t, 1, lookups)

	// Cache hit on the second read; no new lookup.
	got2, err := cache.get("did:plc:a")
	require.NoError(t, err)
	require.Same(t, got, got2)
	require.Equal(t, 1, lookups)

	// Missing row caches a nil and counts as one lookup.
	missing, err := cache.get("did:plc:missing")
	require.NoError(t, err)
	require.Nil(t, missing)
	require.Equal(t, 2, lookups)

	// Repeat miss is served from the cache.
	missing2, err := cache.get("did:plc:missing")
	require.NoError(t, err)
	require.Nil(t, missing2)
	require.Equal(t, 2, lookups)

	// set replaces the cached row; subsequent reads return the new value
	// without going to pebble.
	updated := &backfill.RepoStatus{
		Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: "rev-a"},
		Rev:      "rev-b",
	}
	cache.set("did:plc:a", updated)
	got3, err := cache.get("did:plc:a")
	require.NoError(t, err)
	require.Equal(t, "rev-b", got3.Rev)
	require.Equal(t, 2, lookups)
}

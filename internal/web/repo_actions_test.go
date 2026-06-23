package web

import (
	"testing"

	"github.com/bluesky-social/jetstream/internal/repoexport"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// stubSelector is a no-op repoexport.Selector for tests that only exercise
// the pending-events wiring, not reconstruction.
type stubSelector struct{}

func (stubSelector) SelectBlocksForDID(string) ([]repoexport.BlockSelection, error) {
	return nil, nil
}
func (stubSelector) ActiveSegmentPaths() ([]string, error) { return nil, nil }

func TestRepoExportActions_PassesPendingEventsForDID(t *testing.T) {
	t.Parallel()

	const did = "did:plc:pending"
	var gotDID string
	provider := func(d string) []segment.Event {
		gotDID = d
		return []segment.Event{{Kind: segment.KindCreate, DID: d, Collection: "app.bsky.feed.like", Rkey: "r1", Rev: "rev1"}}
	}

	actions, ok := NewRepoActions(t.TempDir(), nil, stubSelector{}, provider).(repoExportActions)
	require.True(t, ok)
	require.NotNil(t, actions.pendingEvents)

	got := actions.pendingEvents(did)
	require.Equal(t, did, gotDID)
	require.Len(t, got, 1)
	require.Equal(t, did, got[0].DID)
}

func TestRepoExportActions_NilProviderYieldsNoPending(t *testing.T) {
	t.Parallel()

	actions, ok := NewRepoActions(t.TempDir(), nil, stubSelector{}, nil).(repoExportActions)
	require.True(t, ok)
	// VerifyRepo must tolerate a nil provider (offline / pre-steady-state)
	// without panicking when it gathers pending events.
	require.Nil(t, actions.gatherPending("did:plc:whatever"))
}

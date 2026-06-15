package web

import (
	"testing"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)

func TestRepoExportActions_PassesPendingEventsForDID(t *testing.T) {
	t.Parallel()

	const did = "did:plc:pending"
	var gotDID string
	provider := func(d string) []segment.Event {
		gotDID = d
		return []segment.Event{{Kind: segment.KindCreate, DID: d, Collection: "app.bsky.feed.like", Rkey: "r1", Rev: "rev1"}}
	}

	actions, ok := NewRepoActions(t.TempDir(), "http://127.0.0.1:1", provider).(repoExportActions)
	require.True(t, ok)
	require.NotNil(t, actions.pendingEvents)

	got := actions.pendingEvents(did)
	require.Equal(t, did, gotDID)
	require.Len(t, got, 1)
	require.Equal(t, did, got[0].DID)
}

func TestRepoExportActions_NilProviderYieldsNoPending(t *testing.T) {
	t.Parallel()

	actions, ok := NewRepoActions(t.TempDir(), "http://127.0.0.1:1", nil).(repoExportActions)
	require.True(t, ok)
	// VerifyRepo must tolerate a nil provider (offline / pre-steady-state)
	// without panicking when it gathers pending events.
	require.Nil(t, actions.gatherPending("did:plc:whatever"))
}

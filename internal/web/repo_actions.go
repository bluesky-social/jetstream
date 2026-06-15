package web

import (
	"context"

	"github.com/bluesky-social/jetstream-v2/internal/repoexport"
	"github.com/bluesky-social/jetstream-v2/segment"
)

type repoExportActions struct {
	dataDir  string
	relayURL string
	// pendingEvents returns the live writer's not-yet-flushed events for a
	// DID, folded into reconstruction so a record created moments ago is
	// reflected in verification before the next compaction flush. nil when
	// no live writer is available (offline tooling, pre-steady-state).
	pendingEvents func(did string) []segment.Event
}

// NewRepoActions builds the production repo action implementation used by the
// status handler. pendingEvents may be nil; when set it supplies the live
// writer's in-memory pending events so verification does not spuriously report
// a root mismatch for a just-written record.
func NewRepoActions(dataDir, relayURL string, pendingEvents func(did string) []segment.Event) RepoActions {
	return repoExportActions{
		dataDir:       dataDir,
		relayURL:      relayURL,
		pendingEvents: pendingEvents,
	}
}

// gatherPending returns the pending events for did, tolerating a nil provider.
func (a repoExportActions) gatherPending(did string) []segment.Event {
	if a.pendingEvents == nil {
		return nil
	}
	return a.pendingEvents(did)
}

func (a repoExportActions) VerifyRepo(ctx context.Context, did string) (repoexport.VerifyReport, error) {
	return repoexport.Verify(ctx, repoexport.VerifyConfig{
		DataDir:       a.dataDir,
		DID:           did,
		RelayURL:      a.relayURL,
		PendingEvents: a.gatherPending(did),
	})
}

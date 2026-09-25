package web

import (
	"context"

	"github.com/bluesky-social/jetstream/internal/repoexport"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/identity"
)

type repoExportActions struct {
	// archive is the segment archive reconstruction reads, including the
	// selector that prunes which blocks it decodes. Required for
	// verification to run.
	archive          repoexport.Archive
	identityResolver identity.Resolver
	// pendingEvents returns the live writer's not-yet-flushed events for a
	// DID, folded into reconstruction so a record created moments ago is
	// reflected in verification before the next compaction flush. nil when
	// no live writer is available (offline tooling, pre-steady-state).
	pendingEvents func(did string) []segment.Event
}

// NewRepoActions builds the production repo action implementation used by the
// status handler. archive's selector supplies the bloom pruning so
// verification decodes only the blocks an account touches. pendingEvents may
// be nil; when set it supplies the live writer's in-memory pending events so
// verification does not spuriously report a root mismatch for a just-written
// record.
func NewRepoActions(archive repoexport.Archive, identityResolver identity.Resolver, pendingEvents func(did string) []segment.Event) RepoActions {
	return repoExportActions{
		archive:          archive,
		identityResolver: identityResolver,
		pendingEvents:    pendingEvents,
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
		Archive:          a.archive,
		DID:              did,
		IdentityResolver: a.identityResolver,
		PendingEvents:    a.gatherPending(did),
	})
}

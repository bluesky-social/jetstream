package client

import "github.com/bluesky-social/jetstream/segment"

// didTombstoneSnapshot is the §R4 backfill-start suppression set: per DID, the
// highest account-delete/sync seq the server held over the planned range
// (afterSeq, plannedThroughSeq] at backfill start. The client folds it as
// DID-only, seq-scoped suppression of its OWN emitted create/update rows.
//
// It exists to close the §R3 gap: a collection-filtered backfill never
// downloads DID-level tombstones (they carry an empty collection and ride in no
// collection block), so without this snapshot a folding consumer keeps a
// deleted account's records forever. The snapshot is held for the whole
// backfill and is NEVER consulted for record-level entries (record
// deletes/updates carry collections and ride inline) and NEVER turned into a
// synthesized delete/account event (reactivation, design §R4: a record the
// account re-created at seq > tombstone is correctly retained, and the
// reactivation #account arrives on the live tail above the sealed tip).
type didTombstoneSnapshot map[string]uint64

// newDIDTombstoneSnapshot builds the suppression map from a plan's snapshot.
// Nil/empty is the common "nothing deleted in range" case and suppresses
// nothing.
func newDIDTombstoneSnapshot(entries []DIDTombstone) didTombstoneSnapshot {
	if len(entries) == 0 {
		return nil
	}
	snap := make(didTombstoneSnapshot, len(entries))
	for _, ts := range entries {
		// Keep the max per DID (defensive: the server sends one entry per DID,
		// but a duplicate must not lower the suppression bound).
		if ts.Seq > snap[ts.DID] {
			snap[ts.DID] = ts.Seq
		}
	}
	return snap
}

// suppresses reports whether ev is a materialization row whose DID was
// account-deleted/synced at a strictly higher seq within the snapshot range —
// i.e. a stale create/update the consumer must not be left holding. Strictly
// greater (not >=) so a record re-created exactly at the tombstone seq is
// impossible (one seq, one event) and a post-deletion re-create (seq >
// tombstone) is retained.
func (s didTombstoneSnapshot) suppresses(ev *segment.Event) bool {
	if len(s) == 0 || !ev.Kind.IsMaterialization() {
		return false
	}
	killSeq, ok := s[ev.DID]
	return ok && killSeq > ev.Seq
}

// snapshotSelector composes the exact-filter Matcher with DID-only start-snapshot
// suppression, satisfying the downloader's RowSelector. A row is kept iff the
// matcher wants it AND the snapshot does not suppress it. This is applied ONLY on
// the backfill download path — never the live tail, whose post-sealed-tip events
// (including account reactivation) are authoritative and must not be suppressed.
type snapshotSelector struct {
	matcher  *Matcher
	snapshot didTombstoneSnapshot
}

// newSnapshotSelector returns a RowSelector for the backfill download. When the
// snapshot is empty it returns the bare matcher (no wrapping cost), so the
// common no-deletions backfill is unchanged.
func newSnapshotSelector(matcher *Matcher, snapshot didTombstoneSnapshot) RowSelector {
	if len(snapshot) == 0 {
		return matcher
	}
	return &snapshotSelector{matcher: matcher, snapshot: snapshot}
}

func (s *snapshotSelector) Keep(ev *segment.Event) (bool, string) {
	if keep, reason := s.matcher.Keep(ev); !keep {
		return false, reason
	}
	if s.snapshot.suppresses(ev) {
		return false, "did_tombstone"
	}
	return true, ""
}

package oracle

import (
	"fmt"

	"github.com/bluesky-social/jetstream/segment"
)

// CheckFoldConvergence verifies eventual consistency (§R7). It folds the
// complete emitted stream in seq order, then filters the resulting records by
// collection and compares them with independently observed ground truth.
// Temporary stale rows are allowed if later markers remove them.
//
// Account-delete and sync markers match records by DID, not collection.
// Filtering after folding preserves those effects. A collection-filtered
// archive that omits a DID marker therefore fails this check; the sentinel
// index in segment/sentinel.go prevents that omission.
//
// emitted is the client's output. full must be an independent complete server
// stream, such as a direct segment scan. Passing filtered output as full
// would hide missing markers. Empty collections means no collection
// restriction.
func CheckFoldConvergence(emitted, full []ObservedEvent, collections []string) error {
	fullLive, err := groundTruthLive(full)
	if err != nil {
		return fmt.Errorf("oracle fold-convergence: ground-truth fold failed: %w", err)
	}
	emittedLive, err := groundTruthLive(emitted)
	if err != nil {
		return fmt.Errorf("oracle fold-convergence: emitted fold failed: %w", err)
	}
	want := restrictByCollection(fullLive, collections)
	got := restrictByCollection(emittedLive, collections)

	for key, gseq := range want {
		eseq, ok := got[key]
		if !ok {
			return fmt.Errorf("oracle fold-convergence: client stream folds to a MISSING record that ground truth keeps live: %v live_seq=%d (a folding consumer cannot converge — the killer was over-delivered or the create was lost)", key, gseq)
		}
		if eseq != gseq {
			return fmt.Errorf("oracle fold-convergence: client stream folds to a STALE version: %v emitted_seq=%d live_seq=%d", key, eseq, gseq)
		}
	}
	for key, eseq := range got {
		if _, ok := want[key]; !ok {
			return fmt.Errorf("oracle fold-convergence: client stream folds to a record that ground truth DELETED: %v emitted_seq=%d (the killer was never delivered — this is the §R3 DID-tombstone gap if the killer is a DID-level account-delete/sync)", key, eseq)
		}
	}
	return nil
}

// restrictByCollection drops every record whose collection is not selected by
// the filter. An empty filter selects everything. Applied AFTER the fold so
// DID-level killers (empty collection) have already purged their victims.
func restrictByCollection(live map[RecordKey]uint64, collections []string) map[RecordKey]uint64 {
	if len(collections) == 0 {
		return live
	}
	out := make(map[RecordKey]uint64, len(live))
	for key, seq := range live {
		if collectionSelected(key.Collection, collections) {
			out[key] = seq
		}
	}
	return out
}

// collectionSelected reports whether collection matches any filter entry. An
// entry ending in ".*" is a namespace wildcard ("app.bsky.feed.*" matches
// "app.bsky.feed."-prefixed NSIDs); otherwise it is an exact match. This mirrors
// the client matcher's collection semantics (root filter.go) so the
// oracle restriction lines up exactly with what the client delivers.
func collectionSelected(collection string, collections []string) bool {
	for _, c := range collections {
		if len(c) >= 2 && c[len(c)-1] == '*' && c[len(c)-2] == '.' {
			if hasPrefix(collection, c[:len(c)-1]) {
				return true
			}
			continue
		}
		if collection == c {
			return true
		}
	}
	return false
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// groundTruthLive folds the entire event stream into the set of records live at
// the end, mapping RecordKey -> latest create/update seq. A delete at a higher
// seq removes the record; an account-delete or sync at a higher seq removes
// every record for that DID (a DID-level tombstone, matched by DID — these
// carry an empty collection and so can never be matched by collection).
//
// This is the oracle's INDEPENDENT model of network truth: it folds an observed
// event stream directly and shares no code with the production tombstone
// package, so it cannot mask a bug in that package.
func groundTruthLive(events []ObservedEvent) (map[RecordKey]uint64, error) {
	type rec struct {
		seq  uint64
		live bool
	}
	latest := make(map[RecordKey]*rec)
	didKill := make(map[string]uint64) // did -> highest account-delete/sync seq
	for i := range events {
		ev := &events[i]
		switch ev.Kind {
		case segment.KindCreate, segment.KindUpdate, segment.KindCreateResync:
			key := RecordKey{DID: ev.DID, Collection: ev.Collection, Rkey: ev.Rkey}
			r := latest[key]
			if r == nil {
				r = &rec{}
				latest[key] = r
			}
			if ev.Seq >= r.seq {
				r.seq = ev.Seq
				r.live = true
			}
		case segment.KindDelete:
			key := RecordKey{DID: ev.DID, Collection: ev.Collection, Rkey: ev.Rkey}
			r := latest[key]
			if r == nil {
				r = &rec{}
				latest[key] = r
			}
			if ev.Seq >= r.seq {
				r.seq = ev.Seq
				r.live = false
			}
		case segment.KindSync:
			if ev.Seq > didKill[ev.DID] {
				didKill[ev.DID] = ev.Seq
			}
		case segment.KindAccount:
			// A malformed account payload must fail loud, not fold as
			// deleted=false: silently dropping a corrupt DID tombstone could
			// turn a real purge into a no-op and report a false-green
			// convergence. The sibling oracle paths (CheckCompacted,
			// Reconstruct) already treat this decode failure as fatal.
			deleted, err := oracleAccountDeleted(ev.Payload)
			if err != nil {
				return nil, fmt.Errorf("decode account payload at seq=%d did=%s: %w", ev.Seq, ev.DID, err)
			}
			if deleted && ev.Seq > didKill[ev.DID] {
				didKill[ev.DID] = ev.Seq
			}
		}
	}
	out := make(map[RecordKey]uint64)
	for key, r := range latest {
		if r.live && didKill[key.DID] <= r.seq {
			out[key] = r.seq
		}
	}
	return out, nil
}

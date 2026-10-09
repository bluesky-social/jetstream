package migrate

import (
	"bytes"
	"strings"

	"github.com/bluesky-social/jetstream/internal/catalog"
)

// keyClass is what the migration does with one Pebble metadata key
// (migration plan §6.6).
type keyClass uint8

const (
	// classUnknown is a key nobody classified. The migration refuses to
	// run on one: a new prefix needs a decision, not a default.
	classUnknown keyClass = iota
	// classCopy keys are copied as they are and kept in step.
	classCopy
	// classPhase is the lifecycle phase. It is withheld while seeding, so
	// a pod becomes ready only on a complete replica, and copied after.
	classPhase
	// classHandoff keys are copied only at handoff, once local ingest has
	// stopped: the relay cursor must never run ahead of the shipped events.
	classHandoff
	// classDrop keys are local-only, catalog-owned, or the migration's own.
	classDrop
)

func (c keyClass) String() string {
	switch c {
	case classCopy:
		return "copy"
	case classPhase:
		return "phase"
	case classHandoff:
		return "handoff"
	case classDrop:
		return "drop"
	default:
		return "unknown"
	}
}

// Prefixes and keys of the local metadata, from the code that writes them.
var (
	copyPrefixes = []string{
		"repo/", "handle/", "pdshost/", "host/",
		"backfill/timing/",
		"sync/chain/", "sync/host/", "sync/ident/", "sync/acct/",
	}
	copyKeys = []string{
		"backfill/counts", "phase/entered_at", "compaction/seq",
		// Retired backfill cursors: startup refuses a non-empty one in
		// either mode, so they copy as they are.
		"relay/list_repos_cursor", "bootstrap/last_listrepos_cursor",
	}
	dropPrefixes = []string{
		// The identity cache: disaggregated mode keeps its own in memory.
		// It shares sync/ with the sync state, so it is matched first.
		"sync/identity/",
		// The local vacancy registry; the catalog keeps seq/vacancies.
		"seq/gap/",
		// Bootstrap and merge leftovers, which steady state never reads.
		"merge/", "live_segments/",
		// The removed timestamp import.
		"import/",
		// The migration's own local state.
		"migration/",
	}
	dropKeys = []string{
		// Catalog-owned: the import scripts keep it in step with blocks.
		catalog.MainSeqKey,
		// The local seq lease, which the disaggregated writer has no use for.
		"seq/max_reserved",
	}
)

// classify returns key's class.
func classify(key []byte) keyClass {
	k := string(key)
	switch k {
	case "phase":
		return classPhase
	case catalog.RelayCursorKey:
		return classHandoff
	}
	for _, p := range dropPrefixes {
		if strings.HasPrefix(k, p) {
			return classDrop
		}
	}
	for _, d := range dropKeys {
		if k == d {
			return classDrop
		}
	}
	for _, p := range copyPrefixes {
		if strings.HasPrefix(k, p) {
			return classCopy
		}
	}
	for _, c := range copyKeys {
		if k == c {
			return classCopy
		}
	}
	return classUnknown
}

// kept reports whether keys of class c are kept in step with the source,
// given whether the catalog is past seeding.
func (c keyClass) kept(tailing bool) bool {
	return c == classCopy || (c == classPhase && tailing)
}

// prefixOf names key's prefix for the inventory: up to and including its
// first slash, or the whole key.
func prefixOf(key []byte) string {
	if i := bytes.IndexByte(key, '/'); i >= 0 {
		return string(key[:i+1])
	}
	return string(key)
}

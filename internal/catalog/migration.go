package catalog

import (
	"encoding/binary"
	"fmt"

	"github.com/bluesky-social/jetstream/internal/seqspace"
)

// Metadata keys of a catalog built by migrating a local archive
// (specs/notes/2026-10-09-local-to-disagg-migration.md).
const (
	// MigrationStateKey holds the migration's MigrationState. It is absent
	// on a catalog disaggregated mode built itself.
	MigrationStateKey = "migration/state"
	// MigrationHandoffSeqKey holds Main's seq/next at the handoff, written in
	// the transaction that sets MigrationDone.
	MigrationHandoffSeqKey = "migration/handoff_seq"
	// VacanciesKey holds Main's registered seq vacancies: seqs a local
	// archive skipped after an unclean restart (seqspace.Gaps). Only the
	// import scripts write it, and only above the imported frontier, so the
	// set is static once the migration is done. Disaggregated mode never
	// creates a vacancy of its own.
	VacanciesKey = "seq/vacancies"
)

// MigrationState is the value of MigrationStateKey.
type MigrationState string

const (
	// MigrationSeeding: the migrator is importing sealed segments and bulk
	// metadata. phase is not written yet, so no pod serves.
	MigrationSeeding MigrationState = "seeding"
	// MigrationTailing: the seed is done and phase is copied. Pods serve
	// the replica read-only while the migrator ships each new block, seal,
	// and metadata change.
	MigrationTailing MigrationState = "tailing"
	// MigrationHandingOff: the source is stopping local ingest to ship its
	// last blocks.
	MigrationHandingOff MigrationState = "handing_off"
	// MigrationDone: the handoff committed. A disaggregated leader may take
	// the lease; the source must never ingest again.
	MigrationDone MigrationState = "done"
	// MigrationAborted: the migration was abandoned before done. The
	// catalog is to be discarded; no pod may lead on it.
	MigrationAborted MigrationState = "aborted"
	// MigrationReverted: the emergency rollback after done moved the
	// archive back to the local source. No pod may lead on it again.
	MigrationReverted MigrationState = "reverted"
)

// ParseMigrationState parses a stored MigrationStateKey value.
func ParseMigrationState(v []byte) (MigrationState, error) {
	switch s := MigrationState(v); s {
	case MigrationSeeding, MigrationTailing, MigrationHandingOff, MigrationDone, MigrationAborted, MigrationReverted:
		return s, nil
	default:
		return "", fmt.Errorf("catalog: unknown migration state %q", v)
	}
}

// Importing reports whether the migrator may import segments, blocks, and
// metadata in state s.
func (s MigrationState) Importing() bool {
	return s == MigrationSeeding || s == MigrationTailing || s == MigrationHandingOff
}

// BlocksLeader reports whether a disaggregated leader must stand by in
// state s. The empty state (no migration) does not block; every state but
// done does.
func (s MigrationState) BlocksLeader() bool {
	return s != "" && s != MigrationDone
}

// ValidMigrationTransition reports whether SetMigrationState may move the
// state from from to to. The empty from is an absent key.
func ValidMigrationTransition(from, to MigrationState) bool {
	switch to {
	case MigrationSeeding:
		return from == ""
	case MigrationTailing:
		return from == MigrationSeeding || from == MigrationHandingOff
	case MigrationHandingOff:
		return from == MigrationTailing
	case MigrationDone:
		return from == MigrationHandingOff
	case MigrationAborted:
		return from == MigrationSeeding || from == MigrationTailing || from == MigrationHandingOff
	case MigrationReverted:
		return from == MigrationDone
	}
	return false
}

// vacanciesVersion is the VacanciesKey encoding: a version byte, a
// big-endian uint32 count, then each [start, end) as two big-endian uint64s.
const vacanciesVersion = 1

// maxVacancies bounds a decoded set. Each vacancy is one unclean restart of
// the source, so a real archive has a handful.
const maxVacancies = 1 << 16

// EncodeVacancies encodes g for VacanciesKey.
func EncodeVacancies(g *seqspace.Gaps) []byte {
	ranges := g.Ranges()
	out := make([]byte, 5, 5+16*len(ranges))
	out[0] = vacanciesVersion
	binary.BigEndian.PutUint32(out[1:5], uint32(len(ranges)))
	for _, r := range ranges {
		out = binary.BigEndian.AppendUint64(out, r.Start)
		out = binary.BigEndian.AppendUint64(out, r.End)
	}
	return out
}

// DecodeVacancies decodes a VacanciesKey value. An absent key is the empty
// set. The stored set must already be normalized: sorted, disjoint, and not
// adjacent, as EncodeVacancies writes it.
func DecodeVacancies(val []byte, found bool) (*seqspace.Gaps, error) {
	if !found {
		return seqspace.NewGaps(nil)
	}
	if len(val) < 5 || val[0] != vacanciesVersion {
		return nil, Corruptf(SourceMeta, "%s: bad header (%d bytes)", VacanciesKey, len(val))
	}
	n := binary.BigEndian.Uint32(val[1:5])
	if n > maxVacancies || len(val) != 5+16*int(n) {
		return nil, Corruptf(SourceMeta, "%s: %d vacancies in %d bytes", VacanciesKey, n, len(val))
	}
	ranges := make([]seqspace.Gap, n)
	for i := range ranges {
		off := 5 + 16*i
		ranges[i] = seqspace.Gap{
			Start: binary.BigEndian.Uint64(val[off:]),
			End:   binary.BigEndian.Uint64(val[off+8:]),
		}
		if i > 0 && ranges[i].Start <= ranges[i-1].End {
			return nil, Corruptf(SourceMeta, "%s: vacancy [%d,%d) is not after [%d,%d)", VacanciesKey,
				ranges[i].Start, ranges[i].End, ranges[i-1].Start, ranges[i-1].End)
		}
	}
	g, err := seqspace.NewGaps(ranges)
	if err != nil {
		return nil, Corruptf(SourceMeta, "%s: %v", VacanciesKey, err)
	}
	return g, nil
}

// Bridges reports whether a registered vacancy explains a jump from seq
// expect to seq next: one vacancy is exactly [expect, next). Vacancies are
// coalesced and never adjacent, so one is all a jump can take, and a
// vacancy holding expect-1 too would overlap whatever covered expect-1.
func Bridges(g *seqspace.Gaps, expect, next uint64) bool {
	end, ok := g.EndContaining(expect)
	if !ok || end != next {
		return false
	}
	_, before := g.EndContaining(expect - 1)
	return expect == 1 || !before
}

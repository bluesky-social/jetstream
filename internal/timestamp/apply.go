package timestamp

// apply.go is Phase C: turn a segment's offset file (produced by Phase B) into
// a segment.Patch mutate closure that stamps the imported display timestamp
// (indexed_at) onto the matching rows, applying the per-row scope rules from
// design §3.6 / §4a.
//
// The offset file holds byte offsets into the plain (seekable) import CSV.
// PatchPlan seeks each offset, re-reads and re-validates that single row, and
// folds it into a per-segment lookup keyed by (did, collection, rkey). Two
// target shapes hang off each key:
//
//   - allVersionsTS: the last-write-wins all_versions timestamp for the path
//     (patches every materialization row sharing the path).
//   - specific:      a CID->timestamp map; a materialization row is patched
//     only if ComputeCID(dag-cbor, payload) matches, and ALL rows with a
//     matching CID are patched (duplicate-CID rule, §4a).
//
// The mutate closure sets ONLY ev.IndexedAt and returns true iff it changed the
// value, so segment.Patch's guard (which rejects any other field change) and
// its zero-mutation skip both hold. Re-running an already-applied import is a
// no-op: the row already carries the target value, so mutate returns false.

import (
	"encoding/binary"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/cbor"
)

// RowReader re-reads individual rows from the plain import CSV by byte offset.
// It is safe for concurrent use: each ReadRow issues a positioned read through
// an io.SectionReader over a shared *os.File, so the orchestrator's per-segment
// workers can each hold their own reader without seeking a shared cursor.
// Construct with OpenRowReader; Close when the job's apply phase ends.
//
// The header column mapping is parsed once at open time and reused for every
// positioned read. This is load-bearing: a data-row offset carries no header,
// and the operator may have written columns in a non-canonical order, so
// column meaning must come from the file's own header rather than a positional
// assumption.
type RowReader struct {
	f    *os.File
	size int64
	cols columns
}

// OpenRowReader opens the plain import CSV at path for positioned row reads,
// parsing its header to establish the column mapping. It returns the same
// header errors as Parse (ErrHeader) so a malformed header is caught here too.
func OpenRowReader(path string) (*RowReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("timestamp: open import csv: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("timestamp: stat import csv: %w", err)
	}
	// Parse the header from the start of the file to fix the column mapping.
	hr := csv.NewReader(io.NewSectionReader(f, 0, info.Size()))
	header, err := hr.Read()
	if err != nil {
		_ = f.Close()
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("%w: file is empty (no header row)", ErrHeader)
		}
		return nil, fmt.Errorf("%w: %w", ErrHeader, err)
	}
	cols, err := parseHeader(header)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &RowReader{f: f, size: info.Size(), cols: cols}, nil
}

// Close releases the underlying file.
func (rr *RowReader) Close() error { return rr.f.Close() }

// maxRowScanBytes bounds how far ReadRow reads from an offset while looking for
// the row's terminating newline. A single import row is a URI + RFC3339 stamp +
// short scope + CID -- well under 1 KiB; this ceiling stops a corrupt offset
// (or a truncated file with no newline) from reading the whole file into RAM.
const maxRowScanBytes = 64 * 1024

// ReadRow reads and re-validates the CSV row that begins at off. It re-runs the
// same validation Phase A applied, so a row that no longer validates (the file
// changed underneath the offsets, or the offset is corrupt) is rejected rather
// than silently misapplied. The returned Row carries the same Offset.
//
// Re-validation here is deliberate defense in depth, not redundant work: the
// offset file is an untrusted-across-restart artifact (a resumed job may read
// offsets written by a prior process against a file an operator could have
// swapped), so Phase C independently re-derives the row's meaning rather than
// trusting Phase B's classification.
func (rr *RowReader) ReadRow(off int64) (Row, error) {
	if off < 0 || off >= rr.size {
		return Row{}, fmt.Errorf("%w: offset %d out of range [0,%d)", ErrCorruptOffset, off, rr.size)
	}
	span := min(rr.size-off, maxRowScanBytes)
	sr := io.NewSectionReader(rr.f, off, span)
	row, ok, err := parseOneRow(sr, off, rr.cols)
	if err != nil {
		return Row{}, err
	}
	if !ok {
		return Row{}, fmt.Errorf("%w: no valid row at offset %d", ErrCorruptOffset, off)
	}
	return row, nil
}

// ErrCorruptOffset indicates an offset that does not point at a re-parseable,
// re-valid row. It is surfaced (not silently skipped) because it means the
// import CSV and the offset file have desynced -- a condition the operator must
// see rather than have quietly drop patches.
var ErrCorruptOffset = errors.New("timestamp: corrupt import offset")

// target is the resolved import instruction(s) for one (did, collection, rkey)
// path within a single segment.
type target struct {
	// allVersionsTS is the all_versions timestamp for this path, or 0 if no
	// all_versions row targeted it. Last-write-wins across the offset file.
	allVersionsTS int64
	// specific maps a specific_version CID to its timestamp. nil until the
	// first specific_version row for this path. Last-write-wins per CID.
	specific map[cbor.CID]int64
}

// PatchPlan is the resolved per-segment lookup: path -> target. It is built
// once per segment from that segment's offset file, then consulted by the
// mutate closure for every decoded row.
type PatchPlan struct {
	byPath map[pathKey]*target

	// counts for job status / tests.
	rowsPlanned      uint64 // offset entries folded in
	rowsCorrupt      uint64 // offsets that failed re-validation
	pathsAllVersions int
	pathsSpecific    int
}

type pathKey struct {
	did        string
	collection string
	rkey       string
}

// PlanStats reports what BuildPatchPlan folded in.
type PlanStats struct {
	RowsPlanned      uint64
	RowsCorrupt      uint64
	DistinctPaths    int
	PathsAllVersions int
	PathsSpecific    int
}

// Stats returns a snapshot of the plan's fold counters.
func (p *PatchPlan) Stats() PlanStats {
	return PlanStats{
		RowsPlanned:      p.rowsPlanned,
		RowsCorrupt:      p.rowsCorrupt,
		DistinctPaths:    len(p.byPath),
		PathsAllVersions: p.pathsAllVersions,
		PathsSpecific:    p.pathsSpecific,
	}
}

// BuildPatchPlan reads segment's offset file and resolves every offset into the
// per-path lookup by seeking+revalidating each row through rr. A corrupt offset
// increments RowsCorrupt and is skipped (the desync is surfaced via the count
// and, in M6, the job status) rather than aborting the whole segment. An I/O
// error reading the offset file itself is fatal (returned).
//
// offsetFilePath is the packed-uint64 file Phase B wrote (OffsetFileName).
func BuildPatchPlan(offsetFilePath string, rr *RowReader) (*PatchPlan, error) {
	f, err := os.Open(offsetFilePath)
	if err != nil {
		return nil, fmt.Errorf("timestamp: open offset file: %w", err)
	}
	defer func() { _ = f.Close() }()

	plan := &PatchPlan{byPath: make(map[pathKey]*target)}
	buf := make([]byte, offsetRecordSize)
	for {
		if _, err := io.ReadFull(f, buf); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				// A partial trailing record means the offset file was torn
				// (a crash mid-append). Surface it: the file is not a whole
				// number of uint64s, which should never happen under the
				// O_APPEND writer, so refuse to silently ignore the tail.
				return nil, fmt.Errorf("%w: offset file %s has a partial trailing record",
					ErrCorruptOffset, offsetFilePath)
			}
			return nil, fmt.Errorf("timestamp: read offset file: %w", err)
		}
		off := int64(binary.LittleEndian.Uint64(buf))
		row, err := rr.ReadRow(off)
		if err != nil {
			if errors.Is(err, ErrCorruptOffset) {
				plan.rowsCorrupt++
				continue
			}
			return nil, err
		}
		plan.add(row)
	}
	return plan, nil
}

// add folds one validated row into the plan, applying last-write-wins.
func (p *PatchPlan) add(row Row) {
	p.rowsPlanned++
	key := pathKey{did: row.DID, collection: row.Collection, rkey: row.Rkey}
	t := p.byPath[key]
	if t == nil {
		t = &target{}
		p.byPath[key] = t
	}
	switch row.Scope {
	case ScopeAllVersions:
		if t.allVersionsTS == 0 {
			p.pathsAllVersions++
		}
		t.allVersionsTS = row.TimestampMicros // last-write-wins
	case ScopeSpecificVersion:
		if t.specific == nil {
			t.specific = make(map[cbor.CID]int64)
			p.pathsSpecific++
		}
		t.specific[row.CID] = row.TimestampMicros // last-write-wins per CID
	}
}

// ApplyStats reports what the mutate closure did across one segment.Patch.
type ApplyStats struct {
	// RowsMatchedAllVersions is materialization rows patched by an
	// all_versions target.
	RowsMatchedAllVersions uint64
	// RowsMatchedSpecific is materialization rows patched by a
	// specific_version CID match.
	RowsMatchedSpecific uint64
	// SpecificCIDsUnmatched counts (path, CID) specific_version targets that
	// no materialization row in the segment matched -- reported, not an error
	// (§4a: the version was compacted away or never witnessed here).
	SpecificCIDsUnmatched uint64
}

// Mutate is the segment.Patch closure plus the stats it accumulates. Build one
// per segment.Patch call via (*PatchPlan).BuildMutate; read Stats after Patch
// returns.
type Mutate struct {
	fn    func(*segment.Event) bool
	stats ApplyStats

	// matchedSpecific tracks which (path,CID) specific targets were hit, so
	// unmatched ones can be counted after the pass. Keyed to the plan's
	// targets by pointer identity + CID.
	matchedSpecific map[specificKey]struct{}
	plan            *PatchPlan
}

type specificKey struct {
	path pathKey
	cid  cbor.CID
}

// Fn returns the closure to hand to segment.Patch.
func (m *Mutate) Fn() func(*segment.Event) bool { return m.fn }

// Stats returns the apply counters. Call after segment.Patch returns; it
// finalizes SpecificCIDsUnmatched by diffing planned specific targets against
// the ones matched during the pass.
func (m *Mutate) Stats() ApplyStats {
	s := m.stats
	var unmatched uint64
	for key, t := range m.plan.byPath {
		for cid := range t.specific {
			if _, ok := m.matchedSpecific[specificKey{path: key, cid: cid}]; !ok {
				unmatched++
			}
		}
	}
	s.SpecificCIDsUnmatched = unmatched
	return s
}

// BuildMutate returns a Mutate whose closure applies this plan's targets to a
// segment's rows under segment.Patch. The closure:
//
//   - only considers materialization rows (Create/Update/CreateResync); Delete
//     rows have no payload and Identity/Account/Sync have no (collection,rkey),
//     so all correctly skip (§4a);
//   - resolves the row's imported timestamp with specific_version taking
//     precedence over all_versions when both target the row's path and the
//     CID matches (a specific import is the operator's more precise
//     instruction);
//   - sets ONLY ev.IndexedAt and returns true iff it changed the stored value,
//     preserving segment.Patch's guard and zero-mutation-skip idempotency.
func (p *PatchPlan) BuildMutate() *Mutate {
	m := &Mutate{
		matchedSpecific: make(map[specificKey]struct{}),
		plan:            p,
	}
	m.fn = func(ev *segment.Event) bool {
		if !ev.Kind.IsMaterialization() {
			return false
		}
		key := pathKey{did: ev.DID, collection: ev.Collection, rkey: ev.Rkey}
		t := p.byPath[key]
		if t == nil {
			return false
		}

		ts, matchedSpecific := resolveTimestamp(t, ev, m)
		if ts == 0 {
			return false
		}
		if ev.IndexedAt == ts {
			return false // already at target -> idempotent no-op
		}
		ev.IndexedAt = ts
		if matchedSpecific {
			m.stats.RowsMatchedSpecific++
		} else {
			m.stats.RowsMatchedAllVersions++
		}
		return true
	}
	return m
}

// resolveTimestamp picks the imported timestamp for a materialization row.
// specific_version wins over all_versions when the row's payload CID matches a
// specific target; otherwise all_versions applies if present. Returns (0,_)
// when nothing targets the row. It records specific-CID matches on m so
// unmatched specifics can be counted, and it patches ALL rows whose CID matches
// (the duplicate-CID rule, §4a) by virtue of being called per row.
func resolveTimestamp(t *target, ev *segment.Event, m *Mutate) (ts int64, matchedSpecific bool) {
	if len(t.specific) > 0 {
		got := cbor.ComputeCID(cbor.CodecDagCBOR, ev.Payload)
		if sts, ok := t.specific[got]; ok {
			key := pathKey{did: ev.DID, collection: ev.Collection, rkey: ev.Rkey}
			m.matchedSpecific[specificKey{path: key, cid: got}] = struct{}{}
			return sts, true
		}
	}
	if t.allVersionsTS != 0 {
		return t.allVersionsTS, false
	}
	return 0, false
}

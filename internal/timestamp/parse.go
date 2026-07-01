// Package timestamp implements the operator timestamp-import pipeline
// (docs/README.md §8, design specs/notes/2026-07-01-timestamp-import-design.md).
//
// This file is Phase A: streaming parse + validation of the operator's import
// file. The input is a plain (uncompressed) RFC4180 CSV with a header row and
// columns uri,timestamp,scope,cid. Plain rather than zstd so it is randomly
// seekable: Phase B records each valid row's byte offset, and Phase C reopens
// the file and reads a single row back by Seek+decode, with no decompression
// subsystem and no scratch copy (Q-FORMAT, revised — see the design doc).
//
// Validation happens at this durable boundary and follows the #188 lesson:
// reject malformed input at the edge so it cannot wedge a later pass. Bad rows
// are skipped and reported (counts by reason + a bounded sample), never
// aborting the whole file (Q-REJECT); a billion-row file must not die on one
// typo. Structural header problems (a missing required column, a duplicate or
// unrecognized column) are different: they make every row ambiguous, so they
// fail the whole file loudly rather than silently mis-mapping columns.
package timestamp

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
)

// Scope selects which stored record version(s) an imported timestamp applies
// to (design §3.6). The zero value is ScopeAllVersions, matching the CSV
// default for an empty/absent scope field.
type Scope uint8

const (
	// ScopeAllVersions patches every materialization row sharing the row's
	// (did, collection, rkey). It is the default.
	ScopeAllVersions Scope = 0

	// ScopeSpecificVersion patches only the row whose stored payload
	// recomputes to the row's CID. Requires a parseable cid (§4a).
	ScopeSpecificVersion Scope = 1
)

// scopeAllVersions / scopeSpecificVersion are the on-the-wire CSV tokens.
const (
	scopeTokenAllVersions     = "all_versions"
	scopeTokenSpecificVersion = "specific_version"
)

func (s Scope) String() string {
	switch s {
	case ScopeAllVersions:
		return scopeTokenAllVersions
	case ScopeSpecificVersion:
		return scopeTokenSpecificVersion
	default:
		return fmt.Sprintf("Scope(%d)", uint8(s))
	}
}

// Row is one validated import instruction. All string fields are non-empty and
// already syntactically valid; CID is Defined() iff Scope == ScopeSpecificVersion.
type Row struct {
	// Offset is the byte offset of this row's first byte within the source
	// CSV. Phase C reopens the file, Seeks here, and reads exactly this row
	// back. Valid only for the exact file that produced it.
	Offset int64

	// DID is the repo identifier extracted from the AT URI authority. Import
	// is keyed by DID so the segment-level DID bloom does the coarse routing
	// (§3.5); a URI whose authority is a handle, not a DID, is rejected.
	DID string

	// Collection and Rkey are the record path within the repo.
	Collection string
	Rkey       string

	Scope Scope

	// CID is the content identifier for ScopeSpecificVersion rows, parsed once
	// here so Phase C stays off the string-parse path (§4a). Zero (undefined)
	// for ScopeAllVersions.
	CID cbor.CID

	// TimestampMicros is the imported display timestamp in unix microseconds.
	// Always > 0 (0 is the "not imported" sentinel, so a row resolving to 0 is
	// rejected rather than silently indistinguishable from un-imported).
	TimestampMicros int64
}

// RejectReason enumerates why a row was skipped. Stable strings so they can be
// surfaced in job status counts-by-reason and the durable rejects artifact.
type RejectReason string

const (
	ReasonMalformedCSV    RejectReason = "malformed_csv"
	ReasonMissingField    RejectReason = "missing_field"
	ReasonBadURI          RejectReason = "bad_uri"
	ReasonURINotDID       RejectReason = "uri_authority_not_did"
	ReasonURIIncomplete   RejectReason = "uri_missing_collection_or_rkey"
	ReasonBadTimestamp    RejectReason = "bad_timestamp"
	ReasonNonPositiveTime RejectReason = "non_positive_timestamp"
	ReasonUnknownScope    RejectReason = "unknown_scope"
	ReasonMissingCID      RejectReason = "specific_version_missing_cid"
	ReasonBadCID          RejectReason = "bad_cid"
)

// Reject describes one skipped row for reporting. RowNumber is 1-based over
// data rows (the header is not counted). Field/Value carry a bounded diagnostic
// snippet of the offending input; they are truncated to keep the durable
// rejects artifact and status payload bounded even for adversarial input.
type Reject struct {
	RowNumber uint64
	Offset    int64
	Reason    RejectReason
	Field     string
	Value     string
}

// DefaultRejectSampleLimit bounds how many Reject records Parse retains in
// Stats.RejectSample. Counts-by-reason are always complete; only the sample is
// capped so a mostly-malformed (or adversarial) input cannot grow the in-memory
// status payload without bound (Q-REJECT).
const DefaultRejectSampleLimit = 100

// maxDiagnosticValueLen bounds a single reject's captured Field/Value bytes.
const maxDiagnosticValueLen = 256

// Stats is the parse outcome. RowsTotal == RowsValid + RowsRejected.
type Stats struct {
	RowsTotal       uint64
	RowsValid       uint64
	RowsRejected    uint64
	RejectsByReason map[RejectReason]uint64
	RejectSample    []Reject
}

// Options configures Parse. OnRow is required.
type Options struct {
	// OnRow is called once per valid row, in file order. Returning an error
	// aborts the parse and Parse returns that error wrapped. Phase B supplies
	// the bucketer here so parse and routing share a single streaming pass.
	OnRow func(Row) error

	// OnReject, if non-nil, is called once per skipped row, in file order,
	// before the reject is folded into Stats. M6 wires the durable
	// rejects-artifact writer here; the bounded in-memory sample is
	// independent of it.
	OnReject func(Reject)

	// RejectSampleLimit overrides DefaultRejectSampleLimit when > 0. A
	// negative value disables the in-memory sample entirely (counts still
	// accrue; OnReject still fires).
	RejectSampleLimit int
}

// ErrHeader is returned for structural header problems that make the whole file
// unparseable (missing required column, duplicate or unrecognized column). It
// is distinct from per-row rejects: a bad header is fail-the-file, not skip.
var ErrHeader = errors.New("timestamp: invalid CSV header")

// Parse streams src as an import CSV, invoking opts.OnRow for every valid row
// and reporting skipped rows via opts.OnReject and the returned Stats. It never
// aborts on a bad data row; it aborts (returning a wrapped error) only on a
// structural header error, an OnRow error, or an unrecoverable read error.
//
// src is read exactly once, forward-only, so it is safe for tens of GB. The
// byte offsets in Row.Offset are offsets into src, so callers that want Phase C
// to seek them back must pass the same file positioned at its start.
func Parse(src io.Reader, opts Options) (Stats, error) {
	if opts.OnRow == nil {
		return Stats{}, fmt.Errorf("timestamp: Parse requires OnRow")
	}
	sampleLimit := opts.RejectSampleLimit
	if sampleLimit == 0 {
		sampleLimit = DefaultRejectSampleLimit
	}

	r := csv.NewReader(src)
	r.ReuseRecord = true // each string element is freshly allocated; safe to keep
	// FieldsPerRecord defaults to 0: csv sets it to the header's field count
	// after the first Read and enforces it thereafter, surfacing a
	// wrong-field-count row as a recoverable ErrFieldCount we turn into a
	// reject rather than a fatal error.

	header, err := r.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return Stats{}, fmt.Errorf("%w: file is empty (no header row)", ErrHeader)
		}
		return Stats{}, fmt.Errorf("%w: %w", ErrHeader, err)
	}
	cols, err := parseHeader(header)
	if err != nil {
		return Stats{}, err
	}

	stats := Stats{RejectsByReason: make(map[RejectReason]uint64)}
	reject := func(rowNum uint64, offset int64, reason RejectReason, field, value string) {
		stats.RowsTotal++
		stats.RowsRejected++
		stats.RejectsByReason[reason]++
		rj := Reject{
			RowNumber: rowNum,
			Offset:    offset,
			Reason:    reason,
			Field:     field,
			Value:     truncate(value),
		}
		if opts.OnReject != nil {
			opts.OnReject(rj)
		}
		if sampleLimit > 0 && len(stats.RejectSample) < sampleLimit {
			stats.RejectSample = append(stats.RejectSample, rj)
		}
	}

	var rowNum uint64
	for {
		// InputOffset before Read yields the start of the row about to be
		// read (the end of the previously read row), which is exactly the
		// Seek target Phase C needs.
		offset := r.InputOffset()
		rec, readErr := r.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		rowNum++
		if readErr != nil {
			// A wrong-field-count or malformed-quote row: csv reports it and
			// can continue with the next line. Skip-and-report, don't abort.
			if errors.Is(readErr, csv.ErrFieldCount) {
				reject(rowNum, offset, ReasonMissingField, "", strings.Join(rec, ","))
				continue
			}
			if _, ok := errors.AsType[*csv.ParseError](readErr); ok {
				reject(rowNum, offset, ReasonMalformedCSV, "", readErr.Error())
				continue
			}
			// A non-CSV read error (I/O) is unrecoverable.
			return stats, fmt.Errorf("timestamp: read row %d: %w", rowNum, readErr)
		}

		row, reason, field, value := cols.validate(rec, offset)
		if reason != "" {
			reject(rowNum, offset, reason, field, value)
			continue
		}
		stats.RowsTotal++
		stats.RowsValid++
		if err := opts.OnRow(row); err != nil {
			return stats, fmt.Errorf("timestamp: handling row %d: %w", rowNum, err)
		}
	}

	return stats, nil
}

// columns maps the recognized header names to their record indices. An index
// of -1 means the (optional) column is absent.
type columns struct {
	uri       int
	timestamp int
	scope     int
	cid       int
}

func parseHeader(header []string) (columns, error) {
	cols := columns{uri: -1, timestamp: -1, scope: -1, cid: -1}
	seen := make(map[string]struct{}, len(header))
	for i, raw := range header {
		name := strings.TrimSpace(strings.ToLower(raw))
		if _, dup := seen[name]; dup {
			return columns{}, fmt.Errorf("%w: duplicate column %q", ErrHeader, name)
		}
		seen[name] = struct{}{}
		switch name {
		case "uri":
			cols.uri = i
		case "timestamp":
			cols.timestamp = i
		case "scope":
			cols.scope = i
		case "cid":
			cols.cid = i
		default:
			// Strict header: an unrecognized column is almost always a typo
			// of a real one (e.g. "timestmp"). Fail loudly rather than treat
			// the intended column as absent and silently mis-default rows.
			return columns{}, fmt.Errorf("%w: unrecognized column %q (allowed: uri,timestamp,scope,cid)", ErrHeader, name)
		}
	}
	if cols.uri < 0 {
		return columns{}, fmt.Errorf("%w: required column \"uri\" is missing", ErrHeader)
	}
	if cols.timestamp < 0 {
		return columns{}, fmt.Errorf("%w: required column \"timestamp\" is missing", ErrHeader)
	}
	return cols, nil
}

// validate turns one CSV record into a Row or a rejection. On success it
// returns (row, "", "", ""); on failure it returns (zero, reason, field,
// offending-value) so the caller can build a Reject. offset is threaded onto
// the Row for Phase C.
func (c columns) validate(rec []string, offset int64) (Row, RejectReason, string, string) {
	// The header set FieldsPerRecord, so rec has header-length fields; but a
	// column index could still exceed len(rec) if the header itself was
	// shorter than a referenced optional index (it can't, indices come from
	// the header) -- guard defensively anyway.
	get := func(idx int) (string, bool) {
		if idx < 0 || idx >= len(rec) {
			return "", false
		}
		return strings.TrimSpace(rec[idx]), true
	}

	uriStr, ok := get(c.uri)
	if !ok || uriStr == "" {
		return Row{}, ReasonMissingField, "uri", ""
	}
	tsStr, ok := get(c.timestamp)
	if !ok || tsStr == "" {
		return Row{}, ReasonMissingField, "timestamp", ""
	}

	aturi, err := atmos.ParseATURI(uriStr)
	if err != nil {
		return Row{}, ReasonBadURI, "uri", uriStr
	}
	authority := aturi.Authority()
	if !authority.IsDID() {
		return Row{}, ReasonURINotDID, "uri", uriStr
	}
	did := authority.DID().String()
	collection := aturi.Collection().String()
	rkey := aturi.RecordKey().String()
	if collection == "" || rkey == "" {
		return Row{}, ReasonURIIncomplete, "uri", uriStr
	}

	t, err := time.Parse(time.RFC3339, tsStr)
	if err != nil {
		return Row{}, ReasonBadTimestamp, "timestamp", tsStr
	}
	micros := t.UnixMicro()
	if micros <= 0 {
		// 0 is the un-imported sentinel; a non-positive timestamp cannot be
		// stored without becoming indistinguishable from "not imported".
		return Row{}, ReasonNonPositiveTime, "timestamp", tsStr
	}

	scope := ScopeAllVersions
	if scopeStr, present := get(c.scope); present && scopeStr != "" {
		switch scopeStr {
		case scopeTokenAllVersions:
			scope = ScopeAllVersions
		case scopeTokenSpecificVersion:
			scope = ScopeSpecificVersion
		default:
			return Row{}, ReasonUnknownScope, "scope", scopeStr
		}
	}

	row := Row{
		Offset:          offset,
		DID:             did,
		Collection:      collection,
		Rkey:            rkey,
		Scope:           scope,
		TimestampMicros: micros,
	}

	if scope == ScopeSpecificVersion {
		cidStr, present := get(c.cid)
		if !present || cidStr == "" {
			return Row{}, ReasonMissingCID, "cid", ""
		}
		cid, err := cbor.ParseCIDString(cidStr)
		if err != nil {
			return Row{}, ReasonBadCID, "cid", cidStr
		}
		row.CID = cid
	}
	// A cid supplied with all_versions is ignored per §4 D.

	return row, "", "", ""
}

// parseOneRow reads a single CSV record from src and validates it against
// cols, returning (row, true, nil) on success and (zero, false, nil) when the
// record is unreadable or fails validation. A non-CSV read error is returned.
// It is the shared kernel Phase C's positioned RowReader uses to re-derive a
// row's meaning from an offset without trusting Phase B's classification.
func parseOneRow(src io.Reader, offset int64, cols columns) (Row, bool, error) {
	r := csv.NewReader(src)
	// A positioned read starts mid-file with no header, so the field count is
	// unknown; disable the fixed-count check and let validate index defensively.
	r.FieldsPerRecord = -1
	rec, err := r.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return Row{}, false, nil
		}
		if _, ok := errors.AsType[*csv.ParseError](err); ok {
			return Row{}, false, nil
		}
		return Row{}, false, fmt.Errorf("timestamp: read row at offset %d: %w", offset, err)
	}
	row, reason, _, _ := cols.validate(rec, offset)
	if reason != "" {
		return Row{}, false, nil
	}
	return row, true, nil
}

func truncate(s string) string {
	if len(s) <= maxDiagnosticValueLen {
		return s
	}
	return s[:maxDiagnosticValueLen] + "…"
}

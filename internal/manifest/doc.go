// Package manifest indexes sealed segment metadata in memory for archive
// planning and cursor lookup. Open scans the segment directory after
// steady-state startup. Later seals and compaction rewrites arrive through
// ApplySegment as header and footer bytes, so the manifest never needs the
// block region of a segment.
//
// Active segments have no finalized header and are read directly through the
// ingest writer. See docs/README.md §3.5.
package manifest

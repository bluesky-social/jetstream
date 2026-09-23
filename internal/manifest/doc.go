// Package manifest indexes sealed segment metadata in memory for archive
// planning and cursor lookup. Open scans the segment directory after
// steady-state startup. The writer publishes later seals through
// OnSegmentSealed; rewrites refresh affected entries.
//
// Active segments have no finalized header and are read directly through the
// ingest writer. See docs/README.md §3.5.
package manifest

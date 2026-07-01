// Package manifest materializes DESIGN.md §3.5's "segment manifest"
// concept in memory: an authoritative slice of bounds (MinSeq, MaxSeq,
// MinWitnessedAt, MaxWitnessedAt) for every sealed segment, plus a small
// LRU of per-segment block indices for callers that need to seek
// inside a segment.
//
// Open is called once at process startup, after the orchestrator
// declares steady-state, and walks the segments directory once to
// hydrate the bounds slice. The ingest writer publishes new sealed
// segments via OnSegmentSealed (wired through Config.OnAfterSeal in
// internal/ingest). The manifest is the data dependency that lets
// the cursor resolver in internal/subscribe answer "which segment
// holds seq N?" or "which segment holds events at time_us T?" without
// touching disk on the hot path.
//
// Active (unsealed) segments are intentionally not tracked. They have
// no fixed header to read, and the cursor replay engine handles them
// directly via the ingest writer.
package manifest

// Package status gathers the data shown by the public /status page.
//
// The package exposes one type per logical group of stats (see
// snapshot.go) and a Collector that builds a Snapshot on demand,
// caches it for a configurable TTL, and uses singleflight to collapse
// concurrent cold-cache requests into a single backend call.
//
// status is rendering-agnostic: the internal/web package consumes
// Snapshot via a Snapshotter interface. A future JSON or Prometheus
// surface would consume the same Snapshot.
//
// All cost-bearing reads (pebble range scans, segment file walks)
// happen in build paths, never in the cache-hit path. A warm cache
// answers in microseconds.
package status

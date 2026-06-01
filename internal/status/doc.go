// Package status gathers the data shown by the public /status page.
//
// The package exposes one type per logical group of stats (see
// snapshot.go) and a Collector that builds a Snapshot on demand,
// using singleflight to collapse concurrent requests into a single
// backend call.
//
// status is rendering-agnostic: the internal/web package consumes
// Snapshot via a Snapshotter interface. A future JSON or Prometheus
// surface would consume the same Snapshot.
//
// Production wiring uses the in-memory manifest so build paths stay
// cheap after manifest warmup.
package status

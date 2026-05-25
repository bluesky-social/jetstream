// Package web renders the public /status page from a status.Snapshot.
//
// The handler exposes a single route. Snapshots are produced by an
// injected Snapshotter (typically *status.Collector) so the rendering
// path stays decoupled from data gathering.
//
// Templates are embedded at build time; CSS lives inside the template
// in a <style> block. No JavaScript, no external assets.
package web

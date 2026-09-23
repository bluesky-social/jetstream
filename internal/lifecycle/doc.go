// Package lifecycle stores phase and related timing metadata in pebble. The
// persisted phase selects bootstrap, merge, or steady-state startup. This
// keyspace belongs here; internal/store manages only database access and
// lifecycle.
package lifecycle

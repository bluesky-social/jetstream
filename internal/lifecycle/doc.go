// Package lifecycle stores phase and related timing metadata in the metadata
// store. The persisted phase selects bootstrap, merge, or steady-state
// startup. This keyspace belongs here; internal/metastore manages only
// database access.
package lifecycle

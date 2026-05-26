// Package world owns the simulator's on-disk state: the pebble db, the
// global RNG, the deterministic account roster, and the live commit
// generator. The HTTP layer reads through *World; only the traffic
// goroutine writes to pebble after bootstrap.
package world

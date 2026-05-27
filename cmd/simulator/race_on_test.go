//go:build race

package main

// raceEnabled reports whether this test binary was built with -race.
// Used by buildJetstreamForTest to propagate -race to the spawned
// `go build` so it can reuse the race-mode compile cache the parent
// `go test -race` already populated. Without this, the spawned build
// has to recompile every dependency in non-race mode from a cold
// cache (CI runs with caching disabled by policy), which blows past
// the build timeout under CPU contention from parallel tests.
const raceEnabled = true

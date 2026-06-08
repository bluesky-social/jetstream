// Package crashpoint defines named test-only crash checkpoints at durable
// lifecycle boundaries. Production code wires a nil Injector, so checkpoints
// are no-ops outside deterministic oracle/recovery tests.
package crashpoint

import (
	"context"
	"fmt"
)

// Point identifies a deterministic crash simulation checkpoint.
type Point string

// Injector simulates a crash when a configured Point is reached. Implementations
// are test harness concerns; production code should pass nil.
type Injector interface {
	SimulateCrash(context.Context, Point) error
}

const (
	// AfterRepoComplete fires after a repo completion row is durable. Recovery
	// must skip the completed repo without losing its already-flushed segment rows.
	AfterRepoComplete Point = "after-repo-complete"

	// AfterMergeDstFlushBeforeSourceCommit fires after a merge source segment's
	// survivors are fsynced to the destination but before the source cursor is
	// advanced. Recovery may replay duplicates, but must not lose survivors.
	AfterMergeDstFlushBeforeSourceCommit Point = "after-merge-dst-flush-before-source-commit"

	// AfterMergeDstSealBeforeDiscovery fires after merge destination sealing but
	// before discovery and cleanup. Recovery must rerun discovery and remove
	// bootstrap artifacts idempotently.
	AfterMergeDstSealBeforeDiscovery Point = "after-merge-dst-seal-before-discovery"

	// AfterBootstrapLiveCloseBeforeSeal fires after bootstrap-live Close flushes
	// data and cursor state but before its active segment is sealed for merge.
	// Recovery must not assume the source tree is already fully sealed.
	AfterBootstrapLiveCloseBeforeSeal Point = "after-bootstrap-live-close-before-seal"

	// AfterSteadyPhaseBeforeSteadyRun fires after phase=steady_state is durable
	// but before the steady-state live consumer starts. Recovery must dispatch
	// directly to steady-state without rerunning bootstrap or merge.
	AfterSteadyPhaseBeforeSteadyRun Point = "after-steady-phase-before-steady-run"
)

// AllPoints is the single source of truth for the set of declared
// crashpoints. knownPoints (used by Known/Parse) is derived from it, so
// adding a constant here automatically makes it parseable — there is no
// second map to keep in sync. A test asserts every constant is listed.
var AllPoints = []Point{
	AfterRepoComplete,
	AfterMergeDstFlushBeforeSourceCommit,
	AfterMergeDstSealBeforeDiscovery,
	AfterBootstrapLiveCloseBeforeSeal,
	AfterSteadyPhaseBeforeSteadyRun,
}

var knownPoints = func() map[Point]struct{} {
	m := make(map[Point]struct{}, len(AllPoints))
	for _, p := range AllPoints {
		m[p] = struct{}{}
	}
	return m
}()

// String returns the stable environment/test name for p.
func (p Point) String() string {
	return string(p)
}

// Known reports whether p is one of the declared crashpoints.
func Known(p Point) bool {
	_, ok := knownPoints[p]
	return ok
}

// Parse converts a stable crashpoint name into a typed Point.
func Parse(s string) (Point, error) {
	if s == "" {
		return "", fmt.Errorf("crashpoint: empty crashpoint")
	}
	p := Point(s)
	if !Known(p) {
		return "", fmt.Errorf("crashpoint: unknown crashpoint %q", s)
	}
	return p, nil
}

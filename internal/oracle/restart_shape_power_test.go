package oracle

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// dropRowsMatching returns a copy of rows with every row whose kind and
// (collection, rkey) match removed. It models the lost-intermediate
// failure mode: the system dropped a durable row it must have kept. The
// per-shape power tests run it over the REAL recovered disk rows, then
// assert coverage fails — tying the failure power to the actual fixture
// rather than a synthetic one (the unit-level red check lives in
// restart_coverage_unit_test.go).
func dropRowsMatching(rows []EventLogRow, kind, coll, rkey string) []EventLogRow {
	out := make([]EventLogRow, 0, len(rows))
	for _, r := range rows {
		if r.Kind == kind && r.Collection == coll && r.Rkey == rkey {
			continue
		}
		out = append(out, r)
	}
	return out
}

// assertCoverageFailsWithoutRow is the red-first power check for one
// shape: it confirms (a) the shape's signature durable row is actually
// present in the recovered coverage view (anti-vacuity — the fixture
// landed), and (b) removing that row from the observed set makes
// CompareEventLogCoverage fail. If the row never landed, the fixture is
// broken and the test fails loudly rather than passing vacuously.
func assertCoverageFailsWithoutRow(t *testing.T, cov chainCoverageView, kind, coll, rkey string) {
	t.Helper()

	wantPresent := false
	for _, r := range cov.want {
		if r.Kind == kind && r.Collection == coll && r.Rkey == rkey {
			wantPresent = true
			break
		}
	}
	require.Truef(t, wantPresent,
		"power test fixture broken: expected a %s row for %s/%s in the coverage want-set", kind, coll, rkey)

	// Sanity: it passes with the row present.
	require.NoError(t, CompareEventLogCoverage(cov.want, cov.got),
		"coverage should pass before dropping the row")

	// Red: drop the shape's durable row from the observed disk set.
	perturbed := dropRowsMatching(cov.got, kind, coll, rkey)
	require.Lessf(t, len(perturbed), len(cov.got),
		"power test fixture broken: no %s row for %s/%s on disk to drop", kind, coll, rkey)

	err := CompareEventLogCoverage(cov.want, perturbed)
	require.Errorf(t, err, "coverage must fail when the durable %s row for %s/%s is lost", kind, coll, rkey)
	require.Contains(t, err.Error(), "coverage gap")
}

// recordChainForShape returns the (single) record chain of the given
// shape in the spec, failing if absent.
func recordChainForShape(t *testing.T, spec chainSpec, shape chainShape) recordChain {
	t.Helper()
	for _, rc := range spec.records {
		if rc.shape == shape {
			return rc
		}
	}
	t.Fatalf("spec has no chain of shape %q", shape)
	return recordChain{}
}

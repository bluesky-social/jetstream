package main

import (
	"strings"
	"testing"
)

// campaign is a tiny constructor for a Campaign from id->disposition pairs so
// the table tests read as the scorecard they model.
func campaign(commit string, pairs map[string]string) Campaign {
	c := Campaign{Commit: commit}
	for id, disp := range pairs {
		c.Mutants = append(c.Mutants, Mutant{ID: id, Disposition: disp, Result: disp})
	}
	return c
}

func TestEvaluate_CleanCampaignPasses(t *testing.T) {
	base := campaign("abc", map[string]string{
		"m001": "KILLED",
		"m002": "SURVIVED",
	})
	result := campaign("def", map[string]string{
		"m001": "KILLED",
		"m002": "SURVIVED",
	})
	v := Evaluate(base, result)
	if len(v) != 0 {
		t.Fatalf("expected no violations, got %v", v)
	}
}

func TestEvaluate_KilledToSurvivedIsRegression(t *testing.T) {
	base := campaign("abc", map[string]string{"m001": "KILLED"})
	result := campaign("def", map[string]string{"m001": "SURVIVED"})
	v := Evaluate(base, result)
	if len(v) != 1 {
		t.Fatalf("expected exactly one violation, got %d: %v", len(v), v)
	}
	if v[0].Kind != KindRegression || v[0].ID != "m001" {
		t.Fatalf("expected regression on m001, got %+v", v[0])
	}
}

// A mutant that was SURVIVED in the baseline and is now KILLED is an
// IMPROVEMENT, not a failure. It must NOT produce a gate-failing violation, but
// it should surface so the baseline gets refreshed.
func TestEvaluate_SurvivedToKilledIsImprovementNotFailure(t *testing.T) {
	base := campaign("abc", map[string]string{"m001": "SURVIVED"})
	result := campaign("def", map[string]string{"m001": "KILLED"})
	v := Evaluate(base, result)
	for _, viol := range v {
		if viol.Fails() {
			t.Fatalf("an improvement must not fail the gate, got failing violation %+v", viol)
		}
	}
	if !hasKind(v, KindImprovement) {
		t.Fatalf("expected an improvement notice for m001, got %v", v)
	}
}

func TestEvaluate_StaleAndBuildBrokenFail(t *testing.T) {
	base := campaign("abc", map[string]string{"m001": "KILLED", "m002": "KILLED"})
	result := campaign("def", map[string]string{"m001": "STALE", "m002": "BUILD-BROKEN"})
	v := Evaluate(base, result)
	if !hasFailingKind(v, KindStale) {
		t.Fatalf("expected a failing STALE violation, got %v", v)
	}
	if !hasFailingKind(v, KindBuildBroken) {
		t.Fatalf("expected a failing BUILD-BROKEN violation, got %v", v)
	}
}

// A mutant present in the baseline but absent from the fresh campaign means the
// catalog shrank without the baseline being refreshed — the gate must catch it
// rather than silently passing on reduced coverage.
func TestEvaluate_MutantMissingFromResultFails(t *testing.T) {
	base := campaign("abc", map[string]string{"m001": "KILLED", "m002": "KILLED"})
	result := campaign("def", map[string]string{"m001": "KILLED"})
	v := Evaluate(base, result)
	if !hasFailingKind(v, KindMissingFromResult) {
		t.Fatalf("expected a failing missing-from-result violation for m002, got %v", v)
	}
}

// A brand-new mutant in the campaign that the baseline does not know about is
// drift in the other direction: the baseline must be refreshed to record its
// expected disposition. It fails the gate so a new mutant cannot be added
// without recording its baseline.
func TestEvaluate_NewMutantNotInBaselineFails(t *testing.T) {
	base := campaign("abc", map[string]string{"m001": "KILLED"})
	result := campaign("def", map[string]string{"m001": "KILLED", "m099": "KILLED"})
	v := Evaluate(base, result)
	if !hasFailingKind(v, KindNewMutant) {
		t.Fatalf("expected a failing new-mutant violation for m099, got %v", v)
	}
}

func TestEvaluate_UnknownDispositionFails(t *testing.T) {
	base := campaign("abc", map[string]string{"m001": "KILLED"})
	result := campaign("def", map[string]string{"m001": "WHAT"})
	v := Evaluate(base, result)
	if !hasFailingKind(v, KindUnknownDisposition) {
		t.Fatalf("expected a failing unknown-disposition violation, got %v", v)
	}
}

// The whole point of the gate is a non-zero exit on any failing violation; a
// pure improvement or empty set must report success.
func TestHasFailures(t *testing.T) {
	if hasFailures([]Violation{{Kind: KindImprovement, ID: "m001"}}) {
		t.Fatal("an improvement-only set must not be a failure")
	}
	if hasFailures(nil) {
		t.Fatal("empty set must not be a failure")
	}
	if !hasFailures([]Violation{{Kind: KindRegression, ID: "m001"}}) {
		t.Fatal("a regression must be a failure")
	}
}

func TestParseCampaign(t *testing.T) {
	const doc = `{
  "commit": "deadbeef",
  "mutants": [
    {"id": "m001", "disposition": "KILLED", "result": "KILLED@default", "note": "x"},
    {"id": "m002", "disposition": "SURVIVED", "result": "SURVIVED", "note": "gap"}
  ]
}`
	c, err := ParseCampaign(strings.NewReader(doc))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if c.Commit != "deadbeef" || len(c.Mutants) != 2 {
		t.Fatalf("unexpected parse: %+v", c)
	}
	if c.Mutants[0].ID != "m001" || c.Mutants[0].Disposition != "KILLED" {
		t.Fatalf("unexpected first mutant: %+v", c.Mutants[0])
	}
}

func hasKind(vs []Violation, k Kind) bool {
	for _, v := range vs {
		if v.Kind == k {
			return true
		}
	}
	return false
}

func hasFailingKind(vs []Violation, k Kind) bool {
	for _, v := range vs {
		if v.Kind == k && v.Fails() {
			return true
		}
	}
	return false
}

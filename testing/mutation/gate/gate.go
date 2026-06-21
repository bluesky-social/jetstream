// Command gate enforces the oracle mutation campaign baseline (#108).
//
// It diffs a fresh campaign result (the JSON emitted by `run.sh --json`)
// against a committed baseline (testing/mutation/baseline.json) and exits
// non-zero on any failing violation:
//
//   - REGRESSION       a mutant the baseline records as KILLED is now SURVIVED
//   - STALE            a patch no longer applies (catalog drifted from code)
//   - BUILD-BROKEN     a patch no longer compiles
//   - MISSING          a baseline mutant is absent from the fresh campaign
//   - NEW              a campaign mutant is absent from the baseline
//   - UNKNOWN          an unrecognised disposition string
//
// A SURVIVED->KILLED flip is an IMPROVEMENT: surfaced (so the baseline gets
// refreshed) but never fails the gate. This converts RESULTS.md from
// human-maintained prose into an enforced gate keyed off baseline.json.
//
// Usage:
//
//	gate -baseline testing/mutation/baseline.json -result campaign.json
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
)

// Mutant is one row of a campaign or baseline: the mutant id and its coarse
// disposition (KILLED|SURVIVED|STALE|BUILD-BROKEN). Result/Note carry
// human-facing detail and are not used by the gate's verdict.
type Mutant struct {
	ID          string `json:"id"`
	Disposition string `json:"disposition"`
	Result      string `json:"result,omitempty"`
	Note        string `json:"note,omitempty"`
}

// Campaign is the document emitted by run.sh --json and the shape of the
// committed baseline.
type Campaign struct {
	Commit  string   `json:"commit"`
	Mutants []Mutant `json:"mutants"`
}

// Kind classifies a violation. Most kinds fail the gate; KindImprovement is the
// sole non-failing kind (see Fails).
type Kind string

const (
	KindRegression         Kind = "REGRESSION"
	KindStale              Kind = "STALE"
	KindBuildBroken        Kind = "BUILD-BROKEN"
	KindMissingFromResult  Kind = "MISSING"
	KindNewMutant          Kind = "NEW"
	KindUnknownDisposition Kind = "UNKNOWN"
	KindImprovement        Kind = "IMPROVEMENT"
)

const (
	dispKilled      = "KILLED"
	dispSurvived    = "SURVIVED"
	dispStale       = "STALE"
	dispBuildBroken = "BUILD-BROKEN"
)

// Violation is one finding from Evaluate.
type Violation struct {
	Kind    Kind
	ID      string
	Message string
}

// Fails reports whether this violation should fail the gate. Every kind fails
// except an improvement (a SURVIVED->KILLED flip), which is informational.
func (v Violation) Fails() bool {
	return v.Kind != KindImprovement
}

func (v Violation) String() string {
	return fmt.Sprintf("[%s] %s: %s", v.Kind, v.ID, v.Message)
}

// normalizeDisposition collapses the result's free-form disposition to its
// coarse class. run.sh already writes the coarse token in the disposition
// field, but a stress seed-sweep can emit "KILLED@stress(2/5 seeds)"-style
// detail in result; we trust the disposition field and only validate its
// membership in the known set.
func isKnownDisposition(d string) bool {
	switch d {
	case dispKilled, dispSurvived, dispStale, dispBuildBroken:
		return true
	default:
		return false
	}
}

// Evaluate diffs a fresh campaign result against the baseline and returns every
// violation (failing and informational), sorted by mutant id for stable
// output. It is pure: no IO, no process exit, so it is fully unit-testable.
func Evaluate(baseline, result Campaign) []Violation {
	baseByID := indexByID(baseline.Mutants)
	resultByID := indexByID(result.Mutants)

	var violations []Violation

	for id, r := range resultByID {
		if !isKnownDisposition(r.Disposition) {
			violations = append(violations, Violation{
				Kind: KindUnknownDisposition, ID: id,
				Message: fmt.Sprintf("unrecognised disposition %q", r.Disposition),
			})
			continue
		}

		switch r.Disposition {
		case dispStale:
			violations = append(violations, Violation{
				Kind: KindStale, ID: id,
				Message: "patch no longer applies — refresh the mutant or retire it",
			})
		case dispBuildBroken:
			violations = append(violations, Violation{
				Kind: KindBuildBroken, ID: id,
				Message: "patch no longer compiles — refresh the mutant or retire it",
			})
		}

		b, known := baseByID[id]
		if !known {
			violations = append(violations, Violation{
				Kind: KindNewMutant, ID: id,
				Message: fmt.Sprintf("mutant is not in the baseline (disposition %s); refresh baseline.json to record it", r.Disposition),
			})
			continue
		}

		// Regression / improvement only make sense for the runnable
		// dispositions; STALE/BUILD-BROKEN were already flagged above.
		if b.Disposition == dispKilled && r.Disposition == dispSurvived {
			violations = append(violations, Violation{
				Kind: KindRegression, ID: id,
				Message: "baseline KILLED but now SURVIVED — the oracle lost detection power for this mutant",
			})
		}
		if b.Disposition == dispSurvived && r.Disposition == dispKilled {
			violations = append(violations, Violation{
				Kind: KindImprovement, ID: id,
				Message: "baseline SURVIVED but now KILLED — detection improved; refresh baseline.json",
			})
		}
	}

	for id := range baseByID {
		if _, present := resultByID[id]; !present {
			violations = append(violations, Violation{
				Kind: KindMissingFromResult, ID: id,
				Message: "baseline mutant absent from the campaign result — catalog shrank or the run was filtered; refresh baseline.json if intentional",
			})
		}
	}

	sort.Slice(violations, func(i, j int) bool {
		if violations[i].ID != violations[j].ID {
			return violations[i].ID < violations[j].ID
		}
		return violations[i].Kind < violations[j].Kind
	})
	return violations
}

func indexByID(ms []Mutant) map[string]Mutant {
	out := make(map[string]Mutant, len(ms))
	for _, m := range ms {
		out[m.ID] = m
	}
	return out
}

func hasFailures(vs []Violation) bool {
	for _, v := range vs {
		if v.Fails() {
			return true
		}
	}
	return false
}

// ParseCampaign decodes a campaign/baseline JSON document and rejects corrupt
// input at the boundary (project directive: fail loud over corrupt). It rejects:
//
//   - a duplicate mutant id, which would make indexByID silently drop a row and
//     mask a regression;
//   - an empty mutant id;
//   - an unrecognised disposition string. Both the baseline and the fresh result
//     flow through here, so a baseline typo such as "KILED" can never reach the
//     regression check (where == dispKilled would silently miss and a genuine
//     KILLED->SURVIVED regression would pass the gate);
//   - trailing bytes after the JSON document. json.Decoder.Decode reads exactly
//     one value and leaves the rest of the stream unread, so without this check a
//     valid campaign followed by junk or a second document parses clean — hiding
//     corruption in an enforcement artifact instead of failing closed.
func ParseCampaign(r io.Reader) (Campaign, error) {
	var c Campaign
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return Campaign{}, fmt.Errorf("decode campaign json: %w", err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return Campaign{}, fmt.Errorf("decode campaign json: trailing data after the campaign document")
	}
	seen := map[string]struct{}{}
	for _, m := range c.Mutants {
		if m.ID == "" {
			return Campaign{}, fmt.Errorf("campaign contains a mutant with an empty id")
		}
		if !isKnownDisposition(m.Disposition) {
			return Campaign{}, fmt.Errorf("campaign mutant %q has unrecognised disposition %q", m.ID, m.Disposition)
		}
		if _, dup := seen[m.ID]; dup {
			return Campaign{}, fmt.Errorf("campaign contains duplicate mutant id %q", m.ID)
		}
		seen[m.ID] = struct{}{}
	}
	return c, nil
}

func loadCampaign(path string) (Campaign, error) {
	f, err := os.Open(path)
	if err != nil {
		return Campaign{}, err
	}
	defer f.Close()
	return ParseCampaign(f)
}

func main() {
	baselinePath := flag.String("baseline", "testing/mutation/baseline.json", "path to the committed baseline JSON")
	resultPath := flag.String("result", "", "path to the fresh campaign result JSON (from run.sh --json)")
	flag.Parse()

	if *resultPath == "" {
		fmt.Fprintln(os.Stderr, "gate: -result is required")
		os.Exit(2)
	}

	baseline, err := loadCampaign(*baselinePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gate: load baseline: %v\n", err)
		os.Exit(2)
	}
	result, err := loadCampaign(*resultPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gate: load result: %v\n", err)
		os.Exit(2)
	}

	violations := Evaluate(baseline, result)
	if len(violations) == 0 {
		fmt.Printf("gate: PASS — %d mutants match baseline (commit %s)\n", len(result.Mutants), result.Commit)
		return
	}

	var failing int
	for _, v := range violations {
		marker := "note"
		if v.Fails() {
			marker = "FAIL"
			failing++
		}
		fmt.Printf("%s %s\n", marker, v.String())
	}

	if failing > 0 {
		fmt.Fprintf(os.Stderr, "gate: FAIL — %d failing violation(s) against baseline %s\n", failing, *baselinePath)
		os.Exit(1)
	}
	fmt.Printf("gate: PASS — %d informational notice(s), no regressions\n", len(violations))
}
